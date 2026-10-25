"""A fake Kafka Connect REST API for exercising cp_connect without a cluster.

Serves the endpoints cp_connect and the mq-quickstart scenario call, over HTTP
or HTTPS, optionally requiring a client certificate (mTLS) and HTTP basic or
bearer authentication. Every request is recorded so tests can assert what the
role actually sent.

    with FakeConnect(tls_cert=..., tls_key=..., client_ca=..., bearer="t0k3n") as api:
        ... run ansible against api.url ...
        assert api.requests[0]["auth"] == "Bearer t0k3n"

Standalone (molecule): python fake_connect_rest.py --port 18083   (plain HTTP)

make_pki() needs the `cryptography` package (ansible-core depends on it).
"""
import argparse
import base64
import json
import os
import re
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Kafka's JaasBasicAuthFilter rejects with a plain-text body, not JSON.
UNAUTHORIZED_BODY = b"User cannot access the resource."


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        # Handshake here, in the request thread, so a rejected client cert is
        # counted instead of being swallowed by the accept loop.
        if isinstance(self.request, ssl.SSLSocket):
            try:
                self.request.do_handshake()
            except (ssl.SSLError, OSError):
                self.server.fake.tls_rejections += 1
                raise
        super().setup()

    def log_message(self, *args):
        pass

    def _client_cn(self):
        if not isinstance(self.connection, ssl.SSLSocket):
            return None
        cert = self.connection.getpeercert() or {}
        for rdn in cert.get("subject", ()):
            for key, value in rdn:
                if key == "commonName":
                    return value
        return None

    def _send(self, status, payload=None, raw=None, content_type="application/json"):
        body = raw if raw is not None else (b"" if payload is None else json.dumps(payload).encode())
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method):
        fake = self.server.fake
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode() if length else ""
        auth = self.headers.get("Authorization")
        auth_ok = fake.auth_ok(auth)
        fake.requests.append({
            "method": method, "path": self.path, "auth": auth, "auth_ok": auth_ok,
            "client_cn": self._client_cn(), "content_type": self.headers.get("Content-Type"),
            "body": body,
        })
        if not auth_ok:
            return self._send(401, raw=UNAUTHORIZED_BODY, content_type="text/plain")
        status, payload = fake.route(method, self.path.split("?", 1)[0], body)
        self._send(status, payload)

    def do_GET(self):
        self._handle("GET")

    def do_PUT(self):
        self._handle("PUT")

    def do_POST(self):
        self._handle("POST")


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass


class FakeConnect:
    def __init__(self, tls_cert=None, tls_key=None, client_ca=None, basic=None, bearer=None,
                 put_statuses=None, task_state="RUNNING", connectors=None, port=0):
        self.basic, self.bearer = basic, bearer
        self.put_statuses = list(put_statuses or [])
        self.task_state = task_state
        self.connectors = dict(connectors or {})
        self.states = {name: task_state for name in self.connectors}
        self.requests = []
        self.tls_rejections = 0
        self._server = _Server(("127.0.0.1", port), _Handler)
        self._server.fake = self
        self.scheme = "http"
        if tls_cert:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(tls_cert, tls_key)
            if client_ca:
                ctx.verify_mode = ssl.CERT_REQUIRED
                ctx.load_verify_locations(client_ca)
            self._server.socket = ctx.wrap_socket(
                self._server.socket, server_side=True, do_handshake_on_connect=False)
            self.scheme = "https"
        self.port = self._server.server_address[1]
        self.url = "%s://127.0.0.1:%d" % (self.scheme, self.port)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()

    def auth_ok(self, header):
        if self.basic:
            token = base64.b64encode(("%s:%s" % self.basic).encode()).decode()
            return header == "Basic " + token
        if self.bearer:
            return header == "Bearer " + self.bearer
        return True

    def status_of(self, name):
        return {"name": name, "type": "source",
                "connector": {"state": "RUNNING", "worker_id": "127.0.0.1:8083"},
                "tasks": [{"id": 0, "state": self.states[name], "worker_id": "127.0.0.1:8083"}]}

    def route(self, method, path, body):
        if method == "GET" and path == "/":
            return 200, {"version": "8.1.0-ce", "commit": "fake", "kafka_cluster_id": "FakeCluster0"}
        if method == "GET" and path == "/connectors":
            return 200, sorted(self.connectors)
        match = re.fullmatch(r"/connectors/([^/]+)/(config|status|restart)", path)
        if not match:
            return 404, {"error_code": 404, "message": "Unknown endpoint " + path}
        name, leaf = match.groups()
        if method == "PUT" and leaf == "config":
            if self.put_statuses:
                status = self.put_statuses.pop(0)
                return status, {"error_code": status, "message": "Request cannot be completed: rebalance in progress"}
            try:
                config = json.loads(body)
            except ValueError:
                config = None
            if not isinstance(config, dict):
                return 400, {"error_code": 400, "message": "Body must be a JSON object"}
            created = name not in self.connectors
            self.connectors[name] = config
            self.states.setdefault(name, self.task_state)
            return (201 if created else 200), {"name": name, "config": config, "tasks": []}
        if name not in self.connectors:
            return 404, {"error_code": 404, "message": "Connector %s not found" % name}
        if method == "GET" and leaf == "config":
            return 200, self.connectors[name]
        if method == "GET" and leaf == "status":
            return 200, self.status_of(name)
        if method == "POST" and leaf == "restart":
            self.states[name] = "RUNNING"
            return 202, self.status_of(name)
        return 405, {"error_code": 405, "message": "Method not allowed"}


def make_pki(dest):
    """A test CA, a server cert for 127.0.0.1/localhost, a client cert signed by
    the CA, and a rogue CA with its own client cert. Returns {name: path}."""
    import datetime
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    os.makedirs(dest, exist_ok=True)
    now = datetime.datetime.now(datetime.timezone.utc)
    paths = {}

    def write(name, data, mode=0o644):
        path = os.path.join(dest, name)
        with open(path, "wb") as f:
            f.write(data)
        os.chmod(path, mode)
        paths[name] = path

    def cert(subject_cn, key, issuer_name, issuer_key, ca=False, eku=(), san=None):
        builder = (x509.CertificateBuilder()
                   .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject_cn)]))
                   .issuer_name(issuer_name).public_key(key.public_key())
                   .serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(minutes=5))
                   .not_valid_after(now + datetime.timedelta(days=1))
                   .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
        if eku:
            builder = builder.add_extension(x509.ExtendedKeyUsage(list(eku)), critical=False)
        if san:
            builder = builder.add_extension(x509.SubjectAlternativeName(san), critical=False)
        return builder.sign(issuer_key, hashes.SHA256())

    def pem_key(key):
        return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                 serialization.NoEncryption())

    def pem_cert(c):
        return c.public_bytes(serialization.Encoding.PEM)

    for prefix in ("", "rogue-"):
        ca_key = ec.generate_private_key(ec.SECP256R1())
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, prefix + "fake-connect-ca")])
        ca_cert = cert(prefix + "fake-connect-ca", ca_key, ca_name, ca_key, ca=True)
        write(prefix + "ca.pem", pem_cert(ca_cert))
        client_key = ec.generate_private_key(ec.SECP256R1())
        client_cert = cert(prefix + "ansible-controller", client_key, ca_name, ca_key,
                           eku=[ExtendedKeyUsageOID.CLIENT_AUTH])
        write(prefix + "client.pem", pem_cert(client_cert))
        write(prefix + "client.key", pem_key(client_key), 0o600)
        if not prefix:
            server_key = ec.generate_private_key(ec.SECP256R1())
            server_cert = cert("fake-connect", server_key, ca_name, ca_key,
                               eku=[ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
                               san=[x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))])
            write("server.pem", pem_cert(server_cert))
            write("server.key", pem_key(server_key), 0o600)
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=18083)
    args = parser.parse_args()
    fake = FakeConnect(port=args.port)
    print("fake Connect REST API on", fake.url, flush=True)
    fake._server.serve_forever()


if __name__ == "__main__":
    main()
