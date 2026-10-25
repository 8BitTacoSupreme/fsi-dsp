"""The register and health plays, run for real against a fake Connect REST API
that requires a client certificate (tests/ansible/fake_connect_rest.py at the
repo root), with real test PKI."""
import json
import sys

import pytest
import yaml

from conftest import SCENARIO, _load, run_site

pytest.importorskip("cryptography")
fake = _load("fake_connect_rest", SCENARIO.parent.parent / "tests" / "ansible" / "fake_connect_rest.py")


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    return fake.make_pki(str(tmp_path_factory.mktemp("rest-pki")))


def mtls_api(pki, **kwargs):
    return fake.FakeConnect(tls_cert=pki["server.pem"], tls_key=pki["server.key"], client_ca=pki["ca.pem"], **kwargs)


def rest_vars(api, pki, client="client"):
    return json.dumps({
        "connect_rest_host": "127.0.0.1", "connect_rest_port": api.port,
        "connect_rest_ca_pem": pki["ca.pem"],
        "connect_rest_client_cert_pem": pki[client + ".pem"],
        "connect_rest_client_key_pem": pki[client + ".key"],
        # Health and register poll with retries; keep a failing test fast.
        "cp_connect_health_retries": 1, "cp_connect_health_delay": 0,
    })


def register(tmp_path, *extra):
    return run_site(tmp_path, *extra, tags="register")


def test_register_presents_the_controller_certificate(tmp_path, pki):
    with mtls_api(pki) as api:
        proc, _out = register(tmp_path, rest_vars(api, pki))
    assert proc.returncode == 0, proc.stdout[-4000:]
    put = [r for r in api.requests if r["method"] == "PUT"]
    assert [r["path"] for r in put] == ["/connectors/mq-zos-source-transaction/config"]
    assert put[0]["client_cn"] == "ansible-controller"
    assert json.loads(put[0]["body"])["mq.transport.type"] == "bindings"


def test_register_with_a_certificate_from_another_ca_fails(tmp_path, pki):
    with mtls_api(pki) as api:
        proc, _out = register(tmp_path, rest_vars(api, pki, client="rogue-client"))
    assert proc.returncode != 0
    assert api.tls_rejections >= 1
    assert "connector deployment(s) failed" in proc.stdout, proc.stdout[-4000:]


def test_register_in_http_mode_reads_no_mtls_input(tmp_path):
    with fake.FakeConnect() as api:
        undefined = {name: "{{ undefined_mtls_only_input }}" for name in (
            "connect_rest_keystore", "connect_rest_keystore_password", "connect_rest_truststore",
            "connect_rest_truststore_password", "connect_rest_ca_pem", "connect_rest_client_cert_pem",
            "connect_rest_client_key_pem")}
        proc, _out = register(tmp_path, json.dumps(dict(undefined, connect_rest_security="http",
                                                        connect_rest_host="127.0.0.1",
                                                        connect_rest_port=api.port)))
    assert proc.returncode == 0, proc.stdout[-4000:]
    assert [r["method"] for r in api.requests if r["method"] == "PUT"] == ["PUT"]
    assert all(r["client_cn"] is None for r in api.requests)


def test_health_reads_status_over_mtls(tmp_path, pki):
    connectors = {"mq-zos-source-transaction": {"connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector"}}
    with mtls_api(pki, connectors=connectors) as api:
        proc, _out = run_site(tmp_path, rest_vars(api, pki), "ansible_connection=local",
                              "ansible_python_interpreter=%s" % sys.executable,
                              tags="health")
    assert proc.returncode == 0, proc.stdout[-4000:]
    assert "All connectors and tasks RUNNING" in proc.stdout
    status = [r for r in api.requests if r["path"].endswith("/status")]
    assert status and all(r["client_cn"] == "ansible-controller" for r in status)


def test_start_waits_for_the_api_with_the_client_certificate():
    # start.yml needs z/OS (operator commands), so its wait task is checked statically.
    tasks = yaml.safe_load((SCENARIO / "tasks" / "start.yml").read_text())
    wait = next(t for t in tasks if t.get("name") == "Wait for the Connect REST API")["ansible.builtin.uri"]
    assert wait["ca_path"] == "{{ connect_rest_ca_pem if (connect_rest_mtls | bool) else omit }}"
    assert wait["client_cert"] == "{{ connect_rest_client_cert_pem if (connect_rest_mtls | bool) else omit }}"
    assert wait["client_key"] == "{{ connect_rest_client_key_pem if (connect_rest_mtls | bool) else omit }}"
