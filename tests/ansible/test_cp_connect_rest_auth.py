"""
cp_connect against a Connect REST API that uses TLS, mTLS, HTTP basic or bearer auth.

Runs the real role with ansible-playbook (next to sys.executable) against
fake_connect_rest.FakeConnect, and asserts on what the role actually sent and
how the run ended. Needs ansible-core and cryptography in the running Python:

    python -m pytest tests/ansible/test_cp_connect_rest_auth.py -v
"""
import json
import os
import subprocess
import sys

import pytest

from fake_connect_rest import FakeConnect, make_pki

pytest.importorskip("cryptography")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PLAYBOOK_BIN = os.path.join(os.path.dirname(sys.executable), "ansible-playbook")

CONNECTOR = {"name": "jdbc-source-oracle-east",
             "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "tasks.max": "1"}}

PLAY = """\
- name: Exercise cp_connect
  hosts: localhost
  connection: local
  gather_facts: false
  tasks:
    - name: Run cp_connect
      ansible.builtin.include_role:
        name: cp_connect
        tasks_from: "{{ cp_connect_entry | default('main.yml') }}"
"""


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    return make_pki(str(tmp_path_factory.mktemp("pki")))


def run_role(tmp_path, url, *flags, **role_vars):
    """Run cp_connect once. Returns the CompletedProcess (stdout + stderr merged)."""
    if not os.path.exists(PLAYBOOK_BIN):
        pytest.skip("ansible-playbook not found next to " + sys.executable)
    variables = {"cp_connect_url": url, "cp_connectors": [CONNECTOR],
                 "cp_connect_health_retries": 1, "cp_connect_health_delay": 0,
                 "ansible_python_interpreter": sys.executable}
    variables.update(role_vars)
    (tmp_path / "play.yml").write_text(PLAY)
    (tmp_path / "vars.json").write_text(json.dumps(variables))
    (tmp_path / "ansible.cfg").write_text("[defaults]\n")
    env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
    env.update(ANSIBLE_ROLES_PATH=os.path.join(REPO_ROOT, "ansible", "roles"),
               ANSIBLE_CONFIG=str(tmp_path / "ansible.cfg"), ANSIBLE_NOCOLOR="1",
               ANSIBLE_STDOUT_CALLBACK="default", NO_PROXY="127.0.0.1,localhost",
               OBJC_DISABLE_INITIALIZE_FORK_SAFETY="YES")
    cmd = [PLAYBOOK_BIN, "-i", "localhost,", "play.yml", "-e", "@vars.json", *flags]
    return subprocess.run(cmd, cwd=tmp_path, env=env, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=300)


def requests_to(api, method, suffix):
    return [r for r in api.requests if r["method"] == method and r["path"].split("?")[0].endswith(suffix)]


class TestPlainHttpDefaults:
    def test_defaults_send_no_credentials(self, tmp_path):
        with FakeConnect() as api:
            proc = run_role(tmp_path, api.url)
        assert proc.returncode == 0, proc.stdout[-3000:]
        puts = requests_to(api, "PUT", "/config")
        assert len(puts) == 1
        assert puts[0]["content_type"] == "application/json"
        assert all(r["auth"] is None for r in api.requests), api.requests
        assert "created=1" in proc.stdout


class TestHttps:
    def test_private_ca_via_ca_path(self, tmp_path, pki):
        with FakeConnect(tls_cert=pki["server.pem"], tls_key=pki["server.key"]) as api:
            proc = run_role(tmp_path, api.url, cp_connect_ca_path=pki["ca.pem"])
        assert proc.returncode == 0, proc.stdout[-3000:]
        assert len(requests_to(api, "PUT", "/config")) == 1

    def test_private_ca_without_ca_path_fails_verification(self, tmp_path, pki):
        with FakeConnect(tls_cert=pki["server.pem"], tls_key=pki["server.key"]) as api:
            proc = run_role(tmp_path, api.url, cp_connect_validate_health=False)
        assert proc.returncode != 0
        assert "CERTIFICATE_VERIFY_FAILED" in proc.stdout, proc.stdout[-3000:]
        assert not requests_to(api, "PUT", "/config")


class TestMutualTls:
    def _api(self, pki):
        return FakeConnect(tls_cert=pki["server.pem"], tls_key=pki["server.key"], client_ca=pki["ca.pem"])

    def test_no_client_cert_is_rejected(self, tmp_path, pki):
        with self._api(pki) as api:
            proc = run_role(tmp_path, api.url, cp_connect_ca_path=pki["ca.pem"], cp_connect_validate_health=False)
        assert proc.returncode != 0
        assert api.tls_rejections >= 1
        assert not requests_to(api, "PUT", "/config")

    def test_client_cert_is_presented(self, tmp_path, pki):
        with self._api(pki) as api:
            proc = run_role(tmp_path, api.url, cp_connect_ca_path=pki["ca.pem"],
                            cp_connect_client_cert=pki["client.pem"], cp_connect_client_key=pki["client.key"])
        assert proc.returncode == 0, proc.stdout[-3000:]
        puts = requests_to(api, "PUT", "/config")
        assert len(puts) == 1 and puts[0]["client_cn"] == "ansible-controller"
        assert {r["client_cn"] for r in api.requests} == {"ansible-controller"}

    def test_cert_from_another_ca_is_rejected(self, tmp_path, pki):
        with self._api(pki) as api:
            proc = run_role(tmp_path, api.url, cp_connect_ca_path=pki["ca.pem"], cp_connect_validate_health=False,
                            cp_connect_client_cert=pki["rogue-client.pem"], cp_connect_client_key=pki["rogue-client.key"])
        assert proc.returncode != 0
        assert api.tls_rejections >= 1
        assert not requests_to(api, "PUT", "/config")


class TestBasicAuth:
    def test_right_password(self, tmp_path):
        with FakeConnect(basic=("connect-admin", "s3cret")) as api:
            proc = run_role(tmp_path, api.url, cp_connect_user="connect-admin", cp_connect_password="s3cret")
        assert proc.returncode == 0, proc.stdout[-3000:]
        assert api.requests and all(r["auth"].startswith("Basic ") and r["auth_ok"] for r in api.requests)

    def test_wrong_password_fails_the_deploy(self, tmp_path):
        with FakeConnect(basic=("connect-admin", "s3cret")) as api:
            proc = run_role(tmp_path, api.url, cp_connect_user="connect-admin", cp_connect_password="wrong",
                            cp_connect_validate_health=False)
        assert proc.returncode != 0, proc.stdout[-3000:]
        assert "connector deployment(s) failed" in proc.stdout
        assert "HTTP 401" in proc.stdout
        assert len(requests_to(api, "PUT", "/config")) == 1, "a 401 is not retried"

    def test_wrong_password_stops_before_health_checks(self, tmp_path):
        with FakeConnect(basic=("connect-admin", "s3cret")) as api:
            proc = run_role(tmp_path, api.url, cp_connect_user="connect-admin", cp_connect_password="wrong")
        assert proc.returncode != 0
        assert "connector deployment(s) failed" in proc.stdout, proc.stdout[-3000:]
        assert not requests_to(api, "GET", "/status")


class TestBearer:
    def test_token_on_every_call(self, tmp_path):
        with FakeConnect(bearer="t0k3n", task_state="FAILED") as api:
            proc = run_role(tmp_path, api.url, cp_connect_token="t0k3n")
        assert proc.returncode == 0, proc.stdout[-3000:]
        assert requests_to(api, "PUT", "/config")[0]["auth"] == "Bearer t0k3n"
        restarts = requests_to(api, "POST", "/restart")
        assert len(restarts) == 1 and restarts[0]["auth"] == "Bearer t0k3n"
        assert all(r["auth_ok"] for r in api.requests)

    def test_check_mode_sends_token(self, tmp_path):
        with FakeConnect(bearer="t0k3n") as api:
            proc = run_role(tmp_path, api.url, "--check", cp_connect_token="t0k3n")
        assert proc.returncode == 0, proc.stdout[-3000:]
        listing = requests_to(api, "GET", "/connectors")
        assert listing and listing[0]["auth"] == "Bearer t0k3n"
        assert not requests_to(api, "PUT", "/config")

    def test_wrapper_entry_point_sends_token_and_hides_it(self, tmp_path):
        # The six connector wrapper roles call tasks_from: deploy.yml directly.
        with FakeConnect(bearer="t0k3n") as api:
            proc = run_role(tmp_path, api.url, "-vvv", cp_connect_entry="deploy.yml", cp_connect_token="wrong-t0k3n")
        assert proc.returncode != 0
        assert requests_to(api, "PUT", "/config")[0]["auth"] == "Bearer wrong-t0k3n"
        assert "HTTP 401" in proc.stdout, proc.stdout[-3000:]
        assert "wrong-t0k3n" not in proc.stdout


class TestInputsAndRetries:
    def test_token_and_user_together_rejected(self, tmp_path):
        with FakeConnect() as api:
            proc = run_role(tmp_path, api.url, cp_connect_token="t0k3n", cp_connect_user="u", cp_connect_password="p")
        assert proc.returncode != 0
        assert "not both" in proc.stdout, proc.stdout[-3000:]
        assert not api.requests

    def test_transient_conflict_is_retried(self, tmp_path):
        with FakeConnect(put_statuses=[409]) as api:
            proc = run_role(tmp_path, api.url, cp_connect_validate_health=False)
        assert proc.returncode == 0, proc.stdout[-3000:]
        assert len(requests_to(api, "PUT", "/config")) == 2
        assert "created=1" in proc.stdout
