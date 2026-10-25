"""The Connect REST API serves HTTPS and requires a client certificate by
default (connect_rest_security: mtls), with its own stores under the
listeners.https. prefix. Plain HTTP only when inputs.yml asks for it."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from conftest import FIXTURES, SCENARIO, run_build
from test_render import CC_TARGET, read

REST_KEYSTORE = "/u/kafconn/config/ssl/rest-server.p12"
REST_TRUSTSTORE = "/u/kafconn/config/ssl/rest-client-ca.p12"
KAFKA_KEYSTORE = "/u/kafconn/config/ssl/kafka-client.p12"
REST_FILE_INPUTS = ["connect_rest_keystore", "connect_rest_truststore", "connect_rest_ca_pem",
                    "connect_rest_client_cert_pem", "connect_rest_client_key_pem"]
MTLS_ONLY_INPUTS = REST_FILE_INPUTS + ["connect_rest_keystore_password", "connect_rest_truststore_password"]

# The whole listeners.https. set, exactly: once any prefixed key is present
# Connect ignores the worker's ssl.* for the REST listener, so a missing key
# would fall back to Kafka's default, not to the worker setting.
EXPECTED_LISTENER = {
    "listeners.https.ssl.keystore.type": "PKCS12",
    "listeners.https.ssl.keystore.location": REST_KEYSTORE,
    "listeners.https.ssl.keystore.password": "TESTRESTKSPW",
    "listeners.https.ssl.key.password": "TESTRESTKSPW",
    "listeners.https.ssl.truststore.type": "PKCS12",
    "listeners.https.ssl.truststore.location": REST_TRUSTSTORE,
    "listeners.https.ssl.truststore.password": "TESTRESTTSPW",
    "listeners.https.ssl.client.auth": "required",
    "listeners.https.ssl.endpoint.identification.algorithm": "https",
}
MTLS_GATE = ("SECURITY  accept mTLS on the Connect REST API (zos-test.example.com:8083): any client "
             "certificate signed by a CA in kafconn-rest-clients.truststore.p12 has full control (no REST RBAC)")
HTTP_GATE = ("SECURITY  accept a plain-HTTP, unauthenticated Connect REST API on zos-test.example.com:8083 "
             "(connect_rest_security: http)")
# http mode with every mTLS-only input undefined: proves http never reads them.
HTTP_MODE = json.dumps(dict({"connect_rest_security": "http"},
                            **{name: "{{ undefined_mtls_only_input }}" for name in MTLS_ONLY_INPUTS}))


def worker(out):
    lines = read(out, "rendered/connect-distributed.properties").splitlines()
    pairs = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    return lines, pairs


def rest_settings(pairs):
    return {k: v for k, v in pairs.items() if k.startswith("listeners.https.")}


def test_mtls_is_the_default_with_its_own_stores(rendered):
    lines, pairs = worker(rendered)
    assert "listeners=https://0.0.0.0:8083" in lines
    assert "rest.advertised.listener=https" in lines
    assert "rest.advertised.host.name=zos-test.example.com" in lines
    assert "rest.advertised.port=8083" in lines
    assert rest_settings(pairs) == EXPECTED_LISTENER


def test_rest_listener_never_reuses_the_kafka_client_keystore(rendered):
    # On Confluent Platform the worker's own ssl.* is the Kafka mTLS identity.
    _lines, pairs = worker(rendered)
    assert pairs["ssl.keystore.location"] == KAFKA_KEYSTORE
    assert pairs["ssl.keystore.password"] == "TESTKSPW"
    assert pairs["listeners.https.ssl.keystore.location"] != pairs["ssl.keystore.location"]
    assert "ssl.client.auth" not in pairs
    assert not [k for k in pairs if k.startswith("ssl.truststore")]


def test_mtls_gate_says_who_has_control(rendered):
    gates = read(rendered, "gates.txt").splitlines()
    assert MTLS_GATE in gates
    assert gates.index(MTLS_GATE) == gates.index(
        "NETWORK   inbound TCP 8083 to zos-test.example.com from this controller") - 1


def test_cc_target_with_mtls_rest(tmp_path):
    proc, out = run_build(tmp_path, CC_TARGET)
    assert proc.returncode == 0, proc.stdout[-4000:]
    lines, pairs = worker(out)
    assert "listeners=https://0.0.0.0:8083" in lines
    assert rest_settings(pairs) == EXPECTED_LISTENER
    assert not [k for k in pairs if k.startswith("ssl.keystore")]


def test_http_mode_keeps_todays_lines_and_reads_no_mtls_input(tmp_path):
    proc, out = run_build(tmp_path, HTTP_MODE)
    assert proc.returncode == 0, proc.stdout[-4000:]
    lines, pairs = worker(out)
    rest = lines[lines.index("listeners=http://0.0.0.0:8083"):]
    assert rest == ["listeners=http://0.0.0.0:8083", "rest.advertised.host.name=zos-test.example.com",
                    "rest.advertised.port=8083"]
    assert not rest_settings(pairs) and "rest.advertised.listener" not in pairs
    gates = read(out, "gates.txt").splitlines()
    assert HTTP_GATE in gates and MTLS_GATE not in gates


def test_backslash_in_rest_passwords_survives_properties_parsing(tmp_path):
    proc, out = run_build(tmp_path, json.dumps({"connect_rest_keystore_password": "re\\st",
                                                "connect_rest_truststore_password": "tr\\ust"}))
    assert proc.returncode == 0, proc.stdout[-3000:]
    lines, _pairs = worker(out)
    assert "listeners.https.ssl.keystore.password=re\\\\st" in lines
    assert "listeners.https.ssl.key.password=re\\\\st" in lines
    assert "listeners.https.ssl.truststore.password=tr\\\\ust" in lines


def test_unknown_rest_security_stops_the_build(tmp_path):
    proc, _out = run_build(tmp_path, "connect_rest_security=tls")
    assert proc.returncode != 0
    assert "connect_rest_security" in proc.stdout, proc.stdout[-3000:]


def test_inputs_from_before_rest_mtls_are_told_what_to_set(tmp_path):
    # An inputs.yml written before connect_rest_security existed has none of the
    # REST files: the build names the switch instead of an undefined variable.
    old = json.dumps({name: "{{ undefined_old_inputs }}" for name in REST_FILE_INPUTS})
    proc, _out = run_build(tmp_path, old)
    assert proc.returncode != 0
    assert "set connect_rest_security: http" in proc.stdout, proc.stdout[-3000:]


@pytest.mark.parametrize("var", REST_FILE_INPUTS)
def test_missing_rest_file_stops_the_build(tmp_path, var):
    proc, _out = run_build(tmp_path, "%s=%s" % (var, tmp_path / "absent.pem"))
    assert proc.returncode != 0
    assert "%s=%s is missing, unreadable or empty" % (var, tmp_path / "absent.pem") in proc.stdout, \
        proc.stdout[-3000:]


def test_client_key_readable_by_others_stops_the_build(tmp_path):
    key = tmp_path / "open.key.pem"
    key.write_text("-----BEGIN PRIVATE KEY-----\n")
    key.chmod(0o644)
    proc, _out = run_build(tmp_path, "connect_rest_client_key_pem=%s" % key)
    assert proc.returncode != 0
    assert "connect_rest_client_key_pem=%s is readable by group or others" % key in proc.stdout, \
        proc.stdout[-3000:]


@pytest.mark.parametrize("var", REST_FILE_INPUTS)
def test_rest_placeholders_stop_the_build(tmp_path, var):
    value = "<PLACEHOLDER_PATH>/" + var
    proc, _out = run_build(tmp_path, "%s=%s" % (var, value))
    assert proc.returncode != 0
    assert "still has a placeholder: " + value in proc.stdout, proc.stdout[-3000:]


STORES_PLAY = """\
- name: Print zos_binary_stores
  hosts: localhost
  gather_facts: false
  vars_files:
    - {derived}
  tasks:
    - name: Print it
      ansible.builtin.debug:
        msg: "STORES={{{{ zos_binary_stores | to_json }}}}"
"""


@pytest.mark.parametrize("target,rest,dests", [
    ("cp", "mtls", ["kafka-client.p12", "rest-server.p12", "rest-client-ca.p12"]),
    ("cp", "http", ["kafka-client.p12"]),
    ("cc", "mtls", ["rest-server.p12", "rest-client-ca.p12"]),
    ("cc", "http", []),
])
def test_binary_stores_follow_the_target_and_rest_mode(tmp_path, target, rest, dests):
    # What install copies to z/OS: derived.yml rendered for each combination,
    # with the inputs a combination does not use left undefined.
    play = tmp_path / "stores.yml"
    play.write_text(STORES_PLAY.format(derived=SCENARIO / "vars" / "derived.yml"))
    extra = {"kafka_target": target, "connect_rest_security": rest,
             "kafka_client_keystore": "/ctl/kafka.p12" if target == "cp" else "{{ undefined_input }}",
             "connect_rest_keystore": "/ctl/rest.p12" if rest == "mtls" else "{{ undefined_input }}",
             "connect_rest_truststore": "/ctl/rest-ca.p12" if rest == "mtls" else "{{ undefined_input }}"}
    cmd = [str(Path(sys.executable).parent / "ansible-playbook"), "-i", "localhost,", str(play),
           "-e", "@%s" % (FIXTURES / "inputs.test.yml"), "-e", json.dumps(extra)]
    proc = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True, stdin=subprocess.DEVNULL,
                          env=dict(os.environ, ANSIBLE_NOCOLOR="1", ANSIBLE_STDOUT_CALLBACK="default"))
    assert proc.returncode == 0, proc.stdout[-3000:]
    line = next(l for l in proc.stdout.splitlines() if '"msg": "STORES=' in l)
    stores = json.loads(json.loads("{" + line.strip() + "}")["msg"][len("STORES="):])
    assert [s["dest"].rsplit("/", 1)[1] for s in stores] == dests
    assert all(s["dest"].startswith("/u/kafconn/config/ssl/") for s in stores)
    srcs = {"kafka-client.p12": "/ctl/kafka.p12", "rest-server.p12": "/ctl/rest.p12",
            "rest-client-ca.p12": "/ctl/rest-ca.p12"}
    assert [s["src"] for s in stores] == [srcs[d] for d in dests]


def test_install_copies_every_store_in_binary():
    # The copy runs on z/OS, so this is static: one loop over zos_binary_stores,
    # binary, owner-only, tagged and checked.
    block = next(t for t in yaml.safe_load((SCENARIO / "tasks" / "install.yml").read_text())
                 if "block" in t and "zos_binary_stores" in str(t.get("when")))
    steps = {t["name"]: t for t in block["block"]}
    copy = next(t for t in steps.values() if "ibm.ibm_zos_core.zos_copy" in t)
    assert copy["loop"] == "{{ zos_binary_stores }}"
    assert copy["ibm.ibm_zos_core.zos_copy"]["binary"] is True
    assert copy["ibm.ibm_zos_core.zos_copy"]["mode"] == "0600"
    assert copy["ibm.ibm_zos_core.zos_copy"]["src"] == "{{ item.src }}"
    assert copy["ibm.ibm_zos_core.zos_copy"]["dest"] == "{{ item.dest }}"
    assert any("chtag -b" in str(t) and t.get("loop") == "{{ zos_binary_stores }}" for t in steps.values())
