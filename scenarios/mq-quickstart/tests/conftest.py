"""Shared fixtures for the mq-quickstart tests.

Run from the scenario directory with the same Python that has ansible-core
(the render test shells out to ansible-playbook next to sys.executable):

    python -m pytest tests -v
"""
import importlib.util
import io
import os
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

SCENARIO = Path(__file__).resolve().parent.parent
FIXTURES = SCENARIO / "tests" / "fixtures"
UPSTREAM = FIXTURES / "upstream"
TOPDIR = "confluent-connect-zos"
PLUGIN = "confluentinc-kafka-connect-ibmmq-zos-12.1.1"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def zos_lint():
    return _load("zos_lint", SCENARIO / "tools" / "zos_lint.py")


@pytest.fixture(scope="session")
def jcl_filter():
    return _load("jcl_filter", SCENARIO / "filter_plugins" / "jcl.py")


def _add(tar, name, data, mode=0o644):
    info = tarfile.TarInfo(name)
    info.size, info.mode = len(data), mode
    tar.addfile(info, io.BytesIO(data))


def build_fake_archives(dest):
    """A Connect on z/OS tarball and an MQ connector zip with the documented
    layout. The launcher scripts are the real apache/kafka 4.1 files, so the
    shebang rewrite is tested against what Kafka actually ships. They are
    stored WITHOUT exec bits so the build's explicit mode is what the bundle
    test sees, on any controller."""
    dest.mkdir(parents=True, exist_ok=True)
    tarball = dest / "confluent-connect-8.1.0-zOS.tar.gz"
    with tarfile.open(tarball, "w:gz") as tar:
        _add(tar, TOPDIR + "/bin/connect-distributed.sh", (UPSTREAM / "connect-distributed.sh").read_bytes(), 0o644)
        _add(tar, TOPDIR + "/bin/kafka-run-class.sh", (UPSTREAM / "kafka-run-class.sh").read_bytes(), 0o644)
        _add(tar, TOPDIR + "/config/connect-distributed.properties", b"bootstrap.servers=localhost:9092\n")
        _add(tar, TOPDIR + "/share/java/kafka/kafka-clients.jar", bytes(range(256)) * 8)
        # A non-ASCII name makes bsdtar's default format write pax headers;
        # only --format=ustar keeps the bundle readable by older z/OS tar.
        _add(tar, TOPDIR + "/share/doc/NOTICE-café.txt", b"notice\n")
    connector = dest / (PLUGIN + ".zip")
    with zipfile.ZipFile(connector, "w") as zf:
        zf.writestr(PLUGIN + "/lib/kafka-connect-ibmmq-zos.jar", bytes(range(256)) * 4)
        zf.writestr(PLUGIN + "/manifest.json", '{"name": "kafka-connect-ibmmq-zos"}')
    return tarball, connector


def fake_keystore(dest):
    """Stand-in for the PKCS12 the Kafka platform team issues. Written per test
    instead of committed: secret scanners flag any .p12 in the tree."""
    dest.mkdir(parents=True, exist_ok=True)
    keystore = dest / "kafka-client.p12"
    keystore.write_bytes(b"\x30\x82" + bytes(range(64)))
    return keystore


def fake_rest_files(dest):
    """Stand-ins for the Connect REST API's stores and the controller's client
    certificate. Build only checks that each is a readable, non-empty file (and
    that the key is private); the REST API tests use real PEMs instead."""
    dest.mkdir(parents=True, exist_ok=True)
    files = {
        "connect_rest_keystore": dest / "kafconn-rest.keystore.p12",
        "connect_rest_truststore": dest / "kafconn-rest-clients.truststore.p12",
        "connect_rest_ca_pem": dest / "rest-ca.pem",
        "connect_rest_client_cert_pem": dest / "controller.cert.pem",
        "connect_rest_client_key_pem": dest / "controller.key.pem",
    }
    for path in files.values():
        path.write_bytes(b"\x30\x82" + bytes(range(64)))
    files["connect_rest_client_key_pem"].chmod(0o600)
    return files


def run_site(workdir, *extra_vars, tags="build", skip_tags="preflight", env=None):
    """Run site.yml against the test inventory with fake archives, keystore and
    REST files. extra_vars come last, so a test can override any of them.
    Returns (proc, out_dir)."""
    tarball, connector = build_fake_archives(workdir / "downloads")
    keystore = fake_keystore(workdir / "downloads")
    rest_files = fake_rest_files(workdir / "downloads" / "rest")
    playbook = Path(sys.executable).parent / "ansible-playbook"
    cmd = [
        str(playbook), "site.yml",
        "-i", "tests/fixtures/inventory.test.yml",
        "-e", "@tests/fixtures/inputs.test.yml",
        "-e", "connect_tarball=%s" % tarball,
        "-e", "mq_source_connector_zip=%s" % connector,
        "-e", "kafka_client_keystore=%s" % keystore,
        "-e", "out_root=%s" % workdir,
        "--tags", tags,
    ]
    for var, path in rest_files.items():
        cmd += ["-e", "%s=%s" % (var, path)]
    if skip_tags:
        cmd += ["--skip-tags", skip_tags]
    for ev in extra_vars:
        cmd += ["-e", ev]
    run_env = dict(os.environ, ANSIBLE_NOCOLOR="1", ANSIBLE_STDOUT_CALLBACK="default", **(env or {}))
    proc = subprocess.run(cmd, cwd=SCENARIO, env=run_env, capture_output=True, text=True, stdin=subprocess.DEVNULL)
    return proc, workdir / "out" / "zos-test"


def run_build(workdir, *extra_vars):
    """Run the build play only (no preflight). Returns (proc, out_dir)."""
    return run_site(workdir, *extra_vars)


@pytest.fixture(scope="session")
def rendered(tmp_path_factory):
    proc, out = run_build(tmp_path_factory.mktemp("render"))
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-2000:]
    return out
