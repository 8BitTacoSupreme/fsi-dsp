"""
s390x support floor: what Confluent Platform supports on Linux on IBM Z.

Confluent Platform supports s390x from 8.2.0, and lists FIPS mode as not yet
supported on s390x (docs.confluent.io/platform/current/installation/
versions-interoperability, "Linux on IBM Z (s390x) support", checked
2026-10-24). These tests keep the s390x assets inside that statement:
no CP version below 8.2.0, and no CP FIPS mode or FIPS JVM flags.

When Confluent lists FIPS mode for s390x, invert the FIPS tests in the same
change that turns it back on (accelerators/confluent-on-linuxone/KNOWN-GAPS.md G-02).
"""
import glob
import os
import re

import pytest
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CP_S390X_FLOOR = (8, 2, 0)

# Var files that target s390x, and the key in each that names the CP version.
VERSION_FILES = {
    "scenarios/cp-rhel-linuxone/group_vars/all.yml": "confluent_package_version",
    "ansible/inventories/linuxone-demo/group_vars/all.yml": "cp_version",
}

# Every var file whose settings land on s390x hosts.
S390X_VAR_FILES = sorted(
    glob.glob(os.path.join(REPO_ROOT, "scenarios/cp-rhel-linuxone/group_vars/*.yml"))
    + [os.path.join(REPO_ROOT, "ansible/inventories/linuxone-demo/group_vars/all.yml")]
)

# JVM flags and settings that turn on a FIPS provider or FIPS keystores.
FIPS_SETTING_RE = re.compile(
    r"com\.ibm\.fips\.mode|semeru\.fips|java\.security\.properties=\S*fips"
    r"|BcFipsProviderCreator|BCFIPS|BCFKS|enable\.fips\s*=\s*true",
    re.IGNORECASE,
)

# Manifests under these paths deploy to s390x even without an arch selector.
LINUXONE_PATH_RE = re.compile(r"^(accelerators/confluent-on-linuxone/|scenarios/[^/]*linuxone[^/]*/)")
SCAN_ROOTS = ("accelerators", "scenarios")
SKIP_DIRS = {"upstream", ".flox"}


def cp_version(text):
    """Parse 'MAJOR.MINOR.PATCH' into a tuple of ints; None if it is not one."""
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", str(text))
    return tuple(int(part) for part in match.groups()) if match else None


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f) or {}


def walk(node, path=()):
    """Yield (key path, value) for every leaf and every mapping entry."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield path + (str(key),), value
            yield from walk(value, path + (str(key),))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield path + (str(index),), value
            yield from walk(value, path + (str(index),))


def truthy(value):
    return value is True or (isinstance(value, str) and value.strip().lower() in ("true", "yes", "on", "1"))


def fips_findings(doc):
    """Settings in one parsed YAML document that turn on CP or JVM FIPS mode."""
    found = []
    for keys, value in walk(doc):
        leaf = keys[-1].lower()
        if leaf == "fips" and isinstance(value, dict) and truthy(value.get("enabled")):
            found.append(".".join(keys) + ".enabled: true")
        elif "fips" in leaf and truthy(value):
            found.append(".".join(keys) + ": " + str(value))
        elif isinstance(value, str) and FIPS_SETTING_RE.search(value):
            found.append(".".join(keys) + ": " + value)
    return found


def scan_manifests():
    """Parse every YAML document under accelerators/ and scenarios/."""
    docs = []
    for root in SCAN_ROOTS:
        for dirpath, dirnames, filenames in os.walk(os.path.join(REPO_ROOT, root)):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for name in sorted(filenames):
                if not name.endswith((".yml", ".yaml")):
                    continue
                path = os.path.join(dirpath, name)
                rel = os.path.relpath(path, REPO_ROOT)
                with open(path) as f:
                    for doc in yaml.safe_load_all(f):
                        if doc is not None:
                            docs.append((rel, doc))
    return docs


def targets_s390x(rel, doc):
    dumped = yaml.safe_dump(doc)
    return "s390x" in dumped or "kubernetes.io/arch" in dumped or bool(LINUXONE_PATH_RE.match(rel))


class TestVersionParse:
    def test_numeric_not_lexical(self):
        assert cp_version("8.10.0") > cp_version("8.2.0")
        assert cp_version("7.7.0") < CP_S390X_FLOOR
        assert cp_version("8.2.0") == CP_S390X_FLOOR
        assert cp_version("latest") is None


class TestS390xVarFiles:
    def test_var_files_exist(self):
        for rel in VERSION_FILES:
            assert os.path.isfile(os.path.join(REPO_ROOT, rel)), rel
        assert len(S390X_VAR_FILES) >= 4, S390X_VAR_FILES

    @pytest.mark.parametrize("rel,key", sorted(VERSION_FILES.items()))
    def test_cp_version_meets_s390x_floor(self, rel, key):
        value = load_yaml(os.path.join(REPO_ROOT, rel)).get(key)
        assert value is not None, f"{rel}: {key} is missing"
        assert cp_version(value) is not None, f"{rel}: {key}={value!r} is not MAJOR.MINOR.PATCH"
        assert cp_version(value) >= CP_S390X_FLOOR, (
            f"{rel}: {key}={value} -- Confluent Platform supports s390x from 8.2.0"
        )

    @pytest.mark.parametrize("path", S390X_VAR_FILES, ids=lambda p: os.path.relpath(p, REPO_ROOT))
    def test_no_fips_settings(self, path):
        findings = fips_findings(load_yaml(path))
        assert not findings, (
            f"{os.path.relpath(path, REPO_ROOT)}: CP FIPS mode is not yet supported on s390x: {findings}"
        )


class TestS390xManifests:
    def test_scan_not_vacuous(self):
        docs = scan_manifests()
        s390x_docs = [d for d in docs if targets_s390x(*d)]
        assert len(docs) >= 100, len(docs)
        assert len(s390x_docs) >= 10, len(s390x_docs)

    def test_no_cp_fips_mode_on_s390x(self):
        findings = []
        for rel, doc in scan_manifests():
            if targets_s390x(rel, doc):
                findings += [f"{rel}: {item}" for item in fips_findings(doc)]
        assert not findings, "CP FIPS mode is not yet supported on s390x:\n" + "\n".join(findings)
