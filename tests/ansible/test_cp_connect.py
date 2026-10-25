"""
Unit tests for the cp_connect Ansible role.

Tests cover role structure, default variables, meta information, task files
for deploy (PUT idempotent), validate (health check with retries), check mode
(GET-only), molecule scenario configuration, FQCN enforcement, task name
casing, and Connect REST API mock fixture integrity.
"""
import json
import os
import re

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROLE_DIR = os.path.join(REPO_ROOT, 'ansible', 'roles', 'cp_connect')
TASKS_DIR = os.path.join(ROLE_DIR, 'tasks')
MOLECULE_DIR = os.path.join(ROLE_DIR, 'molecule', 'default')
FIXTURES_DIR = os.path.join(
    REPO_ROOT, 'tests', 'ansible', 'fixtures', 'mock_responses', 'connect'
)


def _load_yaml(path):
    """Load and parse a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f)


def _read_text(path):
    """Read file as raw text."""
    with open(path) as f:
        return f.read()


# ---------------------------------------------------------------------------
# TestRoleStructure -- cp_connect role files and default variables
# ---------------------------------------------------------------------------
class TestRoleStructure:
    """Verify cp_connect role directory structure and defaults."""

    def test_defaults_exist(self):
        path = os.path.join(ROLE_DIR, 'defaults', 'main.yml')
        assert os.path.isfile(path), "defaults/main.yml must exist"
        data = _load_yaml(path)
        assert data is not None

    def test_meta_exist(self):
        path = os.path.join(ROLE_DIR, 'meta', 'main.yml')
        assert os.path.isfile(path), "meta/main.yml must exist"

    def test_meta_role_name(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'meta', 'main.yml'))
        assert data['galaxy_info']['role_name'] == 'cp_connect'

    def test_meta_no_dependencies(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'meta', 'main.yml'))
        assert data['dependencies'] == [], (
            "cp_connect must have no role dependencies"
        )

    def test_task_files_exist(self):
        expected = ['main.yml', 'deploy.yml', 'validate.yml', 'check.yml', 'auth.yml']
        for fname in expected:
            path = os.path.join(TASKS_DIR, fname)
            assert os.path.isfile(path), f"Missing task file: {fname}"

    def test_defaults_cp_connect_url(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'defaults', 'main.yml'))
        assert 'cp_connect_url' in data
        assert data['cp_connect_url'] == 'http://localhost:8083'

    def test_defaults_cp_connectors(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'defaults', 'main.yml'))
        assert 'cp_connectors' in data
        assert data['cp_connectors'] == []

    def test_defaults_cp_connect_validate_health(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'defaults', 'main.yml'))
        assert 'cp_connect_validate_health' in data
        assert data['cp_connect_validate_health'] is True

    def test_defaults_cp_connect_health_retries(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'defaults', 'main.yml'))
        assert 'cp_connect_health_retries' in data
        assert data['cp_connect_health_retries'] == 5

    def test_defaults_cp_connect_health_delay(self):
        data = _load_yaml(os.path.join(ROLE_DIR, 'defaults', 'main.yml'))
        assert 'cp_connect_health_delay' in data
        assert data['cp_connect_health_delay'] == 10


# ---------------------------------------------------------------------------
# TestDeployTasks -- deploy.yml PUT idempotent connector creation
# ---------------------------------------------------------------------------
class TestDeployTasks:
    """Verify deploy.yml uses idempotent PUT for connector management."""

    DEPLOY_PATH = os.path.join(TASKS_DIR, 'deploy.yml')

    def test_deploy_uses_uri_module(self):
        text = _read_text(self.DEPLOY_PATH)
        assert 'ansible.builtin.uri' in text, (
            "deploy.yml must use ansible.builtin.uri"
        )

    def test_deploy_uses_put_method(self):
        text = _read_text(self.DEPLOY_PATH)
        assert 'method: PUT' in text, (
            "deploy.yml must use PUT method for idempotent create-or-update"
        )

    def test_deploy_url_contains_connectors_config(self):
        text = _read_text(self.DEPLOY_PATH)
        assert '/connectors/' in text, (
            "deploy.yml URL must contain /connectors/"
        )
        assert '/config' in text, (
            "deploy.yml URL must contain /config for PUT to config endpoint"
        )

    def test_deploy_accepts_200_and_201(self):
        text = _read_text(self.DEPLOY_PATH)
        assert '200' in text, "deploy.yml must accept 200 (updated)"
        assert '201' in text, "deploy.yml must accept 201 (created)"

    def test_deploy_uses_json_body_format(self):
        text = _read_text(self.DEPLOY_PATH)
        assert 'body_format: json' in text or 'body_format: "json"' in text, (
            "deploy.yml must use body_format: json"
        )

    def test_deploy_uses_loop_var_connector_item(self):
        text = _read_text(self.DEPLOY_PATH)
        assert 'connector_item' in text, (
            "deploy.yml must use loop_var: connector_item"
        )

    def test_deploy_uses_retries(self):
        text = _read_text(self.DEPLOY_PATH)
        assert 'retries:' in text, "deploy.yml must have retries for resilience"


# ---------------------------------------------------------------------------
# TestValidateTasks -- validate.yml health checks with retries
# ---------------------------------------------------------------------------
class TestValidateTasks:
    """Verify validate.yml checks connector health with retries."""

    VALIDATE_PATH = os.path.join(TASKS_DIR, 'validate.yml')

    def test_validate_uses_get_for_status(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'method: GET' in text, (
            "validate.yml must use GET to check connector status"
        )

    def test_validate_checks_status_endpoint(self):
        text = _read_text(self.VALIDATE_PATH)
        assert '/status' in text, (
            "validate.yml must check /connectors/{name}/status"
        )

    def test_validate_has_retries(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'retries:' in text, (
            "validate.yml must retry health checks"
        )

    def test_validate_checks_running_state(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'RUNNING' in text, (
            "validate.yml must check for RUNNING state"
        )

    def test_validate_restarts_failed(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'restart' in text.lower(), (
            "validate.yml must restart FAILED connectors"
        )
        assert 'POST' in text, (
            "validate.yml must use POST for restart"
        )

    def test_validate_restart_includes_tasks(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'includeTasks=true' in text, (
            "validate.yml restart must include includeTasks=true"
        )
        assert 'onlyFailed=true' in text, (
            "validate.yml restart must include onlyFailed=true"
        )

    def test_validate_uses_until(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'until:' in text, (
            "validate.yml must use until: for retry loop"
        )

    def test_validate_uses_delay(self):
        text = _read_text(self.VALIDATE_PATH)
        assert 'delay:' in text, (
            "validate.yml must have delay between retries"
        )


# ---------------------------------------------------------------------------
# TestCheckMode -- check.yml GET-only without mutations
# ---------------------------------------------------------------------------
class TestCheckMode:
    """Verify check.yml uses GET-only operations (no PUT/POST/DELETE)."""

    CHECK_PATH = os.path.join(TASKS_DIR, 'check.yml')

    def test_check_uses_get(self):
        text = _read_text(self.CHECK_PATH)
        assert 'method: GET' in text or 'GET' in text, (
            "check.yml must use GET method"
        )

    def test_check_no_put(self):
        text = _read_text(self.CHECK_PATH)
        assert 'method: PUT' not in text, (
            "check.yml must NOT use PUT (read-only check mode)"
        )

    def test_check_no_post(self):
        text = _read_text(self.CHECK_PATH)
        assert 'method: POST' not in text, (
            "check.yml must NOT use POST (read-only check mode)"
        )

    def test_check_no_delete(self):
        text = _read_text(self.CHECK_PATH)
        assert 'method: DELETE' not in text, (
            "check.yml must NOT use DELETE (read-only check mode)"
        )

    def test_check_uses_changed_when_false(self):
        text = _read_text(self.CHECK_PATH)
        assert 'changed_when: false' in text, (
            "check.yml must use changed_when: false on all GET tasks"
        )


# ---------------------------------------------------------------------------
# TestConnectMolecule -- molecule scenario for cp_connect
# ---------------------------------------------------------------------------
class TestConnectMolecule:
    """Verify molecule scenario for cp_connect role."""

    def test_molecule_yml_exists(self):
        path = os.path.join(MOLECULE_DIR, 'molecule.yml')
        assert os.path.isfile(path), "molecule/default/molecule.yml must exist"

    def test_converge_yml_exists(self):
        path = os.path.join(MOLECULE_DIR, 'converge.yml')
        assert os.path.isfile(path), "molecule/default/converge.yml must exist"

    def test_verify_yml_exists(self):
        path = os.path.join(MOLECULE_DIR, 'verify.yml')
        assert os.path.isfile(path), "molecule/default/verify.yml must exist"

    def test_molecule_uses_delegated_driver(self):
        data = _load_yaml(os.path.join(MOLECULE_DIR, 'molecule.yml'))
        assert data['driver']['name'] == 'delegated'

    def test_converge_includes_cp_connect(self):
        text = _read_text(os.path.join(MOLECULE_DIR, 'converge.yml'))
        assert 'cp_connect' in text, (
            "converge.yml must include cp_connect role"
        )

    def test_converge_sets_mock_url(self):
        text = _read_text(os.path.join(MOLECULE_DIR, 'converge.yml'))
        assert 'cp_connect_url' in text, (
            "converge.yml must set cp_connect_url for mock API"
        )

    def test_verify_checks_results(self):
        text = _read_text(os.path.join(MOLECULE_DIR, 'verify.yml'))
        assert 'cp_connect_results' in text, (
            "verify.yml must check cp_connect_results"
        )


# ---------------------------------------------------------------------------
# TestMainTasks -- main.yml task routing
# ---------------------------------------------------------------------------
class TestMainTasks:
    """Verify main.yml routes to deploy, validate, or check."""

    MAIN_PATH = os.path.join(TASKS_DIR, 'main.yml')

    def test_main_includes_deploy(self):
        text = _read_text(self.MAIN_PATH)
        assert 'deploy.yml' in text, "main.yml must include deploy.yml"

    def test_main_includes_validate(self):
        text = _read_text(self.MAIN_PATH)
        assert 'validate.yml' in text, "main.yml must include validate.yml"

    def test_main_validates_conditionally(self):
        text = _read_text(self.MAIN_PATH)
        assert 'cp_connect_validate_health' in text, (
            "main.yml must conditionally include validate based on "
            "cp_connect_validate_health"
        )

    def test_main_check_mode_routing(self):
        text = _read_text(self.MAIN_PATH)
        assert 'ansible_check_mode' in text, (
            "main.yml must route to check.yml in check mode"
        )

    def test_main_sets_results(self):
        text = _read_text(self.MAIN_PATH)
        assert 'cp_connect_results' in text, (
            "main.yml must set cp_connect_results output variable"
        )


# ---------------------------------------------------------------------------
# TestCpConnectFQCN -- FQCN enforcement across all task files
# ---------------------------------------------------------------------------
class TestCpConnectFQCN:
    """Verify all task files use FQCN for every module reference."""

    BARE_MODULES = [
        'uri:', 'set_fact:', 'debug:', 'assert:', 'fail:',
        'include_tasks:', 'include_role:', 'shell:', 'wait_for:'
    ]

    def test_all_tasks_use_fqcn(self):
        task_files = ['main.yml', 'deploy.yml', 'validate.yml', 'check.yml', 'auth.yml']
        for fname in task_files:
            path = os.path.join(TASKS_DIR, fname)
            if not os.path.isfile(path):
                continue
            text = _read_text(path)
            for bare in self.BARE_MODULES:
                lines = text.split('\n')
                for i, line in enumerate(lines):
                    stripped = line.lstrip()
                    if stripped.startswith('#') or not stripped:
                        continue
                    if stripped.startswith('- name:') or stripped.startswith('name:'):
                        continue
                    if stripped.startswith(bare):
                        assert False, (
                            f"Bare module '{bare}' found in {fname} "
                            f"line {i + 1}: '{stripped}'. "
                            f"Use ansible.builtin.{bare[:-1]} instead."
                        )


# ---------------------------------------------------------------------------
# TestCpConnectTaskNames -- task name casing
# ---------------------------------------------------------------------------
class TestCpConnectTaskNames:
    """Verify all task names start with uppercase per ansible-lint."""

    def test_all_task_names_uppercase(self):
        task_files = ['main.yml', 'deploy.yml', 'validate.yml', 'check.yml', 'auth.yml']
        for fname in task_files:
            path = os.path.join(TASKS_DIR, fname)
            if not os.path.isfile(path):
                continue
            data = _load_yaml(path)
            if not data:
                continue
            for task in data:
                if isinstance(task, dict) and 'name' in task:
                    name = task['name']
                    # Skip Jinja-only names
                    if name.startswith('{{'):
                        continue
                    assert name[0].isupper(), (
                        f"Task name in {fname} must start with uppercase: "
                        f"'{name}'"
                    )


# ---------------------------------------------------------------------------
# TestConnectFixtures -- mock fixture integrity
# ---------------------------------------------------------------------------
class TestConnectFixtures:
    """Verify Connect REST API mock fixtures are valid JSON."""

    def test_connectors_list_valid(self):
        path = os.path.join(FIXTURES_DIR, 'connectors_list.json')
        assert os.path.isfile(path), "connectors_list.json must exist"
        with open(path) as f:
            data = json.load(f)
        assert isinstance(data, list)
        assert len(data) == 2

    def test_connector_status_running_valid(self):
        path = os.path.join(FIXTURES_DIR, 'connector_status_running.json')
        assert os.path.isfile(path)
        with open(path) as f:
            data = json.load(f)
        assert data['connector']['state'] == 'RUNNING'
        assert data['tasks'][0]['state'] == 'RUNNING'

    def test_connector_status_failed_valid(self):
        path = os.path.join(FIXTURES_DIR, 'connector_status_failed.json')
        assert os.path.isfile(path)
        with open(path) as f:
            data = json.load(f)
        assert data['tasks'][0]['state'] == 'FAILED'
        assert 'trace' in data['tasks'][0]

    def test_connector_created_valid(self):
        path = os.path.join(FIXTURES_DIR, 'connector_created.json')
        assert os.path.isfile(path)
        with open(path) as f:
            data = json.load(f)
        assert 'name' in data
        assert 'config' in data
        assert 'tasks' in data
