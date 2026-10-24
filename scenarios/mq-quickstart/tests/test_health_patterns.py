"""The health play greps the worker logs with every health_log_patterns entry
(Ansible's search test is Python re.search). Each pattern must compile, and the
Kafka-side ones must match what Kafka 4.1 actually logs, or the hint never shows."""
import re

import pytest
import yaml

from conftest import SCENARIO

PATTERNS = yaml.safe_load((SCENARIO / "vars" / "derived.yml").read_text())["health_log_patterns"]

# (log line, a word the matching hint must contain). The first two are
# NetworkClient's own messages in Kafka 4.1; the others are JSSE and Kafka
# exception texts.
LOG_LINES = [
    ("WARN [Producer clientId=x] Connection to node -1 (kafka-0.cp.test/10.1.2.3:9093) could not be "
     "established. Node may not be available. (org.apache.kafka.clients.NetworkClient)", "advertised"),
    ("ERROR [AdminClient clientId=x] Connection to node -1 (kafka-0.cp.test/10.1.2.3:9093) failed "
     "authentication due to: SSL handshake failed (org.apache.kafka.clients.NetworkClient)", "certificate"),
    ("org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed", "certificate"),
    ("javax.net.ssl.SSLHandshakeException: No subject alternative DNS name matching kafka-0.cp.test found.", "SAN"),
    ("org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [t]",
     "kafka-access.sh"),
    ("org.apache.kafka.common.errors.GroupAuthorizationException: Not authorized to access group: g",
     "kafka-access.sh"),
]


@pytest.mark.parametrize("entry", PATTERNS, ids=[p["pattern"][:40] for p in PATTERNS])
def test_every_pattern_compiles(entry):
    re.compile(entry["pattern"])
    assert entry["hint"]


@pytest.mark.parametrize("line,word", LOG_LINES, ids=[w + ":" + l[:30] for l, w in LOG_LINES])
def test_kafka_failures_get_the_right_hint(line, word):
    hints = [p["hint"] for p in PATTERNS if re.search(p["pattern"], line)]
    assert any(word in h for h in hints), hints
