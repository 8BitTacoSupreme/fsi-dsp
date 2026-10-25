#!/usr/bin/env bash
# =============================================================================
# validate-mtls.sh — assert that mTLS controls are applied and CP FIPS mode is
# not set (it is not yet supported on s390x)
# =============================================================================
# CLUSTER-DEPENDENT: run against a real OCP-on-LinuxONE cluster after deploying
# the 02-tls layer. NOT executed in CI.
#
# Prerequisites:
#   - `oc` logged into OCP with confluent namespace access
#   - `kafka-broker-api-versions.sh` or `kcat` available
#   - Valid client cert/key pair signed by the cluster CA
#   - KAFKA_BOOTSTRAP, CA_CERT, CLIENT_CERT, CLIENT_KEY env vars set
#
# Usage:
#   KAFKA_BOOTSTRAP=kafka.confluent.svc.cluster.local:9092 \
#   CA_CERT=/path/to/ca.pem \
#   CLIENT_CERT=/path/to/client.pem \
#   CLIENT_KEY=/path/to/client-key.pem \
#   bash layers/02-tls/validate-mtls.sh
# =============================================================================
set -euo pipefail

: "${KAFKA_BOOTSTRAP:?Set KAFKA_BOOTSTRAP to the Kafka internal mTLS listener endpoint}"
: "${CA_CERT:?Set CA_CERT to the path of the CA certificate PEM}"
: "${CLIENT_CERT:?Set CLIENT_CERT to the path of a valid client certificate PEM}"
: "${CLIENT_KEY:?Set CLIENT_KEY to the path of the client private key PEM}"

NAMESPACE="${NAMESPACE:-confluent}"
PASS=0
FAIL=0

log_pass() { echo "[PASS] $1"; (( PASS++ )) || true; }
log_fail() { echo "[FAIL] $1" >&2; (( FAIL++ )) || true; }

# ---------------------------------------------------------------------------
echo "=== 02-tls validation ==="
echo ""

# 1. Kafka TLS Secret exists with expected keys
echo "--- Check: Kafka TLS Secret exists ---"
if oc get secret kafka-tls -n "${NAMESPACE}" -o jsonpath='{.data.tls\.crt}' &>/dev/null; then
  log_pass "kafka-tls Secret exists with tls.crt"
else
  log_fail "kafka-tls Secret missing or lacks tls.crt"
fi
echo ""

# 2. spec.tls.fips.enabled is NOT set on the Kafka CR: CP FIPS mode is not yet
#    supported on s390x (KNOWN-GAPS.md G-02)
echo "--- Check: CP FIPS mode not set on Kafka CR (s390x) ---"
if fips_enabled=$(oc get kafka kafka -n "${NAMESPACE}" \
     -o jsonpath='{.spec.tls.fips.enabled}' 2>/dev/null); then
  if [ "${fips_enabled}" = "true" ]; then
    log_fail "spec.tls.fips.enabled=true on Kafka CR — CP FIPS mode is not yet supported on s390x (KNOWN-GAPS.md G-02)"
  else
    log_pass "spec.tls.fips.enabled not set on Kafka CR"
  fi
else
  log_fail "Kafka CR 'kafka' not found in namespace ${NAMESPACE}"
fi
echo ""

# 3. Client with no cert → SSL handshake rejected
echo "--- Check: client with NO cert gets SSL handshake rejected ---"
if command -v kcat &>/dev/null; then
  if kcat -b "${KAFKA_BOOTSTRAP}" -L \
       -X ssl.ca.location="${CA_CERT}" \
       2>&1 | grep -qi "ssl handshake\|certificate\|connection refused\|error"; then
    log_pass "No-cert client rejected (SSL handshake failed as expected)"
  else
    log_fail "No-cert client was NOT rejected — mTLS may not be enforcing client auth"
  fi
else
  echo "[INFO] kcat not available — testing with kafka-broker-api-versions.sh approach"
  # Attempt connection without client cert using openssl s_client
  no_cert_result=$(echo "" | openssl s_client \
    -connect "${KAFKA_BOOTSTRAP}" \
    -CAfile "${CA_CERT}" \
    -verify_return_error 2>&1 || true)
  if echo "${no_cert_result}" | grep -qi "handshake failure\|alert\|error"; then
    log_pass "No-cert client rejected at TLS handshake"
  else
    log_fail "No-cert client was not clearly rejected — verify mTLS listener config"
  fi
fi
echo ""

# 4. Client with valid cert → connection succeeds
echo "--- Check: client with valid cert connects successfully ---"
if command -v kcat &>/dev/null; then
  if kcat -b "${KAFKA_BOOTSTRAP}" -L \
       -X ssl.ca.location="${CA_CERT}" \
       -X ssl.certificate.location="${CLIENT_CERT}" \
       -X ssl.key.location="${CLIENT_KEY}" \
       2>&1 | grep -q "Metadata for all"; then
    log_pass "Valid-cert client connected and received broker metadata"
  else
    log_fail "Valid-cert client failed to retrieve broker metadata"
  fi
else
  echo "[INFO] Using openssl s_client for certificate validation"
  valid_result=$(echo "" | openssl s_client \
    -connect "${KAFKA_BOOTSTRAP}" \
    -CAfile "${CA_CERT}" \
    -cert "${CLIENT_CERT}" \
    -key "${CLIENT_KEY}" \
    -verify_return_error 2>&1 || true)
  if echo "${valid_result}" | grep -q "Verify return code: 0"; then
    log_pass "Valid-cert client: TLS handshake successful"
  else
    log_fail "Valid-cert client: TLS handshake failed — check cert chain"
  fi
fi
echo ""

# 5. TLS 1.0/1.1 rejected (only 1.2 and 1.3 allowed)
echo "--- Check: TLS 1.0 connection rejected ---"
tls10_result=$(echo "" | openssl s_client \
  -connect "${KAFKA_BOOTSTRAP}" \
  -CAfile "${CA_CERT}" \
  -cert "${CLIENT_CERT}" \
  -key "${CLIENT_KEY}" \
  -tls1 2>&1 || true)
if echo "${tls10_result}" | grep -qi "alert\|error\|failure"; then
  log_pass "TLS 1.0 connection rejected"
else
  log_fail "TLS 1.0 connection was not rejected — check ssl.enabled.protocols configOverride"
fi
echo ""

# ---------------------------------------------------------------------------
echo "=== Result: ${PASS} passed, ${FAIL} failed ==="
if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi
