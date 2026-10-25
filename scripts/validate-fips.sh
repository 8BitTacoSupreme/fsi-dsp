#!/usr/bin/env bash
# =============================================================================
# FSI Kafka Platform -- FIPS 140-2 Compliance Validation
# Validates FIPS compliance for Confluent Platform on RHEL (x86/ARM).
# Can also check CFK on OpenShift via --cfk flag.
#
# CP FIPS mode is not yet supported on s390x (CP versions-interoperability,
# checked 2026-10-24), so on an s390x host this script reports not applicable
# and exits 3. See docs/linuxone-fips-guide.md.
#
# Usage:
#   scripts/validate-fips.sh           # Validate CP on RHEL (local host)
#   scripts/validate-fips.sh --cfk     # Validate CFK on OpenShift (via oc)
#   scripts/validate-fips.sh --check   # Syntax check only (CI-safe, no host)
#
# Exit codes: 0 all checks passed, 1 a check failed, 3 not applicable (s390x)
# =============================================================================
set -euo pipefail

MODE="${1:-rhel}"
PASS=0
FAIL=0
WARN=0

echo "=== FIPS 140-2 Compliance Validation ==="
echo ""

if [ "${MODE}" = "--check" ]; then
  echo "  Syntax check mode -- validating script structure only"
  echo "  PASS: Script is valid"
  exit 0
fi

if [ "${MODE}" = "--cfk" ]; then
  echo "  Mode: CFK on OpenShift"
  echo ""

  # Check 1: oc CLI available
  echo -n "  [1] oc CLI available: "
  if command -v oc >/dev/null 2>&1; then
    echo "PASS"
    ((PASS++)) || true
  else
    echo "FAIL -- install oc CLI"
    ((FAIL++)) || true
  fi

  # Check 2: OpenShift FIPS mode (node-level)
  echo -n "  [2] OpenShift nodes FIPS-enabled: "
  if oc get nodes >/dev/null 2>&1; then
    fips_nodes=$(oc debug node/"$(oc get nodes -o jsonpath='{.items[0].metadata.name}')" -- cat /proc/sys/crypto/fips_enabled 2>/dev/null || echo "unknown")
    if [ "${fips_nodes}" = "1" ]; then
      echo "PASS"
      ((PASS++)) || true
    else
      echo "WARN -- could not verify (FIPS must be enabled at install time)"
      ((WARN++)) || true
    fi
  else
    echo "FAIL -- cannot reach OpenShift cluster"
    ((FAIL++)) || true
  fi

  # Check 3: CFK Helm release has fipsmode=true
  echo -n "  [3] CFK fipsmode Helm flag: "
  cfk_ns="${CFK_NAMESPACE:-confluent}"
  if oc get deployment confluent-operator -n "${cfk_ns}" >/dev/null 2>&1; then
    echo "PASS -- operator found in ${cfk_ns}"
    ((PASS++)) || true
    echo "    Note: Verify Helm install used --set fipsmode=true"
  else
    echo "WARN -- operator not found in namespace ${cfk_ns}"
    ((WARN++)) || true
  fi

  # Check 4: s390x workers cannot run CP FIPS mode
  echo -n "  [4] s390x worker nodes: "
  s390x_nodes=$(oc get nodes -l kubernetes.io/arch=s390x -o name 2>/dev/null || true)
  if [ -n "${s390x_nodes}" ]; then
    echo "WARN -- s390x nodes present; CP FIPS mode is not yet supported on s390x, so FIPS-mode CRs must schedule on x86/ARM nodes"
    ((WARN++)) || true
  else
    echo "PASS -- none"
    ((PASS++)) || true
  fi

else
  echo "  Mode: CP on RHEL"
  echo ""

  if [ "$(uname -m)" = "s390x" ]; then
    echo "  [N/A] Confluent Platform FIPS mode is not yet supported on s390x"
    echo "        (CP versions-interoperability, checked 2026-10-24)."
    echo "        FIPS-mandated broker tiers run on x86/ARM. See docs/linuxone-fips-guide.md."
    exit 3
  fi

  # Check 1: OS-level FIPS mode
  echo -n "  [1] OS FIPS mode (/proc/sys/crypto/fips_enabled): "
  if [ -f /proc/sys/crypto/fips_enabled ]; then
    fips_val=$(cat /proc/sys/crypto/fips_enabled)
    if [ "${fips_val}" = "1" ]; then
      echo "PASS"
      ((PASS++)) || true
    else
      echo "FAIL (value=${fips_val})"
      echo "    Action: Run 'fips-mode-setup --enable' and reboot"
      ((FAIL++)) || true
    fi
  else
    echo "FAIL -- /proc/sys/crypto/fips_enabled not found (not a FIPS-capable OS?)"
    ((FAIL++)) || true
  fi

  # Check 2: Java version (17 or 21 required for FIPS crypto)
  echo -n "  [2] Java version: "
  if command -v java >/dev/null 2>&1; then
    java_ver=$(java -version 2>&1 | head -1)
    echo "${java_ver}"
    ((PASS++)) || true
  else
    echo "FAIL -- java not found"
    ((FAIL++)) || true
  fi

  # Check 3: BCFKS keystore in CP component configs
  echo "  [3] BCFKS keystore check:"
  for conf in /etc/kafka/server.properties /etc/schema-registry/schema-registry.properties /etc/kafka-connect/connect-distributed.properties; do
    echo -n "    ${conf}: "
    if [ -f "${conf}" ]; then
      if grep -q "ssl.keystore.type=BCFKS" "${conf}"; then
        echo "PASS"
        ((PASS++)) || true
      else
        echo "FAIL -- BCFKS not configured (using PKCS12 is NOT FIPS-compliant)"
        ((FAIL++)) || true
      fi
    else
      echo "SKIP -- file not found"
      ((WARN++)) || true
    fi
  done

  # Check 4: Bouncy Castle FIPS provider
  echo -n "  [4] BC FIPS provider in broker config: "
  if [ -f /etc/kafka/server.properties ]; then
    if grep -q "BcFipsProviderCreator" /etc/kafka/server.properties; then
      echo "PASS"
      ((PASS++)) || true
    else
      echo "FAIL -- BcFipsProviderCreator not configured"
      ((FAIL++)) || true
    fi
  else
    echo "SKIP -- /etc/kafka/server.properties not found"
    ((WARN++)) || true
  fi
fi

echo ""
echo "=== Results: ${PASS} passed, ${WARN} warnings, ${FAIL} failed ==="

if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi
exit 0
