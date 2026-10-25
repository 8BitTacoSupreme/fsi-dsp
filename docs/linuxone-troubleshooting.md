# LinuxONE Troubleshooting Guide

Comprehensive debugging reference for Confluent Platform on IBM LinuxONE (s390x).

## Usual Suspects on LinuxONE

| Category | Issue | Detail | Fix |
|----------|-------|--------|-----|
| SSL/JDK | `java.security` provider order | Semeru defaults to IBMJCE + IBMJSSE2, not SunJCE/SunJSSE. Cipher suite names differ, default enabled list is narrower. | Verify provider order in `$JAVA_HOME/conf/security/java.security` |
| SSL/JDK | PKCS12 MAC mismatch | Keystores minted on x86 with an HmacPBESHA1 MAC can fail to load on the s390x host, more so under OS/JDK FIPS policies (outside Confluent's s390x support statement). | Regenerate with `keytool -J-Dkeystore.pkcs12.macAlgorithm=HmacPBESHA256` |
| SSL/JDK | Cross-platform keystore portability | "Minted on x86, broken on s390x" -- regenerate on the target host with explicit algorithm flags. | Use `-keyalg RSA -keysize 2048 -sigalg SHA256withRSA` explicitly |
| SSL/JDK | x86/s390x cipher suite intersection | x86/OpenJDK client vs s390x/IBM JDK broker -- intersection can be empty. | Pin `ssl.cipher.suites` on both sides to a verified common set |
| SSL/JDK | TLS 1.3 edge cases on older Semeru | Session resumption and curve negotiation quirks. | Force TLSv1.2 temporarily to diagnose; patch JDK to latest Semeru |
| SSL/JDK | CEX cards / PKCS#11 | Separate config path: PKCS#11 provider in `java.security`, `ssl.keystore.location=NONE`, CCA vs EP11 mode matters. | Get software keys working first. See `docs/linuxone-cex-guide.md` |
| Non-SSL | Multi-arch image availability | Confluent multi-arch images exist, but sidecars (monitoring agents, log shippers) may run x86 under QEMU emulation -- 10x throughput penalty. | Verify s390x-native sidecar builds |
| Non-SSL | RocksDB JNI for Kafka Streams | Historically the s390x pain point. Verify `linux-s390x` variant in the Streams JAR. | Check JAR contents: `jar tf kafka-streams-*.jar \| grep s390x` |
| Non-SSL | Connector JAR compatibility | Pure Java = fine. JNI dependencies (JDBC drivers, native compression) need s390x builds. | Verify connector JAR native libraries target s390x |
| Non-SSL | Endianness in custom serializers | Hand-rolled ByteBuffer with assumed little-endian = broken on s390x (big-endian). | Use Avro/Protobuf/JSON Schema (endian-safe). Fix custom serializers. |
| Non-SSL | LPAR time skew | Drift > few minutes causes "certificate not yet valid" failures. | Verify NTP on all LPARs: `chronyc sources` or `ntpstat` |
| Non-SSL | Hostname vs IP SAN mismatch | `ssl.endpoint.identification.algorithm=HTTPS` rejects cert if client connects by IP but cert only has DNS SANs. | Add IP SANs to broker certs or ensure clients use DNS names |

## Per-Component TLS Debug

| Component | TLS Surfaces | Debug Knob | Key Log File |
|-----------|-------------|------------|-------------|
| Broker (inter-broker) | 1 | `KAFKA_OPTS="-Djavax.net.debug=ssl:handshake:verbose"` | server.log |
| Schema Registry | 2 (kafkastore + REST listener) | `SCHEMA_REGISTRY_OPTS="-Djavax.net.debug=ssl:handshake"` | schema-registry.log |
| Kafka Connect | 3 (worker + REST API + connector) | `KAFKA_OPTS="-Djavax.net.debug=ssl:handshake"` | connect.log |
| Control Center | 5 (streams + SR + Connect + ksqlDB + interceptors) | `CONTROL_CENTER_OPTS="-Djavax.net.debug=ssl:handshake"` | control-center.log |
| Client apps | 1 | `JAVA_TOOL_OPTIONS="-Djavax.net.debug=ssl:handshake"` or in-code `System.setProperty` | stderr |

### Debug Workflow

1. **Identify the failing surface**: Which component, which connection (broker-to-broker, client-to-broker, SR-to-broker)?
2. **Enable TLS debug**: Set the appropriate `*_OPTS` environment variable and restart the component.
3. **Capture the handshake**: Look for `ServerHello`, `Certificate`, `CertificateRequest`, `Finished` in the log.
4. **Check for**: Missing cipher suite overlap, cert chain validation failure, hostname verification failure, expired cert.
5. **Cross-reference**: Compare the cert's SANs with the connection hostname, the provider's supported cipher suites with the peer's.

### Diagnostic Bundle Script

Collect a diagnostic bundle for support escalation:

```bash
#!/usr/bin/env bash
set -euo pipefail
BUNDLE_DIR="/tmp/kafka-diag-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${BUNDLE_DIR}"

# JDK info
java -version 2>&1 > "${BUNDLE_DIR}/java-version.txt"
java -XshowSettings:all 2>&1 > "${BUNDLE_DIR}/java-settings.txt" || true

# OS FIPS state (diagnostic only; CP FIPS mode is not yet supported on s390x)
cat /proc/sys/crypto/fips_enabled > "${BUNDLE_DIR}/fips-enabled.txt" 2>/dev/null || echo "n/a" > "${BUNDLE_DIR}/fips-enabled.txt"

# Effective Kafka config (redact passwords)
grep -v password /etc/kafka/server.properties > "${BUNDLE_DIR}/server-properties-redacted.txt" 2>/dev/null || true

# Cert chain dump
for cert in /etc/kafka/ssl/*.pem; do
  openssl x509 -in "${cert}" -text -noout > "${BUNDLE_DIR}/$(basename ${cert}).txt" 2>/dev/null || true
done

# Keystore listing
keytool -list -v -keystore /etc/kafka/ssl/*.keystore.p12 -storetype PKCS12 -storepass "" > "${BUNDLE_DIR}/keystore-list.txt" 2>/dev/null || echo "keystore listing failed" > "${BUNDLE_DIR}/keystore-list.txt"

# Handshake log (last 500 lines)
tail -500 /var/log/kafka/server.log > "${BUNDLE_DIR}/server-log-tail.txt" 2>/dev/null || true

# CEX adapter info (if available)
lszcrypt -V > "${BUNDLE_DIR}/cex-adapters.txt" 2>/dev/null || echo "no CEX" > "${BUNDLE_DIR}/cex-adapters.txt"

# NTP status
chronyc sources > "${BUNDLE_DIR}/ntp-sources.txt" 2>/dev/null || ntpstat > "${BUNDLE_DIR}/ntp-status.txt" 2>/dev/null || echo "no NTP info" > "${BUNDLE_DIR}/ntp-status.txt"

# Package into tarball
tar czf "${BUNDLE_DIR}.tar.gz" -C /tmp "$(basename ${BUNDLE_DIR})"
echo "Diagnostic bundle: ${BUNDLE_DIR}.tar.gz"
```
