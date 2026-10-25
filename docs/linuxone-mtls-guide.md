# LinuxONE mTLS Setup Guide

Step-by-step guide for provisioning mTLS certificates on Confluent Platform running on IBM LinuxONE (s390x).

## Prerequisites

1. **Generate on target**: All private keys and keystores must be generated on the target host, not the Ansible controller. This keeps private keys off the controller and keystores compatible with the target JDK.
2. **PKCS12 only**: Do not use JKS. PKCS12 is the industry standard and loads on both OpenJDK and IBM Semeru.
3. **Explicit algorithms**: Always specify `-keyalg RSA -keysize 2048 -sigalg SHA256withRSA` explicitly. Do not rely on JDK defaults, which differ between OpenJDK and IBM Semeru.
4. **SAN requirements**: Every broker cert must include DNS SANs for FQDN and short hostname, plus IP SAN. Missing SANs cause `ssl.endpoint.identification.algorithm=HTTPS` to reject connections.

## Automated Deployment

The `cp_mtls` Ansible role automates the full certificate lifecycle:

```bash
cd scenarios/cp-rhel-linuxone/
ansible-playbook --ask-vault-pass playbooks/deploy-mtls.yml
ansible-playbook --ask-vault-pass playbooks/verify-mtls.yml
```

See `scenarios/cp-rhel-linuxone/README.md` for quickstart instructions.

## Manual Steps (Reference)

### 1. CA Generation

On the CA host:

```bash
# Generate CA private key (RSA 4096, passphrase-protected)
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 \
  -aes256 -pass pass:$CA_PASSPHRASE -out ca-key.pem

# Generate self-signed CA certificate (10-year validity)
openssl req -new -x509 -key ca-key.pem -passin pass:$CA_PASSPHRASE \
  -days 3650 -subj "/CN=FSI-Kafka-Root-CA/O=FSI/OU=Platform" \
  -out ca-cert.pem
```

### 2. Broker Keystores

On each broker:

```bash
# Generate broker private key
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out broker0.key.pem

# Generate CSR with SANs
openssl req -new -key broker0.key.pem \
  -subj "/CN=broker0.kafka.fsi.internal/O=FSI/OU=Platform" \
  -addext "subjectAltName=DNS:broker0.kafka.fsi.internal,DNS:broker0,IP:10.20.30.40" \
  -addext "extendedKeyUsage=serverAuth,clientAuth" \
  -out broker0.csr
```

Sign on CA host, then assemble PKCS12 keystore on broker:

```bash
openssl pkcs12 -export -in broker0-cert.pem -inkey broker0.key.pem \
  -certfile ca-cert.pem -name broker0 \
  -password pass:$KEYSTORE_PASSWORD \
  -out broker0.keystore.p12
```

### 3. Truststore

On every host:

```bash
keytool -importcert -keystore kafka.truststore.p12 -storetype PKCS12 \
  -storepass $TRUSTSTORE_PASSWORD -alias CARoot -file ca-cert.pem -noprompt
```

### 4. Client Keystores

Same as broker, but with `clientAuth` EKU only. CN becomes the Kafka principal for ACL mapping.

### 5. Broker Configuration

Add to `server.properties`:

```properties
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/ssl/broker0.keystore.p12
ssl.keystore.password=<password>
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.p12
ssl.truststore.password=<password>
ssl.client.auth=required
ssl.endpoint.identification.algorithm=HTTPS
```

### 6. Client Configuration

```properties
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/ssl/client.keystore.p12
ssl.keystore.password=<password>
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.p12
ssl.truststore.password=<password>
security.protocol=SSL
```

### 7. Verification

```bash
# TLS handshake test
openssl s_client -connect broker0.kafka.fsi.internal:9093 \
  -CAfile /etc/kafka/ssl/ca-cert.pem -verify_return_error -brief

# Kafka API version check (confirms broker accepts TLS connections)
kafka-broker-api-versions --bootstrap-server broker0.kafka.fsi.internal:9093 \
  --command-config client-ssl.properties

# Produce/consume roundtrip
echo "test" | kafka-console-producer --bootstrap-server broker0.kafka.fsi.internal:9093 \
  --topic test-mtls --producer.config client-ssl.properties
kafka-console-consumer --bootstrap-server broker0.kafka.fsi.internal:9093 \
  --topic test-mtls --consumer.config client-ssl.properties --from-beginning --max-messages 1
```

### 8. LinuxONE / IBM JDK Gotcha Checklist

| Issue | Symptom | Fix |
|-------|---------|-----|
| Provider order | `NoSuchAlgorithmException` for standard algorithms | Check `java.security` provider order: IBMJCE before SunJCE |
| PKCS12 MAC mismatch | `IOException: keystore was tampered with` | Regenerate with `-J-Dkeystore.pkcs12.macAlgorithm=HmacPBESHA256` |
| Cipher suite intersection | `SSLHandshakeException: no cipher suites in common` | Pin `ssl.cipher.suites` on both sides to a common set |
| TLS 1.3 session resumption | Intermittent handshake failures | Force TLSv1.2 temporarily to diagnose; patch JDK |
| Missing SAN | `CertificateException: No subject alternative names` | Regenerate cert with DNS and IP SANs |
| LPAR time skew | `NotBefore`/`NotAfter` failures | Verify NTP synchronization across all LPARs |

### 9. ACL Setup Post-mTLS

After mTLS is verified, configure ACLs using the CN-extracted principal:

```bash
kafka-acls --bootstrap-server broker0.kafka.fsi.internal:9093 \
  --command-config admin-ssl.properties \
  --add --allow-principal User:fsi-app1 \
  --operation Read --operation Describe \
  --topic corebanking.transactions.v1.account-transaction
```
