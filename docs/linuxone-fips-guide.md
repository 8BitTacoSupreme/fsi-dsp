# LinuxONE FIPS Status

Where FIPS stands for Confluent Platform on IBM LinuxONE / Linux on IBM Z (s390x), and
what this kit does about it.

## Position

Confluent Platform FIPS mode is not yet supported on s390x. Confluent's support statement
for Linux on IBM Z lists "FIPS mode" under "The following are not yet supported on s390x"
(docs.confluent.io/platform/current/installation/versions-interoperability, "Linux on IBM Z
(s390x) support", checked 2026-10-24 against the 8.3 docs). The same section sets the s390x
floor at CP 8.2.0 and requires a separately purchased IBM Z software entitlement.

A tier whose policy requires FIPS 140 validated crypto on the brokers therefore runs its
brokers on x86/ARM until Confluent lists FIPS mode for s390x.

## Scope

- Every CP component on Linux on Z / LinuxONE: brokers, KRaft controllers, Schema Registry,
  Connect, ksqlDB, MDS, Control Center. CP FIPS mode (`enable.fips`, `enable.fips.mode`,
  the BC-FIPS JSSE provider, BCFKS keystores) is the mode Confluent supports elsewhere; on
  s390x it is not yet supported.
- z/OS is not a CP platform ("z/OS is not supported"). Kafka Connect on z/OS is a separate,
  certified build; see `scenarios/mq-quickstart`.

## What still holds

- mTLS, TLS 1.2/1.3 and the AES-GCM cipher suites pinned by the accelerator's layer 02.
- CPACF accelerates AES and SHA transparently; no application change is needed.
- PKCS12 keystores with an explicit MAC algorithm, generated on the target host, for
  portability across JDKs (ADR-013).

## OS and JDK FIPS modes

OS/JDK FIPS modes on s390x (RHEL `fips=1`, IBM Semeru `IBMJCEPlusFIPS`) are outside
Confluent's s390x support statement; this kit makes no claim that they work or break with
CP. If a site policy puts LinuxONE LPARs in OS FIPS mode, raise it with Confluent and IBM
support before placing CP there, and record the answer in the BOM.

## What the kit enforces

| Asset | Setting |
|---|---|
| `scenarios/cp-rhel-linuxone/group_vars/all.yml` | `confluent_package_version` 8.2.x, `fips_enabled: false`, no FIPS JVM flags |
| `accelerators/confluent-on-linuxone/layers/02-tls/` | no `spec.tls.fips.enabled` on any CR; `validate-mtls.sh` fails if it is set |
| `scripts/validate-fips.sh` | exits 3 (not applicable) on s390x |
| `tests/ansible/test_s390x_support_floor.py` | fails on an s390x CP version below 8.2.0, and on any s390x manifest or var file that turns on CP or JVM FIPS mode |

## Keystore portability checks (not FIPS)

```bash
# Verify a PKCS12 keystore loads on the target JDK
keytool -list -keystore /etc/kafka/ssl/broker0.keystore.p12 \
  -storetype PKCS12 -storepass "${KEYSTORE_PASSWORD}"
```

If this fails with MAC algorithm errors, regenerate with:

```bash
keytool -importkeystore -srckeystore old.p12 -destkeystore new.p12 \
  -deststoretype PKCS12 \
  -J-Dkeystore.pkcs12.macAlgorithm=HmacPBESHA256 \
  -J-Dkeystore.pkcs12.macIterationCount=100000
```

```bash
# TLS handshake against a broker
openssl s_client -connect broker0.kafka.fsi.internal:9093 \
  -CAfile /etc/kafka/ssl/ca-cert.pem \
  -verify_return_error -brief

# Certificate validation is time-sensitive: keep clock offset under 1 second
chronyc sources -v
```

## Re-check trigger

Re-check the support statement at each engagement kickoff and at each CP minor release.
When Confluent lists FIPS mode for s390x, follow `accelerators/confluent-on-linuxone/KNOWN-GAPS.md`
G-02 ("Re-check trigger") to turn it back on in one change.
