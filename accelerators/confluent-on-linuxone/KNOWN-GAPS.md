# Known Gaps — s390x / LinuxONE Accelerator

This document registers every known gap, constraint, or limitation in the
`accelerators/confluent-on-linuxone/` tree. Each entry includes the gap, its
operational impact, the approved workaround, and current status.

---

## Gap Register

| # | Gap | Impact | Workaround | Status |
|---|-----|--------|------------|--------|
| G-01 | Fetch-by-SHA requires network access at activation | `flox activate` (and CI) requires outbound HTTPS to github.com | Pre-fetch in a network-accessible environment and artifact-cache `base/upstream/`; or configure OCP cluster egress to allow github.com access | Open |
| G-02 | CP FIPS mode not yet supported on s390x | No Confluent Platform component on LinuxONE can run CP FIPS mode; layer 02 does not set `spec.tls.fips.enabled` | Tiers whose policy requires FIPS 140 validated broker crypto run brokers on x86/ARM until Confluent lists FIPS mode for s390x; mTLS, TLS 1.2/1.3 and CPACF stay in place | Open |
| G-03 | No native Dynatrace Kafka Connector | Dynatrace does not publish a first-party Kafka Connect connector | HTTP Sink connector (confluentinc/kafka-connect-http) POSTing to Dynatrace generic log ingest API v2; see `layers/04-audit/connect-cr.yaml` | Workaround in place |
| G-04 | Control Center next-gen s390x image availability unverified | C3 next-gen UI may not have a published s390x multi-arch image | Use Control Center classic (included in CP 8.2.0 s390x images) until next-gen image is verified; check `docker manifest inspect confluentinc/confluent-control-center:8.2.0` | Needs verification |
| G-05 | SR bootstrap Job base image must be s390x | `job-sr-bootstrap.yaml` image must run on s390x workers; standard `alpine` or `debian` images may lack s390x variants | Use `registry.access.redhat.com/ubi9/ubi-minimal:latest` — UBI9 has s390x multi-arch manifests; verify with `docker manifest inspect` before deploying | Workaround in place |
| G-06 | x86 node-agent sidecars (Splunk/DT) have QEMU overhead | Running x86 Splunk/Dynatrace forwarder agents as sidecars on s390x nodes uses QEMU binary translation — significant CPU penalty | Route audit events via Kafka Connect (Connect-based SIEM shipping) instead of node-agent sidecars; `layers/04-audit/connect-cr.yaml` implements this pattern | Mitigated by design |
| G-07 | Flox manifest.lock stub must be regenerated | `manifest.lock` in `.flox/env/` is a stub — actual Nix store paths are platform-specific | On a machine with Flox installed, run `flox activate` in `accelerators/confluent-on-linuxone/` to generate a real lock file; commit the result | Open |
| G-08 | Custom s390x Connect image required | `layers/04-audit/connect-cr.yaml` references a custom Connect image with Splunk Sink + HTTP Sink JARs pre-installed for s390x | Build with `docker buildx build --platform linux/s390x`; base image must be CP 8.2.0 Connect s390x image; JAR versions must match connector compatibility matrix | Open |
| G-09 | Cluster Linking schema sync (CP 8.2+) needs verification | Schema Registry Cluster Linking for automatic schema migration between clusters may have s390x-specific behavior | Test schema sync feature in a staging environment; fall back to manual SR migration tool if needed; see `MIGRATION.md` Section 2.4 | Needs verification |
| G-10 | FKO + CMF are parallel Helm operators outside the kustomize build | `kustomize build overlays/{dev,prod}` is no longer self-sufficient — FKO and CMF must be installed first or FlinkApplication CRs will be rejected by the API server (CRDs absent) | Install via RUNBOOK Step 1b: `ansible-playbook ansible/playbooks/flink_operators.yml`; `--check` mode provides audit-only path | Mitigated by design |
| G-11 | CFK 3.2.0+ on s390x officially supports only Flink Applications | Per Confluent's statement: CFK 3.2.0+ on s390x is supported only for managing Flink Applications. Layers 01-04 (Kafka, SR, Connect, C3) are run-at-your-own-risk on s390x | Informational — this accelerator adds layers 01-04 as FSI hardening controls; the supported Flink Application path (layer 05) is the primary s390x CFK use case | Informational |
| G-12 | Custom s390x SQL-runner image required before FlinkApplications start | FlinkApplication CRs reference `<PLACEHOLDER_CP_FLINK_SQL_RUNNER_IMAGE>` — unresolved, pods will fail image pull | Build with `docker buildx build --platform linux/s390x` per `layers/05-flink/sql-runner/README.md`; push to your registry; replace all `<PLACEHOLDER_CP_FLINK_SQL_RUNNER_IMAGE>` references | Open |
| G-13 | Flink checkpoint state encryption — cluster-dependent | `state.checkpoints.dir` in FlinkEnvironment and FlinkApplications references `<PLACEHOLDER_ENCRYPTED_CHECKPOINT_STORAGE_URI>`. Unencrypted checkpoint state is a regulatory finding (PCI-DSS 3.4, GLBA) | Use an OCP StorageClass with `encryption: true` or an S3-compatible endpoint with SSE-KMS; replace placeholder in `layers/05-flink/flink-environment.yaml` | Open |

---

## Gap Detail

### G-01: Fetch-by-SHA network access requirement

**Context:** Locked decision D-01 — upstream code is never committed. `base/fetch-upstream.sh`
clones `mmondics/Confluent-LinuxONE-Mirror` at a pinned SHA at activation time.

**Impact:** CI runners and developer machines must have outbound HTTPS access to
`github.com`. Air-gapped environments are not directly supported.

**Workaround options:**
1. **CI caching**: cache `base/upstream/` as a CI artifact keyed on `UPSTREAM_SHA`
   (most CI systems support artifact caches with SHA-based keys).
2. **Internal mirror**: mirror the upstream repo to an internal GitHub Enterprise or
   Gitea instance; update `UPSTREAM_REPO` in `fetch-upstream.sh` to point there.
3. **OCP egress policy**: ensure OCP cluster egress NetworkPolicy allows outbound HTTPS
   to github.com for CI runner pods.

---

### G-02: CP FIPS mode not yet supported on s390x

**Context:** Confluent's support statement for Linux on IBM Z lists "FIPS mode" under
"The following are not yet supported on s390x" (CP versions-interoperability, "Linux on
IBM Z (s390x) support", checked 2026-10-24 against the 8.3 docs). CFK's
`spec.tls.fips.enabled` turns on CP FIPS mode, so layer 02 does not set it.

**Impact:** No CP component on LinuxONE runs in CP FIPS mode. A tier whose policy requires
FIPS 140 validated crypto on the brokers cannot place its brokers on s390x today; it runs
them on x86/ARM until Confluent lists FIPS mode for s390x. mTLS, TLS 1.2/1.3 and CPACF
acceleration are unaffected.

**OS/JDK FIPS:** OS/JDK FIPS modes on s390x (RHEL `fips=1`, IBM Semeru `IBMJCEPlusFIPS`)
are outside Confluent's s390x support statement; this kit makes no claim that they work
or break with CP.

**Detection:** `layers/02-tls/validate-mtls.sh` fails if `spec.tls.fips.enabled` is set on
the Kafka CR. `tests/ansible/test_s390x_support_floor.py` fails on any s390x manifest or
var file that turns on CP or JVM FIPS mode.

**The earlier rule** (FIPS takes effect only on an OCP cluster installed with `fips: true`)
still holds for x86/ARM OCP clusters running CP FIPS mode. It does not apply here.

**Re-check trigger:** when Confluent lists FIPS mode for s390x, restore the `fips` blocks
in the 02-tls patches, invert the FIPS tests in `test_s390x_support_floor.py` and
`validate-mtls.sh` check 2, mark G-02 Resolved, and change the cflt-ai
`tools/vendor-sources.json` G-02 status in the same commit as that submodule bump.

---

### G-03: No native Dynatrace Kafka Connector

**Context:** Dynatrace does not publish a Confluent-certified Kafka Connect connector.
Monitoring `confluent-audit-log-events` via Dynatrace requires a bridge.

**Workaround:** `kafka-connect-http` (confluentinc) posts JSON batches to Dynatrace's
generic log ingest API v2 (`/api/v2/logs/ingest`). The payload is JSON-formatted audit
events; Dynatrace DPL rules extract structured attributes.

**Limitation:** HTTP Sink is not a purpose-built observability connector — there is no
built-in schema translation. Dynatrace log processing pipelines must be configured to
parse Confluent's audit event schema (documented in `observability/dynatrace/`).

---

### G-04: Control Center next-gen s390x image availability

**Context:** Confluent Control Center v2 ("next-gen") introduced a React-based UI.
Multi-arch s390x images for the next-gen UI may not be in the CP 8.2.0 release.

**Verification command:**
```bash
docker manifest inspect confluentinc/confluent-control-center:8.2.0 \
  | jq '.manifests[].platform | select(.architecture == "s390x")'
```

**Fallback:** Classic Control Center (the existing C3 image) is included in the
`confluentinc/confluent-control-center` multi-arch manifest and is s390x-supported.

---

### G-05: SR Bootstrap Job — s390x base image

**Context:** `layers/03-schema-governance/job-sr-bootstrap.yaml` runs a curl-based
REST initialization Job. The Job must run on s390x workers (node affinity set).

**Verification:**
```bash
docker manifest inspect registry.access.redhat.com/ubi9/ubi-minimal:latest \
  | jq '.manifests[].platform | select(.architecture == "s390x")'
```

UBI9 minimal has s390x in its multi-arch manifest. If the UBI9 image is unavailable
(air-gapped), build a custom `FROM scratch` + static curl binary for s390x.

---

### G-06: SIEM delivery via Connect, not node-agent sidecars

**Context:** Splunk and Dynatrace typically ship data from Kubernetes via host-level
agents (DaemonSets). On s390x nodes, x86 agent binaries require QEMU emulation
(qemu-user-static), which introduces 2-5x CPU overhead.

**Design mitigation:** This accelerator routes audit events via Kafka Connect Sink
connectors — arch-neutral Java running in s390x-native JVM with CPACF acceleration.
Do not add x86 agent sidecars to the CP pods.

---

### G-07: Flox manifest.lock stub

**Context:** Flox lock files contain Nix store paths which are platform-specific and
generated by the Flox daemon. The stub `manifest.lock` in `.flox/env/` is a placeholder.

**Regeneration:**
```bash
cd accelerators/confluent-on-linuxone
flox activate
# Flox resolves packages and writes the real manifest.lock
git add .flox/env/manifest.lock
git commit -m "chore(accelerator): regenerate Flox manifest.lock"
```

---

### G-08: Custom s390x Connect image

**Context:** `layers/04-audit/connect-cr.yaml` references a custom Connect image
(`<PLACEHOLDER_S390X_CONNECT_IMAGE>`) with Splunk Sink and HTTP Sink JARs pre-installed.

**Build process:**
```bash
docker buildx build \
  --platform linux/s390x \
  --build-arg BASE=confluentinc/confluent-kafka-connect:8.2.0 \
  -f connect.Dockerfile \
  -t <YOUR_REGISTRY>/fsi-connect-s390x:8.2.0 \
  --push .
```

The `connect.Dockerfile` should download connector JARs (arch-neutral Java) from
Confluent Hub and install them into the CFK Connect image base.

---

### G-10: FKO + CMF parallel Helm operators

**Context:** `layers/05-flink/` introduces FlinkApplication, FlinkEnvironment, and
CMFRestClass CRs that require FKO and CMF CRDs to be registered before `kustomize build`
output can be applied. The overlay is no longer self-sufficient as a `kustomize build | oc apply`.

**Impact:** Applying the kustomize output without FKO + CMF installed results in
`no kind "FlinkApplication" is registered` API server errors.

**Workaround:** RUNBOOK Step 1b installs FKO then CMF via the `flink_operators` Ansible role
(readiness-gated, `wait: true`). The `--check` playbook invocation provides a GET-only audit
path for compliance verification without mutations.

```bash
ansible-playbook ansible/playbooks/flink_operators.yml          # install
ansible-playbook ansible/playbooks/flink_operators.yml --check  # audit
```

---

### G-11: CFK s390x support scope — Flink Applications only

**Context:** Confluent's official statement for CFK 3.2.0+ on s390x (IBM LinuxONE):
CFK is supported on s390x only for managing Flink Applications. All other CFK-managed
components (Kafka, SchemaRegistry, Connect, ControlCenter, KsqlDB) are not officially
supported on s390x in CFK 3.2.x.

**Impact:** Layers 01-04 (RBAC, mTLS, SR governance, audit logging) apply to the Kafka,
SR, and Connect CRs — these components are run-at-your-own-risk on s390x.

**Informational:** This accelerator adds layers 01-04 as FSI hardening controls based on
the working Mondics reference (CP 8.2.0 on s390x without FSI hardening). Layer 05 (Flink)
is the officially supported CFK on s390x path and is the primary accelerator value.

---

### G-12: Custom s390x SQL-runner image

**Context:** FlinkApplication CRs reference `<PLACEHOLDER_CP_FLINK_SQL_RUNNER_IMAGE>`.
This is a custom image based on `confluentinc/cp-flink:1.20.0-cp1` with
`flink-sql-connector-kafka` (3.2.0-1.20) and `flink-sql-avro-confluent-registry` (1.20.0)
pre-installed, plus the GoodLabs `fsi-sql-runner.jar`.

**Build process:**

```bash
docker buildx build \
  --platform linux/s390x \
  -f layers/05-flink/sql-runner/Dockerfile \
  -t <YOUR_REGISTRY>/fsi-flink-sql-runner:1.20.0-cp1 \
  layers/05-flink/sql-runner/ \
  --push
```

See `layers/05-flink/sql-runner/README.md` for full instructions including air-gapped
environments, JAR version matrix, and OCP internal registry push.

---

### G-13: Flink checkpoint state encryption

**Context:** Flink job state is checkpointed to `state.checkpoints.dir`. On a production
LinuxONE cluster, this directory must be encrypted. `flink-environment.yaml` references
`<PLACEHOLDER_ENCRYPTED_CHECKPOINT_STORAGE_URI>` as a reminder.

**Regulatory impact:** Unencrypted checkpoint state may contain sensitive in-flight data
(transaction amounts, account numbers). PCI-DSS 3.4 and GLBA require encryption of
sensitive data at rest, including transient processing state.

**Options:**
1. **OCP encrypted StorageClass**: Create a StorageClass with `encryption: true` (IBM
   LinuxONE LUKS2 or pervasive encryption via CP Assist for Cryptographic Functions — CPACF).
   Set `state.checkpoints.dir: pvc:///flink-checkpoints` using a PVC backed by this StorageClass.
2. **S3-compatible object store with SSE-KMS**: Set `state.checkpoints.dir: s3://bucket/checkpoints`
   with SSE-KMS enabled. Requires `flink-s3-fs-hadoop` or `flink-s3-fs-presto` JAR in the image.
3. **IBM Cloud Object Storage (on-prem)**: For on-prem LinuxONE deployments with IBM COS.

Replace `<PLACEHOLDER_ENCRYPTED_CHECKPOINT_STORAGE_URI>` in `layers/05-flink/flink-environment.yaml`
with the chosen endpoint before applying.
