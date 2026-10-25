# CFK on OpenShift on LinuxONE (s390x)

Confluent for Kubernetes on OpenShift running on IBM LinuxONE (s390x architecture).

## Prerequisites

- OpenShift Container Platform 4.x on s390x (or mixed-arch cluster with s390x workers)
- CFK operator installed (Helm or OLM)
- Multi-arch container images (Confluent provides s390x variants from CP 8.2.0)

## LinuxONE-Specific Notes

- **FIPS**: Confluent Platform FIPS mode is not yet supported on s390x, so do not set `spec.tls.fips.enabled` on s390x CRs. A tier whose policy requires FIPS 140 validated broker crypto runs its brokers on x86/ARM. See `docs/linuxone-fips-guide.md`.
- **Node affinity**: The Helm values include `nodeAffinity` rules targeting `kubernetes.io/arch: s390x` for mixed-arch clusters. Remove if the cluster is s390x-only.
- **Sidecar images**: Monitoring agents and log shippers may run x86 under QEMU emulation with ~10x throughput penalty. Verify s390x-native builds are available.
- **Multi-arch images**: Confluent multi-arch images include s390x variants. Set `pullPolicy: IfNotPresent` to avoid unnecessary pulls.

## Quick Start

```bash
# 1. Install CFK operator
helm repo add confluentinc https://packages.confluent.io/helm
helm install confluent-operator confluentinc/confluent-for-kubernetes -n confluent

# 2. Apply s390x-specific values
helm upgrade confluent-operator confluentinc/confluent-for-kubernetes \
  -n confluent -f values/kafka.yaml

# 3. Verify pods running on s390x nodes
oc get pods -n confluent -o wide
```

## Troubleshooting

See `docs/linuxone-troubleshooting.md` for s390x-specific debugging guidance.
