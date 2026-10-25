# CP on RHEL on LinuxONE (s390x)

Confluent Platform on RHEL running on IBM LinuxONE (s390x architecture) with automated mTLS certificate provisioning.

## Prerequisites

- IBM LinuxONE LPAR running RHEL 8.x or 9.x (s390x)
- Confluent Platform 8.2.0 or later (the s390x floor) and IBM's Linux on Z software entitlement
- IBM Semeru Runtime 17 (`java-17-ibm-semeru-openj9`) installed on all hosts
- Ansible controller with network access to all target hosts
- `community.crypto >= 2.15.0` collection installed (`ansible-galaxy collection install -r ../../ansible/requirements.yml`)
- `openssl` and `keytool` available on all target hosts

## LinuxONE-Specific Notes

- **JDK**: IBM Semeru 17 is the default. The `java.security` provider order differs from OpenJDK (IBMJCE + IBMJSSE2, not SunJCE/SunJSSE). Cipher suite names and default enabled lists may differ.
- **FIPS**: Confluent Platform FIPS mode is not yet supported on s390x (CP versions-interoperability, checked 2026-10-24), so `fips_enabled` stays `false`. A tier whose policy requires FIPS 140 validated crypto on the brokers runs its brokers on x86/ARM until Confluent lists FIPS mode for s390x. Keep PKCS12 keystores with an explicit MAC algorithm (`-J-Dkeystore.pkcs12.macAlgorithm=HmacPBESHA256`) for keystore portability across JDKs. See `docs/linuxone-fips-guide.md`.
- **CPACF**: IBM CPACF (CP Assist for Cryptographic Functions) accelerates AES, SHA, and other crypto operations transparently -- no application changes needed.
- **CEX**: For hardware-backed private keys via CEX cards, see `docs/linuxone-cex-guide.md`. Get software keys working first.

## Quick Start

```bash
# 1. Install collection dependencies
ansible-galaxy collection install -r ../../ansible/requirements.yml

# 2. Copy and edit inventory
cp inventory/hosts.yml.example inventory/hosts.yml
$EDITOR inventory/hosts.yml

# 3. Copy and encrypt vault
cp group_vars/vault.yml.example group_vars/vault.yml
$EDITOR group_vars/vault.yml
ansible-vault encrypt group_vars/vault.yml

# 4. Deploy mTLS certificates
ansible-playbook --ask-vault-pass playbooks/deploy-mtls.yml

# 5. Verify mTLS
ansible-playbook --ask-vault-pass playbooks/verify-mtls.yml
```

## Troubleshooting

See `docs/linuxone-troubleshooting.md` for the complete per-component TLS debug guide and the "Usual Suspects on LinuxONE" checklist.
