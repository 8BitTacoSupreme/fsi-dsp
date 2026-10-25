# Layer 01: MDS RBAC Hardening

## FSI Rationale

Confluent Platform with no RBAC runs as an open cluster — any authenticated principal
can produce to or consume from any topic, read any schema, and alter cluster configuration.
In an FSI context this violates:

- **SOX**: segregation of duties requires that trading/risk system engineers cannot access
  their own audit trails
- **FFIEC IT Examination Handbook**: explicit least-privilege and role-separation controls
  required for financial infrastructure
- **PCI-DSS v4.x § 7**: access to cardholder data restricted on a need-to-know basis

**Failure mode without this layer:** unbounded cluster admin — any authenticated user is
effectively a superuser. Audit logs alone cannot compensate for unrestricted access.

## What this layer does

Enables **MDS (Metadata Service)** with an **LDAP IdP** and deploys six
`ConfluentRolebinding` CRs implementing FSI-standard segregation of duties:

| Role CR | Confluent predefined role | Scope | Notes |
|---------|--------------------------|-------|-------|
| `platform-admin` | `SystemAdmin` | cluster | Ops team only |
| `topic-admin` | `ResourceOwner` | Topic PREFIXED | Domain-scoped |
| `producer-only` | `DeveloperWrite` | Topic+TransactionalId PREFIXED | Cannot consume |
| `consumer-only` | `DeveloperRead` | Topic+Group PREFIXED | Cannot produce |
| `schema-admin` | `ResourceOwner` | Subject PREFIXED (SR cluster) | Schema namespace only |
| `auditor-readonly` | `DeveloperRead` | `confluent-audit-log-events` + SR subjects | **NOT** payments.* |

All role bindings use LDAP **groups** (`type: group`), never individuals. The LDAP
directory is the identity boundary; individual user membership is managed there.

## Locked decision D-02: auditor-readonly is audit-topic-scoped

`auditor-readonly` is bound **only** to:
- `confluent-audit-log-events` topic (LITERAL)
- SR subjects (PREFIXED, all prefixes) — schema metadata only
- Cluster metadata (list/describe only)

It is **explicitly not bound** to `payments.*` or any other business topic. Confluent
`DeveloperRead` always grants consume — binding auditors to business topics would expose
transaction payload. The audit trail is sufficient for forensic purposes without
business data access.

## RBAC drift reconciliation

After initial deployment, use `ansible/roles/cp_rbac` for drift detection:

```bash
ansible-playbook ansible/playbooks/cp_rbac.yml \
  -i inventory/linuxone.yml \
  -e rbac_action=diff
```

The role runs LIST/DIFF/ADD/REMOVE against the declarative `ConfluentRolebinding` CRs.
Run in CI or as a scheduled job to detect manual role additions that bypass GitOps.

## Validation

```bash
bash layers/01-rbac/validate-rbac.sh
```

Cluster-dependent. Requires `confluent` CLI authenticated to MDS.

## Cross-references

- `docs/adr/013-linuxone-deployment-guidance.md` — s390x platform decisions
- `ansible/roles/cp_rbac/` — drift reconciliation tooling
- `layers/04-audit/` — auditor-readonly consumes from the topic this layer restricts it to
- `DESIGN.md` — locked decision D-02 (auditor-readonly scope)
