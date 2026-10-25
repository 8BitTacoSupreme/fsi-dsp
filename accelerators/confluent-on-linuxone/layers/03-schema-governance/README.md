# Layer 03: Schema Registry Governance

## FSI Rationale

Without schema governance, Schema Registry defaults to `BACKWARD` compatibility — allowing
schema changes that break older consumers. In FSI regulated systems:

- **Regulatory replay**: SEC/FINRA requirement to reconstruct trade events from message logs
  years after the fact. A breaking schema change makes historical records unreadable with
  current consumers — a reportable control failure.
- **Audit trail integrity**: AML/OFAC transaction records must be replayable across any
  version window. `FULL_TRANSITIVE` ensures every registered schema is backward and
  forward compatible with ALL prior versions, not just v(n-1).
- **Hard delete finality**: `?permanent=true` on the SR REST API irreversibly destroys
  schema metadata — there is no recycle bin. Without RBAC enforcement, any authenticated
  user can cause unrecoverable data governance failures.

**Failure mode without this layer:** silent breaking schema change — producers register
an incompatible v2, existing consumers break without warning; or a hard-delete wipes
compliance-relevant schema history permanently.

## What this layer does

1. **`configOverrides.server`**: sets `schema.compatibility.level=full_transitive` as
   the server-side default. This applies to all subjects that do not have an explicit
   override.

2. **Bootstrap Job** (`job-sr-bootstrap.yaml`): an idempotent `restartPolicy: Never` Job
   that PUTs `FULL_TRANSITIVE` to `/config` and `READWRITE` to `/mode` via the SR REST API.
   This is the one sanctioned explicit initialization step — required because configOverrides
   may not take effect until SR restart, and the REST API confirms the active running config.

3. **Hard delete enforcement**: controlled by withholding `ResourceOwner` roles broadly.
   The `?permanent=true` query parameter can only be used by principals with
   `ResourceOwner` on the subject. Layer 01 (RBAC) scopes `schema-admin` to a PREFIXED
   namespace — a `schema-admin` for `payments.` subjects cannot hard-delete a `fraud.`
   subject.

4. **Subject naming pattern**: configurable per overlay via `$(SUBJECT_NAME_PATTERN)`.
   Prod enforces `^[a-z][a-z0-9-]*\.[a-z][a-z0-9-]*-(key|value)$` (domain.entity pattern);
   dev relaxes to allow test subjects.

## Hard delete enforcement is RBAC-based, not a CR flag

There is no single CR field to disable hard deletes in Schema Registry. The enforcement
boundary is layer 01's `schema-admin` `ConfluentRolebinding` with `ResourceOwner` scoped
to a PREFIXED subject namespace:

- `schema-admin` for `payments.` can hard-delete `payments.transaction-value` ✓
- `schema-admin` for `payments.` cannot hard-delete `fraud.alert-value` (403) ✓
- `consumer-only` cannot hard-delete any subject (403) ✓

Do not grant `SystemAdmin` or broad `ResourceOwner` on Subject to any service account
that is not explicitly responsible for schema lifecycle management.

## Cross-references

- `docs/adr/002-compatibility-by-tier.md` — SLA-tier to compatibility-mode mapping
- `docs/adr/013-linuxone-deployment-guidance.md` — s390x platform decisions
- `layers/01-rbac/` — RBAC boundary that enforces hard-delete restriction
- `KNOWN-GAPS.md` — SR bootstrap Job s390x image requirement (UBI9 + curl)

## Validation

```bash
bash layers/03-schema-governance/validate-schema-governance.sh
```

Cluster-dependent. Asserts: global compatibility = FULL_TRANSITIVE; incompatible schema
change rejected; hard-delete by non-privileged principal rejected with 403.
