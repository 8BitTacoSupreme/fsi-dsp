# MQ Quickstart: Kafka Connect on Z with IBM MQ in bindings mode

Automates Confluent's *Connect on Z Quickstart*
([reference/connect-on-z-quickstart-2025-08.pdf](reference/connect-on-z-quickstart-2025-08.pdf))
as an Ansible playbook on IBM's `ibm.ibm_zos_core` collection, with:

- one inputs file
- an SSH preflight that fails fast with a readable report
- a build-time lint that rejects every defect found in the original guide

Steps that need another team's authority (RACF, PROCLIB, Kafka permissions)
are generated and staged for that team, never run. The result is two playbook
runs and a short ticket list (`out/<host>/gates.txt`) in place of a multi-day
back-and-forth with the mainframe team.

Target: Confluent's certified **Connect on z/OS 8.1.0** build with the z/OS MQ
connectors (source 12.1.1, sink 2.1.2), IBM Semeru 17/21, z/OS V2R5 or later.
Kafka: **Confluent Platform over mTLS** (the default; Linux on Z in the same
CEC, an adjacent LinuxONE, or distributed on-prem) or **Confluent Cloud** over
SASL_SSL with an API key.

## Why Connect on Z

The Connect worker runs as a z/OS UNIX started task. Its Java work is
zIIP-eligible (`-Xifa` is on by default), so it is dispatched on zIIP engines
rather than general processors. Because it runs on the same z/OS image as the
queue manager, the MQ connectors can use **bindings mode**: a cross-memory
connection with no channel and no network hop. Confluent's Connect on z/OS
page notes that "bindings mode is more secure than client mode."

A worker on LinuxONE or Linux on Z can only reach MQ in client mode over a
channel. That is the path in [ADR-009](../../docs/adr/009-linuxone-kafka-offload.md).
Here the worker stays on z/OS. The Kafka brokers it feeds can still run on
Linux on Z or LinuxONE (see [Kafka targets](#kafka-targets)).
This scenario always assumes zIIP and bindings, so the preflight fails when no
zIIP is online.

## Kafka targets

`kafka_target` picks the cluster; for Confluent Platform, `cp_placement` says
where it runs relative to this LPAR.

| `kafka_target` / `cp_placement` | Brokers | Network ticket (owner) | Broker-side constraints |
|---|---|---|---|
| `cp` / `same_cec` (default) | CP on Linux on Z (LPAR or z/VM guest) in this CEC | Path inside the CEC: HiperSockets or SMC-D (systems programmer) | s390x: CP 8.2.0 or later, IBM's Linux on Z entitlement, no FIPS mode, no IPv6 |
| `cp` / `linuxone` | CP on an adjacent LinuxONE | SMC-R over RoCE Express, or OSA (network team) | As `same_cec` |
| `cp` / `distributed` | CP on on-prem x86 or ARM | Routed path and firewall rules (network team) | Confluent's general support matrix |
| `cc` | Confluent Cloud | Outbound to the cluster endpoint (network team) | API key over SASL_SSL |

- **Same worker config for all three CP placements.** The worker reaches the
  brokers' mTLS listener with a PKCS12 client keystore and trusts their CA
  through the RACF key ring. Placement changes only the network ticket and who
  owns it. The kit configures none of the transports named above.
- **The listener has to fit the path.** The brokers must advertise addresses
  this LPAR reaches over that path, and their certificate SANs must cover those
  names. This is item 1 of the Kafka platform team's contract in
  `inputs.example.yml`, and the health check names both failures.
- **s390x constraints** come from Confluent's "Linux on IBM Z (s390x) support"
  list. CP is not supported on z/OS itself; this Connect on z/OS build is the
  z/OS path.
- **Version compatibility.** Confluent documents CP 3.2+ Connect workers as
  compatible with CP 3.0+ brokers, so this 8.1.0 worker can feed 8.2+ s390x
  brokers. CP 8.1.x Community support ended 2026-10-15; Enterprise Standard
  support runs to 2027-10-15.
- **Authorization is generated, not applied.** On `cp`, the build writes
  `out/<host>/kafka/kafka-access.sh` for the Kafka platform team. It contains
  RBAC role bindings (`cp_authorizer: rbac`, through MDS) or ACLs (`acls`) for
  the worker principal:
  - the Connect group and its three internal topics;
  - `_confluent-command`, for the connector license;
  - write on the source topic;
  - read on the sink topic and its `connect-<name>` group.

## The pattern

```
  Ansible controller                     z/OS image (one LPAR)                       Kafka (kafka_target)
  ------------------                     -------------------------------------      --------------------
  inputs.yml + vault.yml                 +-----------------------------------+
    |                                    | KAFCONN started task              |
    |-- 0 preflight.sh ---- ssh -------> |   BPXBATCH PGM bash               |
    |                                    |     start-connect.sh              |
    |-- 1 build: repack, render, lint    |       Connect worker JVM -- zIIP  |
    |                                    |         IBM MQ source (bindings) -+-- cp: SSL (mTLS) :9093 --> topic
    |-- 2 install --- sftp (zos_copy) -> |               ^ cross-memory      |   cc: SASL_SSL   :9092 --> topic
    |-- 3 stage --> HLQ.KAFCONN.CNTL     |         queue manager -- queue    |
    |                                    +-----------------------------------+
    |-- 4-6 start / register / health ---- HTTP :8083 --> Connect REST API
    |
    +-- GATED (people, not Ansible); out/<host>/gates.txt lists them:
          RACFSTC + RACFRING .... security team
          PROCCOPY .............. systems programmer
          path to the brokers ... systems programmer (same_cec) or network team; inbound 8083
          kafka-access.sh ....... Kafka platform team (cp)
```

## Steps

| # | Step | Automated? | How | Owner |
|---|---|---|---|---|
| 0 | Preflight | Yes | `preflight.sh` over SSH. Remote checks: z/OS ≥ V2R5, Java 17+, bash, IBM Python 3.12+, ZOAU 1.4.x, MQ `java/lib` (libmqjbnd + jars), SCSQAUTH/SCSQANLE, codeset, free space, a TCP connection to every bootstrap broker, online zIIP. Controller checks: the tarball and zip layouts, and on `cp` the client keystore | you |
| 1 | Build | Yes, controller only | Unpack. Point the launcher shebangs at z/OS bash. Repack as ustar with `--no-xattrs`. Render every file, then lint it. Output goes to `out/<host>/` for review, with `gates.txt` and, on `cp`, `kafka/kafka-access.sh` | you |
| 2 | Install | Yes | `zos_copy` the bundle in binary and extract it untagged. `zos_encode` the two launchers to IBM-1047. Symlink the MQ jars into each connector's `lib/`. Upload the config with per-file encoding. On `cp`, copy the PKCS12 keystore in binary (0600, tagged binary) | you |
| 3 | Stage | Yes, own HLQ | `zos_data_set` + `zos_copy` put the PROC and gated jobs in `<HLQ>.KAFCONN.CNTL` and the CA PEM in `<HLQ>.KAFCONN.CACERT`. The run then prints the ticket checklist and stops | you |
| — | RACF | Generated, gated | `RACFSTC` (STARTED profile) and `RACFRING` (CA, key ring, `IRR.DIGTCERT.LISTRING`) | security team |
| — | PROCLIB | Generated, gated | `PROCCOPY` (IEBCOPY into the target PROCLIB) | systems programmer |
| — | Network | Ticket | The path to every broker for `cp_placement` or Confluent Cloud (the preflight tests each one); inbound REST port (the start step tests it) | systems programmer (`same_cec`) or network team |
| — | Kafka access | Generated, gated (`cp`) | `out/<host>/kafka/kafka-access.sh`: RBAC role bindings or ACLs for the worker principal | Kafka platform team |
| — | zIIP | Check only | The preflight runs `D M=CPU`. Enabling zIIP is a sysprog/licensing decision | sysprog |
| 4 | Start | Yes, after the gates | Read-only gate checks (`mls` PROCLIB, `RLIST STARTED`), then `zos_operator "S KAFCONN"`, then wait for REST | you |
| 5 | Register | Yes | fsi-dsp `ansible/roles/cp_connect`, unchanged: `PUT /connectors/{name}/config`, poll status, restart failed tasks | you |
| 6 | Health | Yes | Every connector and task RUNNING, plus failure signatures in the STDOUT/STDERR log files | you |
| 7 | Stop | On demand | `--tags stop` runs `zos_operator "P KAFCONN"` | you |

## Quick start

```bash
cd scenarios/mq-quickstart

# Controller: ansible-core >= 2.16 (required by ibm.ibm_zos_core 2.1)
ansible-galaxy collection install -r requirements.yml -p ./collections

cp inventory.example.yml inventory.yml     # z/OS host, IBM Python + ZOAU paths
cp inputs.example.yml inputs.yml           # the one inputs file; replace every <PLACEHOLDER_...>
ansible-vault create vault.yml             # cp: vault_kafka_keystore_password; cc: vault_kafka_api_key,
                                           # vault_kafka_api_secret; both: vault_confluent_license

# Run 1: preflight -> build -> install -> stage. Stops at the gates and prints the ticket list.
ansible-playbook site.yml -e @inputs.yml -e @vault.yml --ask-vault-pass

# Tickets, listed in out/<host>/gates.txt: RACFSTC + RACFRING (security), PROCCOPY (sysprog),
# the path to the brokers (sysprog or network), kafka/kafka-access.sh (Kafka platform team, cp).
# The jobs are in <HLQ>.KAFCONN.CNTL; copies for the tickets are in out/<host>/jcl/.

# Run 2: start -> register -> health
ansible-playbook site.yml -e @inputs.yml -e @vault.yml --ask-vault-pass \
  -e gates_confirmed=true --tags start,register,health
```

Useful variations:

- `--tags build --skip-tags preflight`: render and lint only, nothing touches z/OS.
- `--tags health`: re-check a running worker.
- `--tags stop`: stop the task.

## Assets

| Path | What it is |
|---|---|
| [reference/connect-on-z-quickstart-2025-08.pdf](reference/connect-on-z-quickstart-2025-08.pdf) | The original quickstart, unchanged (sha256 `4e7a01cd…fac55`) |
| [site.yml](site.yml) | The playbook, with plays tagged `preflight`, `build`, `install`, `stage`, `start`, `register`, `health`, `stop` |
| [tasks/build.yml](tasks/build.yml) | Controller side: input asserts, repack, render, lint |
| [tasks/install.yml](tasks/install.yml) | Bundle transfer, untagged extract, IBM-1047 conversion, MQ jar symlinks, config upload |
| [tasks/stage.yml](tasks/stage.yml) | Staging library, gated members, CA certificate, ticket checklist |
| [tasks/start.yml](tasks/start.yml) | Read-only gate checks, `S`, REST wait |
| [tasks/health.yml](tasks/health.yml) | Connector/task state, log signatures |
| [preflight.sh](preflight.sh) | SSH preflight. Exit 0 = ok, 1 = any FAIL, 2 = usage |
| [inputs.example.yml](inputs.example.yml) | **The inputs file**, with contract blocks for the MQ team and the Kafka platform team |
| [inventory.example.yml](inventory.example.yml) | z/OS host plus the environment block IBM documents for `ibm_zos_core` |
| [vars/derived.yml](vars/derived.yml) | Every path and name, derived once from the inputs |
| [vars/connectors.yml](vars/connectors.yml) | MQ source/sink connector configs (flat PUT bodies) |
| [templates/connect-distributed.properties.j2](templates/connect-distributed.properties.j2) | Distributed worker |
| [templates/connect.env.j2](templates/connect.env.j2) | STDENV: literal `NAME=VALUE` records |
| [templates/start-connect.sh.j2](templates/start-connect.sh.j2) | Start script; sets no environment |
| [templates/connect-log4j2.yaml.j2](templates/connect-log4j2.yaml.j2) | log4j2 with an IBM-1047 Console appender, WARN+ to STDOUT |
| [templates/KAFCONN.jcl.j2](templates/KAFCONN.jcl.j2) | Started-task PROC: BPXBATCH `PGM`, STEPLIB, STDENV, STDOUT/STDERR to z/OS UNIX files |
| [templates/gated/RACFSTC.jcl.j2](templates/gated/RACFSTC.jcl.j2) | Gated: STARTED profile (IKJEFT1B) |
| [templates/gated/RACFRING.jcl.j2](templates/gated/RACFRING.jcl.j2) | Gated: CA add, key ring, connect, LISTRING permit |
| [templates/gated/PROCCOPY.jcl.j2](templates/gated/PROCCOPY.jcl.j2) | Gated: IEBCOPY the PROC into the PROCLIB |
| [templates/kafka-access.sh.j2](templates/kafka-access.sh.j2) | Gated (`cp`): RBAC role bindings or ACLs for the worker principal, for the Kafka platform team |
| [templates/gates.txt.j2](templates/gates.txt.j2) | The ticket list, written to `out/<host>/gates.txt` |
| [tools/zos_lint.py](tools/zos_lint.py) | Lint of everything z/OS reads; one rule per guide defect |
| [filter_plugins/jcl.py](filter_plugins/jcl.py) | Splits long apostrophe-enclosed JCL values per the JCL Reference (fill to col 71, resume col 16) |
| [tests/](tests/) | pytest: lint rules vs verbatim guide excerpts, preflight vs a fake z/OS, a real build run |
| [ansible.cfg](ansible.cfg), [requirements.yml](requirements.yml), [.ansible-lint](.ansible-lint) | Scenario-local config. No sudo become; `ibm.ibm_zos_core` pinned to 2.1.x |
| [.gitignore](.gitignore) | Keeps `out/` (rendered files hold the keystore password or API secret), `collections/` and the site copies (`inputs.yml`, `inventory.yml`, `vault.yml`) out of git |

## What the kit fixes in the original guide

Each defect is a `tools/zos_lint.py` rule. Each rule is proven to fire on a
verbatim excerpt of the guide (`tests/fixtures/guide/`) or an upstream Kafka
file (`tests/fixtures/upstream/`). The build fails if any rendered file
breaks a rule.

| Defect in the guide | Why it breaks | Kit | Rule |
|---|---|---|---|
| Inline comments in `.properties` (`tasks.max=1 # Adjust as needed`; 14 lines in the distributed example alone) | Java properties have no inline comments; the value becomes `1 # Adjust as needed` | Comments on their own lines | P1 |
| `connect.env` written as a shell script (`export`, `$LIBPATH`, `$KAFKA_HOME`, quotes, `#` lines) | BPXBATCH STDENV is one literal `NAME=VALUE` per record: no expansion, no comments | Fully resolved records | E1–E4 |
| `KAFKA_OPTS` set in both the start script and `connect.env` | One silently overwrites the other: you lose either the keyring or the heap | Environment only in STDENV; the start script sets nothing | S1, E5 |
| `KAFKA_CONNECT_PLUGINS` | Nothing reads it (not in `kafka-run-class.sh` at Kafka 3.5 or trunk) | `plugin.path` only | E6 |
| `-XX:+UseG1GC -XX:+UseStringDeduplication` | HotSpot flags. OpenJ9 ignores unknown `-XX:` silently, and `kafka-run-class.sh` injects G1 flags when `KAFKA_JVM_PERFORMANCE_OPTS` is unset | `KAFKA_JVM_PERFORMANCE_OPTS=-Xgcpolicy:gencon …` | E7, E9 |
| Heap in `KAFKA_OPTS` | `connect-distributed.sh` sets `KAFKA_HEAP_OPTS=-Xms256M -Xmx2G` when unset | `KAFKA_HEAP_OPTS` | E10 |
| `CLASSPATH` export of the MQ jars | Confluent: put `com.ibm.mq.allclient.jar` in the connector `lib/`, "do not export the CLASSPATH". IBM: copying MQ jars elsewhere is unsupported | Symlinks into each `lib/` (Connect's `PluginUtils` follows symlinks) | E6 |
| PROC `PARM=` expands to **165** characters | The JCL PARM limit is 100 | `PARM='PGM <bash> <start script>'`, with the resolved length checked | J4 |
| PROC symbolics `JUSER`, `MQHOME` never used; comments past column 71 | Dead parameters; column 72 is the continuation column | No symbolics; every line ≤ 71 | J3, J1 |
| BPXBATCH `SH` | Under `SH`, `/etc/profile` and `.profile` override STDENV | `PGM` | J6 |
| `rest.port=8082`; "default 8082 for distributed" (also on Confluent's z/OS page) | `rest.port` was removed in Kafka 3.0 (present in `WorkerConfig` @2.8, gone @3.0) and is ignored; the listener default is `http://:8083` | `listeners=http://0.0.0.0:<port>` + `rest.advertised.*` | P3 |
| `RACDCERT CERTAUTH ADD('label') DSN(…)` | `ADD()` takes the data set and the label goes in `WITHLABEL()`; there is no `DSN()` keyword. Input must be a cataloged VB sequential data set | Correct syntax; the PEM is staged as VB | J7 |
| No permission to read the key ring | Reading your own ring needs READ on FACILITY `IRR.DIGTCERT.LISTRING` | `PERMIT` plus refreshes in RACFRING | — |
| `safkeyring://` without a protocol handler | On Semeru 17/21 that needs `java.protocol.handler.pkgs`; `safkeyringjce://` does not | `safkeyringjce://OWNER/RING` | — |
| `producer.*` security commented out as "inherited"; no `confluent.topic.*` | Connect gives connector clients only their prefixed settings (Kafka 4.1 `Worker`), and the license client needs its own connection and security settings | One security block per target, written for the worker, `producer.`, `consumer.` and `confluent.topic.` | — |
| `#!/bin/bash` in Kafka's scripts (the guide's own caveat) | z/OS has no `/bin/bash` | Shebangs rewritten during repack | S2 |
| `POST` with a `{"name","config"}` wrapper | `PUT /connectors/{name}/config` takes the flat map and is create-or-update | Flat PUT bodies | C1 |
| Logs to the JES spool; log4j2 without a charset | Confluent's z/OS page: spool truncation, unreadable logs | STDOUT/STDERR to z/OS UNIX files; IBM-1047 charset | L1 |
| Paths mix 7.5.2 and 7.6.1; "IBM Java 10" | No Java 10 exists on z/OS (8, Semeru 11/17/21/25) | One derived path set; Java checked | — |

One thing the guide did not get wrong: on Confluent's page, `.properties` files
are converted to IBM-1047. Stock Java reads properties as ISO-8859-1, so the
kit keeps that as an input (`connect_props_encoding`). The health check names
"Missing required configuration" as the signature of the wrong choice.

## Delivery semantics and FSI notes

- **At-least-once.** Exactly-once on the MQ source needs connector 13.x, a
  state topic and one task; the z/OS-certified source is 12.1.1. Regulatory
  consumers must be idempotent: dedupe on a business key, not on offsets.
- **Confluent Platform authenticates with mTLS**, as the FSI canon asks.
  - The worker, its producers and consumers, and the connectors' license
    client share one PKCS12 client keystore. Trust comes from the RACF key ring.
  - The private key sits in a z/OS UNIX file (0600, owned by the started task's
    user), not in RACF or ICSF. Kafka opens `ssl.keystore.location` as a plain
    file path (Kafka 4.1 `DefaultSslEngineFactory`), so a `safkeyringjce://`
    ring cannot be the keystore. Keeping the key in RACF or ICSF would need a
    custom `ssl.engine.factory.class`, which the kit does not provide. Get the
    security team's sign-off on the file.
  - Authorization stays with the Kafka platform team: `kafka-access.sh` lists
    the bindings, and the kit never applies them.
  - **FIPS:** Confluent Platform has no FIPS mode on s390x. A broker tier that
    must run in FIPS mode needs the `distributed` placement today.
- **Confluent Cloud authenticates with SASL/PLAIN and an API key**, as in the
  guide. The FSI canon prefers mTLS or OAuth.
- **Secrets on both targets:**
  - Secrets live in Ansible Vault.
  - Rendered files are 0600 under `out/` (gitignored).
  - The connector config, with the license client's credentials, is stored in
    the `-configs` topic. Since CP 8.0.1, REST responses mask password fields.
- **License:** an empty `confluent.license` runs the premium connectors on a
  30-day trial on either target.
- **Converters are `StringConverter`**, as in the guide. Production canon is
  Avro or Protobuf with Schema Registry.
- **The Connect REST API is plain HTTP with no authentication.** Limit inbound
  access to the controller (the firewall ticket).
- **Ordering:** `tasks.max=1` keeps the queue's order.
- **Logs:** STDOUT carries WARN and above, because the file is opened with
  OAPPEND and never rotates. The full INFO log rolls in
  `<base>/logs/connect.log` (10 × 100 MB).

## Verification

Run from this directory with the Python that has ansible-core 2.16+ and the
collection installed:

```bash
python -m pytest tests -v        # 110 tests
ansible-playbook site.yml -i tests/fixtures/inventory.test.yml \
  -e @tests/fixtures/inputs.test.yml --syntax-check
ansible-lint site.yml tasks vars
shellcheck preflight.sh tests/fixtures/bin/ssh
# and on both rendered modes of out/<host>/kafka/kafka-access.sh (rbac, acls)
```

What the tests prove:

- **`test_lint.py`:**
  - Every rule fires on its guide excerpt or upstream file. The guide PROC's
    PARM is measured at 165 characters; its orphan symbolics are named.
  - The 11 rendered files are clean.
  - A run that checks zero files exits 2.
- **`test_jcl_filter.py`:** every value length from 4 to 262 characters, in
  three statement layouts, reads back exactly through the linter's
  independent JCL reader.
- **`test_render.py`:** a real `--tags build` run against fake archives that
  use apache/kafka 4.1's own launcher scripts. It checks:
  - the PROC and STDENV contents;
  - that the start script parses;
  - that the bundle is POSIX ustar with no pax headers, keeps exec bits and
    shebangs, and leaves jars byte-identical;
  - the jvm_default and sink variants;
  - that a placeholder or an over-long install path stops the build;
  - the `cp` default: the mTLS keystore block for the worker, `producer.`,
    `consumer.` and the license client, with no SASL line, and backslashes
    escaped for `.properties`;
  - the `cc` variant keeps the API-key block. The `cp` fixture leaves the CC
    credentials undefined, and the `cc` run leaves the CP inputs undefined, so
    neither path reads the other's secrets;
  - `kafka-access.sh` in RBAC and ACL modes, with and without the sink; a `cc`
    build leaves no `kafka/`;
  - the network ticket for each placement, the Kafka ticket, and the cacerts
    ticket for `cp` with `jvm_default`;
  - that a bad target choice, a missing keystore, a placeholder principal,
    cluster id or CA label, and a `confluent.cloud` bootstrap on the `cp`
    default each stop the build.
- **`test_preflight.py`:**
  - The good image passes all 19 checks (18 on `cc`, which has no keystore).
  - Each of 19 broken prerequisites fails with its own `[FAIL]` line.
  - A missing, unreadable or empty keystore fails on `cp`.
  - Every bootstrap broker is tested, in one call; a broker without its own
    `OPEN` answer fails.
  - Unparseable output fails.
  - An unreachable host skips the rest.
  - The zIIP assertion downgrades to WARN and says so.
  - The playbook's preflight play renders its environment for both targets.
- **`test_health_patterns.py`:** every log signature compiles, and Kafka 4.1's
  own messages (unreachable node, failed TLS authentication, SAN mismatch,
  authorization) each reach the right hint.
- **Mutation check:** 30 mutations for the original kit and 34 for the Kafka
  targets, each breaking one behavior under test in a scratch copy, all turned
  the suite red. A no-op control stayed green.

### Not verified: needs a z/OS LPAR

- Anything `ibm.ibm_zos_core` executes. Module options were checked against
  `ansible-doc` for 2.1.0; behavior was not run.
- BPXBATCH and STDENV behavior on a live system, and z/OS iconv mapping LF to
  NL (X'15') through `zos_copy`.
- Whether Confluent's z/OS build reads IBM-1047 `.properties`. Confluent says
  convert; stock Java reads ISO-8859-1 (`connect_props_encoding`).
- RACF:
  - the jobs themselves;
  - whether `RLIST STARTED KAFCONN.*` works without GENERIC on your release;
  - the authority the installer needs to list the profile (`-e verify_racf=false`
    skips the check and says so).
- That `mls` (ZOAU) output names members the way the start check expects.
- Command output formats (`uname -I`, `D M=CPU`, `zoaversion`, `df -kP`). They
  come from IBM's documented examples, not live captures. Every parser fails
  closed.
- `libmqjbnd*.so` file names in `<mq>/java/lib` (IBM documents only the
  `mqjbnd` library name).
- The MQ sink validator wanting `mq.hostname` in bindings mode on the z/OS
  sink 2.1.2. The sink ships disabled.
- The REST port default in Confluent's z/OS build. The kit sets `listeners=`
  explicitly, so it does not depend on it.

### Not verified: needs a Confluent Platform cluster

- The PKCS12 keystore on z/OS:
  - the binary copy;
  - `chtag -b`;
  - Semeru reading a cp_mtls-issued PKCS12 (HmacPBESHA256 MAC) from z/OS UNIX.
- An mTLS session from the z/OS worker to a CP listener, over any of the three
  placements.
- The generated `kafka-access.sh` against a live MDS or broker. Its commands
  follow Confluent's documentation and pass shellcheck; they have not been run.
- HiperSockets, SMC-D, SMC-R and OSA. These appear only as ticket text: the kit
  configures none of them.

## Sources

- **Confluent (via the confluent-docs MCP):**
  - [Connect on z/OS](https://docs.confluent.io/platform/current/connect/connect-zos.html)
  - IBM MQ Source [overview](https://docs.confluent.io/kafka-connectors/ibmmq-source/current/overview.html) and [config](https://docs.confluent.io/kafka-connectors/ibmmq-source/current/source_connector_config.html)
  - IBM MQ Sink [config](https://docs.confluent.io/kafka-connectors/ibmmq-sink/current/connector_config.html)
  - [Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)
  - [Worker config](https://docs.confluent.io/platform/current/installation/configuration/connect/index.html)
  - [Connect to Confluent Cloud](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html)
  - [License config](https://docs.confluent.io/platform/current/connect/license.html)
  - [Versions and interoperability](https://docs.confluent.io/platform/current/installation/versions-interoperability.html), including "Linux on IBM Z (s390x) support"
  - [Configure RBAC for a Connect cluster](https://docs.confluent.io/platform/current/connect/rbac/connect-rbac-connect-cluster.html) and [for a Connect worker](https://docs.confluent.io/platform/current/connect/rbac/connect-rbac-worker.html)
  - [Kafka Connect security basics](https://docs.confluent.io/platform/current/connect/security.html): worker, connector and license topic ACLs
- **Apache Kafka source:**
  - `bin/kafka-run-class.sh` and `bin/connect-distributed.sh` (3.5, 4.1, trunk)
  - `connect/runtime/.../WorkerConfig.java` (2.8, 3.0) and `RestServerConfig.java` (3.9, 4.0, 4.1)
  - `PluginUtils.java`
  - `FileConfigProvider.java` (4.1)
  - 4.1: `DefaultSslEngineFactory.java`, `Worker.java`, `NetworkClient.java`, `ControllerApis.java`
- **IBM:**
  - `ibm_zos_core` v2.1.0: `collection-requirements.rst`, `release_notes.rst`, module docs, verified with `ansible-doc`
  - z/OS UNIX: BPXBATCH, `uname`, `chtag`, `ls -T`
  - RACF Command Language Reference: RACDCERT ADD, ADDRING, CONNECT; STARTED class; ADDUSER
  - `IRR.DIGTCERT.LISTRING`
  - IKJEFT1B
  - Semeru on z/OS: JSSE `safkeyringjce`, `file.encoding`
  - OpenJ9: `-XX:` handling, `-Xgcpolicy`
  - MQ 9.4 for z/OS: Java bindings, STEPLIB, `java/lib`
  - Java SDK products on z/OS
