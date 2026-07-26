# 🌊 THE WARP-REACTIVE LEXICON

> "We do not wait for the metal to answer; we compose the moment it does."

This document defines the high-density terminology of the **Axiom-Warp-Reactive** ecosystem — the non-blocking, Vert.x-hosted counterpart to `axiom-jdbc`. Same core vocabulary (Strike, Breach, Sovereign), extended with the language of composition, arming, and containment that a non-blocking engine demands.

---

## 🏗️ 1. RESOURCE CONTROL (The Provision)

| Term | Definition | Mechanical Reality |
| :--- | :--- | :--- |
| **Provision** | The act of supplying raw resources. | Direct access to a Vert.x `Pool` — no framework bean lifecycle, no proxying. |
| **RawProvisioner** | The reactive connection handler. | Validates pool config against the data contract, then exposes `run`/`runAtomic` gateways over a Vert.x `Pool.withConnection`. |
| **Sovereign Gate** *(provisioning)* | The isolation boundary. | `RawProvisioner` is described in its own source as "the Sovereign Gate" — it isolates the raw `Pool` and enforces the AHE containment boundary before anything touches business logic. |

---

## 🛡️ 2. EXECUTION BOUNDARIES (The Perimeter)

| Term | Definition | Mechanical Reality |
| :--- | :--- | :--- |
| **Async** | The reactive perimeter guard. | The only code permitted to touch a raw `SqlConnection`. `Async.use` hands a client to your logic; `Async.transaction` wraps it in `begin()`/`commit()`/`rollback()`. |
| **Warp** | The gateway leap. | `AxiomWarp` — the single facade for reads, writes, ingestion, sync, parallel strikes, and history-aware queries. Built via a fluent protocol: `protocol().withFactory(...).withCache(...).withDialect(...)`. |
| **WarpScope** | The parallel fork. | `WarpScope.forkAndJoin` — builds one strike per instruction and fans them out over a single connection, joined via Vert.x `CompositeFuture`. Not thread-per-task; it's event-loop composition. |
| **ParallelGateway** | The coordination contract. | Ensures every parallel instruction is routed through a sovereign gateway (`Async::use` or `Async::transaction`) rather than touching a connection directly. |

---

## 🔨 3. DATA MOVEMENT (The Forge)

| Term | Definition | Mechanical Reality |
| :--- | :--- | :--- |
| **The Forge** | The definition-chamber. | `Forge` — builds *unexecuted* strike/batch/sync definitions from SQL + contract + data. Nothing runs until it's armed. |
| **Arm / Armable** | Deferred execution. | `ArmableStrike.arm(SqlClient)`, `ArmableBatch.arm(...)`, `ArmableSync.arm(...)` — a strike is a pure function waiting for a connection. `AxiomWarp` decides *when* and *under what transaction* it fires. |
| **Dialect** | The SQL accent. | `POSTGRES` / `GENERIC` — rewrites Axiom's `?` placeholders into the target driver's native marker (`$1`, `$2`, …) at forge time. |
| **Pipeline** | The streaming ingestion contract. | `Pipeline` / `DefaultPipeline` — reads a CSV via Vert.x's async filesystem and `RecordParser`, with `map`/`filter` transform stages before batching into the table. |
| **Sync Strike** | Delta mirroring. | `Forge.withDialectForSync` — mirrors a `MapDelta` to a table across update → delete → insert, composed with `Future.flatMap`, not sequential blocking calls. |

---

## 🧪 4. VALIDATION & DIAGNOSTICS (The Breach)

| Term | Definition | Mechanical Reality |
| :--- | :--- | :--- |
| **Sovereign Gate** *(execution)* | The pre-execution checkpoint. | `SovereignGate.execute` — verifies contract/plan/data alignment (`IngressIntegrity.verifyAlignment`) before any `ExecutionEngine` binds or runs a strike. |
| **AHE Protocol** | The containment law. | Axiom-Hostile-Environment: the formal boundary between the Axiom Core (pure, immutable) and the Vert.x runtime (fast, mutable, "untrusted zone"). Governs draining, allocation, perimeter access, and data sovereignty — see `AHE_Protocol.md`. |
| **TemporalStreamBuffer** | The black-box recorder. | A rolling, time-windowed queue of successful strike results, kept in memory so the system can look back at the recent past when something breaks. |
| **SovereignFlow** | The forensic attach point. | `SovereignFlow.attachContextOnBreach` — on failure, pulls the buffer's recent history and appends it to the failure message as a "PRE-FAILURE SNAPSHOT." On success, it silently logs the event instead. |
| **withHistory** | The evidence switch. | `AxiomWarp.withHistory(Duration)` — reconfigures every read/write/parallel call on the returned `WarpStrike` to run through `SovereignFlow`, with a hard guard against requesting a lookback longer than the buffer's configured window. |
| **Breach** | Any failure state. | Consistent with `axiom-jdbc` — but here, breaches can carry forensic context instead of just an error string. |

---

## 🕳️ 5. CONTAINMENT (The Wilderness)

| Term | Definition | Mechanical Reality |
| :--- | :--- | :--- |
| **Untrusted Zone** | Vert.x itself. | Per the AHE Protocol: fast, but built on mutable collections and imperative callbacks that contradict Axiom's sovereign design — treated as hostile territory to be contained, not trusted. |
| **Drain Protocol** | Immediate extraction. | Data must never linger in a Vert.x container. The instant a `Future`/`RowSet` resolves, it's folded into a `PersistentList`/`PersistentMap`/`Result` via Axiom's transient builders. |
| **BareVertx** | The direct-access gateway. | Strips Vert.x's defensive public API (list conversions, null-checks, state machines) and calls its internal implementations (`SucceededFuture`, `CompositeFutureImpl`) directly — zero allocation, zero fluff. |
| **The Ghost Codec** | The JSON neutralizer. | `AxiomGhostCodec`/`AxiomGhostFactory` — registered via Vert.x's SPI to replace its default JSON layer with a no-op. Since Jackson is excluded from the build entirely, this is enforcement, not convenience. |

---

## ⚔️ THE COMMANDMENTS OF WARP (REACTIVE)

1.  **No Magic:** If you cannot see the `SqlClient` being passed, it is a lie.
2.  **No Reflection:** We materialize data through explicit converters, not by hallucinating fields.
3.  **No Hibernate, No Jackson:** We speak SQL and PersistentMaps. The Ghost Codec proves it.
4.  **Nothing Fires Unarmed:** Every strike is a definition until `.arm(client)` is called.
5.  **The Wilderness Stays Outside:** Vert.x handlers and callbacks are transport only — business logic lives in the Axiom domain, drained the instant a `Future` resolves.
