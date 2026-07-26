# ⚔️ Axiom Warp: Two Engines, One Sovereignty
### *Integrity of State, and Integrity of Flow*

Axiom ships two database engines, not one, because it refuses to pretend one execution model fits every workload. `axiom-jdbc` and `axiom-warp-reactive` share a contract, a vocabulary, and a philosophy — but they make opposite bets about *how* work should wait.

---

## 🌊 `axiom-jdbc` — Integrity of State

Built on **Project Loom**: `ScopedValue`-bound connections and `StructuredTaskScope` for parallelism. Every strike runs on a virtual thread that blocks the way JDBC has always blocked — except now blocking is nearly free.

### 1. The Thread Is the Unit of Truth
A `Connection` is clamped to its virtual thread for the life of a call via `BoundScope`. There is exactly one place execution can happen, and exactly one place it's guaranteed to end — success or `Perimeter Breach`.

### 2. Structured, Not Scattered, Concurrency
`WarpScope.joinResults` forks one virtual thread and one connection per parallel strike inside a `StructuredTaskScope`, then joins them all. If any subtask fails, the whole scope fails — no orphaned work, no half-finished batch.

### 3. Bulk by Brute Force
`Hammer` streams a CSV and slams rows into a `PreparedStatement` batch of 1,000 at a time. It's not clever. It's fast, it's synchronous, and you can read every line of it in one sitting.

**The value:** when the unit of work is "do the query, get the answer, move on," there is no faster or more honest way to write it than blocking code on a thread that costs nothing to block.

---

## 🌊 `axiom-warp-reactive` — Integrity of Flow

Built on **Vert.x 5** and `vertx-sql-client`. No thread blocks, ever — every strike is a `Future<Result<T>>`, composed rather than awaited.

### 1. True Asynchronous Composition
`Async.transaction` chains `begin() → businessLogic → commit()/rollback()` with `Future.compose`. `SyncStrike`'s update → delete → insert ordering is the same three-step dance as the blocking version — but expressed as a `flatMap` chain that never occupies a thread while waiting on the database.

### 2. Backpressure Where It Actually Lives
Not a marketing claim across the whole engine — a specific, real mechanism: `DefaultPipeline`'s CSV ingestion uses Vert.x's `RecordParser`. The parser is `pause()`d the instant a 1,000-row batch is full, and only `resume()`d once that batch has actually landed in the database. The file cannot outrun the table.

### 3. Forensics Instead of Faith (The AHE Protocol)
When a strike fails, `SovereignFlow.attachContextOnBreach` doesn't just report an error — it reaches into the `TemporalStreamBuffer`, a rolling window of everything that succeeded recently, and attaches that history to the failure. You don't just learn *that* it broke. You see the state of the world in the seconds before it did.

### 4. A Hard Line Against the Runtime
Vert.x is fast, but its idioms — mutable collections, defensive null-checks, callback state machines — are treated as a contained "hostile environment," not a foundation. `BareVertx` bypasses the defensive public API to call Vert.x's internals directly. The `Ghost Codec` replaces its default JSON layer with a no-op, because Jackson isn't on the classpath and never will be. Nothing crosses the perimeter without being drained into a `PersistentMap` first.

**The value:** when the unit of work is a pipeline — many strikes, composed, streaming, needing to explain itself when it breaks — an engine that never blocks and never forgets what just happened is worth the extra ceremony.

---

## 🧭 Choosing Between Them

| If you need... | Reach for... |
| :--- | :--- |
| A single query, answered and returned | `axiom-jdbc` |
| A handful of strikes joined together, no pipeline | Either — `WarpScope` exists in both |
| A CSV ingested with real backpressure | `axiom-warp-reactive` |
| A failure to explain itself with recent history | `axiom-warp-reactive` (AHE Protocol) |
| The absolute minimum of moving parts | `axiom-jdbc` |
| An engine that never occupies a thread while waiting on I/O | `axiom-warp-reactive` |

---

## ⚔️ The Universal Law

Both engines are **Sovereign**. Neither cares which database sits behind it — `axiom-jdbc` speaks whatever `java.sql.Connection` is handed to it; `axiom-warp-reactive` translates through `Dialect` (`POSTGRES`/`GENERIC` today). Both refuse:

* **Zero Magic:** No ORM, no reflection, no annotations.
* **Pure Data:** Every result is a `PersistentMap`, built through `axiom-core`, never a mutable entity.
* **Explicit Contracts:** Every bound value has a declared `AxiomProtocol` type — nothing is inferred.

---

*"JDBC is for Integrity of State. Axiom Warp is for Integrity of Flow. Sovereignty is not negotiable in either."*
