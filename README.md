# 🌊 Axiom Warp (Reactive)

Non-blocking database engine for the **Axiom** framework, built on **Vert.x 5** and `vertx-sql-client`. It is the event-loop counterpart to `axiom-jdbc`: same contract-driven, zero-reflection philosophy, same `PersistentMap`/`PersistentList` result shapes, same "strike" vocabulary — but every operation returns a `Future<Result<T>>` instead of blocking a thread.

Where `axiom-jdbc` is the precision hammer for request/response work, `axiom-warp-reactive` is built for pipelines: composed async chains, streaming ingestion with real backpressure, and forensic diagnostics on failure via the **AHE Protocol**.

---

## Requirements

- **Java 26** (compiled with `--enable-preview`)
- **Vert.x 5.1.5** — `vertx-core`, `vertx-sql-client`
- A reactive SQL driver compatible with `vertx-sql-client` (the built-in `Dialect` enum ships `POSTGRES` and `GENERIC`)
- Axiom core + Axiom spec on the classpath

The build deliberately excludes Netty's HTTP/2, HTTP/3, and QUIC codecs (not needed for a SQL client), and excludes Jackson entirely — see [The Ghost Codec](#the-ghost-codec) below.

---

## 🏛️ Integration

Summon the Specification engine into your project:

**Maven**

```xml 
<dependency>    
     <groupId>com.ensemblu</groupId>   
     <artifactId>axiom-warp-reactive</artifactId>   
     <version>2.0.0</version>  
</dependency>   
```   
**Gradle**

```groovy
 implementation("com.ensemblu:axiom-warp-reactive:2.0.0")   
```

---

## Package Structure

```
dev.axiom.reactive
├── api
│   ├── AxiomWarp.java            // Facade + builder + Strike/Ingest/Sync fingers
│   ├── AxiomWarpBehavior.java    // Full reactive contract (WarpStrike + history + shutdown)
│   ├── WarpStrike.java           // read / write / parallelRead / parallelWrite
│   └── ArmableStrike.java        // A strike definition awaiting a SqlClient
├── buffer
│   └── TemporalStreamBuffer.java // Rolling-window "black box recorder"
├── codec
│   ├── AxiomGhostCodec.java      // No-op JSON codec — neutralizes Vert.x's default
│   └── AxiomGhostFactory.java    // SPI factory for the Ghost Codec
├── engine
│   ├── Forge.java                // Builds strike/batch/sync definitions
│   ├── ReactiveBinder.java       // Binds typed values onto a Vert.x Tuple
│   ├── ReactiveExecutionEngine.java  // Executes a plan against a SqlClient
│   ├── ReactiveResultConverter.java  // RowSet -> PersistentList<PersistentMap>
│   ├── ReactiveResultRow.java    // Single-row navigator/materializer
│   ├── SovereignFlow.java        // AHE Protocol: attaches forensic context on failure
│   ├── core
│   │   ├── ExecutionEngine.java
│   │   └── SovereignGate.java    // Plan + contract verification before execution
│   └── dialect
│       └── Dialect.java          // POSTGRES / GENERIC placeholder translation
├── ingest
│   ├── Pipeline.java             // Streaming CSV ingestion contract
│   └── DefaultPipeline.java      // Vert.x AsyncFile + RecordParser implementation
├── provision
│   └── RawProvisioner.java       // Vert.x Pool bootstrap + run/runAtomic gateways
├── scope
│   ├── Async.java                // Perimeter guard: connection use + transactions
│   ├── ParallelGateway.java      // Contract for parallel strike coordination
│   └── WarpScope.java            // Fork/join of parallel strikes via CompositeFuture
└── syntax
    └── BareVertx.java            // Direct-access gateway, bypasses defensive Vert.x API
```

---

## Getting Started

### 1. Build the engine

```java
AxiomWarp warp = AxiomWarp.protocol()
        .withFactory(RawProvisioner.basedOnConfig(configSource)
                .withPoolProvider(config -> MyPool.from(config))
                .validateRules()
                .getOrThrow())
        .withCache(TemporalStreamBuffer.ofWindowDuration(Duration.ofMinutes(5)))
        // or .withoutCache() to disable AHE forensic history
        .withDialect(Dialect.POSTGRES);
```

### 2. Read

```java
Future<Result<PersistentList<PersistentMap<String, Object>>>> rows =
        warp.strike().shot("SELECT * FROM users").arm(client);
```

`AxiomWarp.read` / `.write` also expose the raw perimeter directly if you need manual `SqlClient` control:

```java
warp.read(client -> warp.strike().shot("SELECT * FROM users").arm(client));
```

### 3. Write (typed, transactional)

```java
warp.write(client ->
        warp.strike()
              .dynamic("INSERT INTO users (id, name) VALUES (:java.id, :java.name)")
              .withContract(Axiom
                          .Data
                          .<String,AxiomProtocol>emptyMap()
                            .put("id",AxiomProtocol.LONG)
                            .put("name",AxiomProtocol.STRING))
              .withData(Axiom
                      .Data
                      .<String,Object>emptyMap()
                            .put("id",1L)
                            .put("name","Ofek"))
              .arm(client)
);
```

`AxiomWarp.write` runs the logic through `RawProvisioner.runAtomic` → `Async.transaction`, which begins a Vert.x transaction, commits on `Result.success`, and rolls back on `Result.failure` or exception.

### 4. Bulk / batch strikes

```java
warp.write(client ->
        warp.strike()
            .bulk("INSERT INTO users (id, name) VALUES (:java.id, :java.name)")
            .withContract(types)
            .withData(listOfRows)
            .arm(client)
);
```

### 5. Stream-ingest a CSV file

```java
warp.ingest()
    .stream("users.csv")
    .usingFileHeaders()
    .onTableName("users")
    .arm(client);   // Future<Result<Long>>
```

`DefaultPipeline` opens the file with Vert.x's async filesystem, parses it line-by-line with `RecordParser`, and applies real backpressure: the parser is `pause()`d while a 1,000-row batch is flushed, and `resume()`d only after the batch insert succeeds. `map()` and `filter()` build a functional transform chain applied to every row before batching.

### 6. Sync a delta to the database

```java
warp.sync()
    .tableName("users")
    .whereDelete("id = :java.id")
    .whereUpdate("id = :java.id")
    .withDelta(mapDelta)
    .arm(client);   // Future<Result<Nothing>>
```

Same update → delete → insert ordering as the blocking `SyncStrike`, composed here with `Future.flatMap` instead of sequential blocking calls.

### 7. Run strikes in parallel

```java
warp.parallelRead(instructions);   // no transaction
warp.parallelWrite(instructions);  // atomic transaction wrapping all strikes
```

`WarpScope.forkAndJoin` builds one `ArmableStrike` per instruction, fans them out over a single connection via `CompositeFuture`, and flattens successful results into one `PersistentList` — or fails the whole batch if any strike fails.

### 8. Query with forensic history on failure (the AHE Protocol)

```java
WarpStrike withHistory = warp.withHistory(Duration.ofSeconds(30));

withHistory.read(client -> warp.strike().shot("SELECT * FROM ledger").arm(client));
```

If the strike fails, `SovereignFlow.attachContextOnBreach` pulls the last 30 seconds of successful operations out of the `TemporalStreamBuffer` and appends that snapshot to the failure message — a black-box recorder for the moments leading up to a breach. Requesting a lookback window larger than the buffer's configured window throws immediately (`AXIOM BREACH: Lookback exceeds Buffer Window`).

### 9. Shutdown

```java
warp.shutdown();
```

---

## Core Concepts

### The Sovereign Gate

Every strike passes through `SovereignGate.execute`, mirroring the blocking engine: forge the SQL into an `ExecutionPlan`, verify the type contract aligns with the plan and data (`IngressIntegrity.verifyAlignment`), *then* bind and execute. Nothing reaches the Vert.x driver unverified.

### Dialect Translation

`Dialect` is a two-value enum (`POSTGRES`, `GENERIC`) that rewrites Axiom's `?` placeholders into the target driver's native marker (`$1`, `$2`, …) at forge time. The engine is otherwise driver-agnostic — swapping dialects doesn't touch calling code.

### Arming: Deferred Execution

Every strike built through `Forge` (`TypedStrike`, `AddBatchRequest`, `AddSyncTable`, …) is a pure definition — a `Function` that hasn't touched a connection yet. Nothing executes until you call `.arm(client)`. This is what lets `AxiomWarp.write` decide *when* and *under what transaction* a strike actually runs, rather than the strike deciding for itself.

### The AHE Protocol (Axiom-Hostile-Environment)

Vert.x's `io.vertx` namespace is treated as an **untrusted zone**: fast, but built on mutable collections and imperative callbacks that don't match Axiom's immutable, functional design. The AHE Protocol is the containment boundary:

- **Drain Protocol** — data is folded out of Vert.x containers into `PersistentList`/`PersistentMap`/`Result` the moment a `Future` resolves; business logic never sees a raw `RowSet` or `List`.
- **Zero-Allocation Bridge** — bridging code avoids `ArrayList` churn; `BareVertx` calls straight into Vert.x's internal `SucceededFuture` / `CompositeFutureImpl` rather than the defensive public API.
- **Perimeter Containment** — `Async` and `SovereignGate` are the only code allowed to touch a raw `SqlConnection`/`Transaction`; everything else operates on `SqlClient` through those gates.
- **Immutable Sovereignty** — anything Vert.x hands back is treated as unsafe until converted; anything passed *into* Vert.x is a frozen snapshot.

See `AHE_Protocol.md` in the repo root for the full mandate list.

### The Ghost Codec

`AxiomGhostCodec` / `AxiomGhostFactory` are registered via `META-INF/services/io.vertx.core.spi.JsonFactory` and effectively neutralize Vert.x's default JSON layer — `fromString`/`fromBuffer`/`fromValue` all return `null`; `toString`/`toBuffer` pass values through unchanged. Since `pom.xml` explicitly excludes Jackson from the dependency tree, this is a deliberate statement: Axiom does not parse or produce JSON through a third-party codec, full stop.

---

## Design Notes

- **No streaming result sets.** `ReactiveResultConverter` fully materializes a `RowSet<Row>` into a `PersistentList` — backpressure is real for the CSV *ingestion* pipeline (via `RecordParser.pause()/resume()`), but query results are not currently streamed row-by-row.
- **Parallel execution shares one connection.** Unlike the blocking `WarpScope` (one JDBC connection per forked task), the reactive `WarpScope.forkAndJoin` fans multiple strikes out over a *single* `SqlClient` — appropriate for Vert.x's event-loop model, where the connection itself is non-blocking.
- **No retry/backoff/circuit-breaker primitives exist yet.** Failures propagate as `Result.failure`, optionally enriched with AHE forensic history — there is currently no declarative retry or dead-letter mechanism in this jar.
- **No LISTEN/NOTIFY or CDC integration exists yet.** All operations are still pull-based (`read`/`write`/`parallelRead`/`parallelWrite`); push-based reactivity is not implemented in this module as shown.

---


## 📜 Legal

This project is governed by the principles of immutable software architecture. See `LICENSE.md` for the specific terms of use.
