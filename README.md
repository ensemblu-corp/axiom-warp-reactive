
# 🌊 Axiom Warp Reactive

![Version](https://img.shields.io/badge/version-2.0.0-blue)
![Java](https://img.shields.io/badge/Java-26-orange)
![Vert.x](https://img.shields.io/badge/Vert.x-5.1.6-purple)
![Depends](https://img.shields.io/badge/depends%20on-axiom--spec-informational)
![License](https://img.shields.io/badge/license-Limited%20Commercial-red)

**Non-blocking database engine for Axiom — built on Vert.x 5 and `vertx-sql-client`.**

The event-loop counterpart to `axiom-warp-jdbc`: same contract-driven, zero-reflection philosophy, same `PersistentMap` / `PersistentList` result shapes, same “strike” vocabulary — but every operation returns a `Future<Result<T>>`.

Where JDBC is the precision hammer for request/response work, **Warp Reactive** is built for pipelines: composed async chains, streaming ingestion with real backpressure, and forensic diagnostics via the **AHE Protocol**.

---

## Requirements

- **Java 26**
- **Vert.x 5.1.6** (`vertx-core`, `vertx-sql-client`) — managed by this module
- A reactive SQL driver compatible with `vertx-sql-client` (`Dialect` ships `POSTGRES` and `GENERIC`)
- [`axiom-spec`](https://github.com/ensemblu-corp/axiom-spec) `2.0.0` (and therefore `axiom`)

Jackson is **excluded**. Netty HTTP/2, HTTP/3, and QUIC codecs are excluded — they are not needed for a SQL client.

---

## Installation

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

## Quick start

```java
import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.reactive.api.AxiomWarp;
import com.ensemblu.axiom.reactive.engine.dialect.Dialect;
import com.ensemblu.axiom.reactive.buffer.TemporalStreamBuffer;
import com.ensemblu.axiom.reactive.provision.RawProvisioner;
import com.ensemblu.axiom.spec.database.materializer.AxiomProtocol;
import io.vertx.core.Future;

// 1. Build the engine
AxiomWarp warp = AxiomWarp.protocol()
        .withFactory(RawProvisioner.basedOnConfig(configSource)
                .withPoolProvider(config -> MyPool.from(config))
                .validateRules()
                .getOrThrow())
        .withCache(TemporalStreamBuffer.ofWindowDuration(Duration.ofMinutes(5)))
        // or .withoutCache()
        .withDialect(Dialect.POSTGRES);

// 2. Read (shot)
Future<Result<PersistentList<PersistentMap<String, Object>>>> rows =
        warp.strike().shot("SELECT * FROM users").arm(client);

// Or via the perimeter:
warp.read(client -> warp.strike().shot("SELECT * FROM users").arm(client));

// 3. Write (typed, transactional)
warp.write(client ->
        warp.strike()
                .dynamic("INSERT INTO users (id, name) VALUES (:java.id, :java.name)")
                .withContract(Axiom.Data.<String, AxiomProtocol>emptyMap()
                        .put("id", AxiomProtocol.LONG)
                        .put("name", AxiomProtocol.STRING))
                .withData(Axiom.Data.<String, Object>emptyMap()
                        .put("id", 1L)
                        .put("name", "Ofek"))
                .arm(client));

// 4. Stream-ingest a CSV file (real backpressure)
warp.ingest()
        .stream("users.csv")
        .usingFileHeaders()
        .onTableName("users")
        .arm(client);   // Future<Result<Long>>
```

---

## Package structure

```
com.ensemblu.axiom.reactive
├── api
│   ├── AxiomWarp.java              // Facade + builder
│   ├── AxiomWarpBehavior.java
│   ├── WarpStrike.java             // read / write / parallel*
│   └── ArmableStrike.java
├── buffer
│   └── TemporalStreamBuffer.java   // Rolling-window “black box” recorder
├── codec
│   ├── AxiomGhostCodec.java        // Neutralises Vert.x default JSON
│   └── AxiomGhostFactory.java      // SPI registration
├── engine
│   ├── Forge.java
│   ├── ReactiveBinder.java
│   ├── ReactiveExecutionEngine.java
│   ├── ReactiveResultConverter.java
│   ├── ReactiveResultRow.java
│   ├── SovereignFlow.java
│   ├── core/
│   │   ├── ExecutionEngine.java
│   │   └── SovereignGate.java
│   └── dialect/Dialect.java
├── ingest
│   ├── Pipeline.java
│   └── DefaultPipeline.java        // Byte-oriented CSV (2.0.0)
├── provision
│   └── RawProvisioner.java
├── scope
│   ├── Async.java
│   ├── ParallelGateway.java
│   └── WarpScope.java
└── syntax
    └── BareVertx.java              // Direct Vert.x internal bridges
```

---

## AHE Protocol (summary)

The **Axiom Hardware Envelope** is the containment boundary between Vert.x and Axiom:

| Rule | Meaning |
|------|---------|
| **Drain Protocol** | Data is folded out of Vert.x containers into `PersistentList` / `PersistentMap` / `Result` the moment a `Future` resolves. Business logic never sees a raw `RowSet`. |
| **Zero-Allocation Bridge** | Bridging code avoids `ArrayList` churn; `BareVertx` calls into Vert.x internals where safe. |
| **Perimeter Containment** | Only `Async` and `SovereignGate` may touch a raw `SqlConnection` / `Transaction`. |
| **Immutable Sovereignty** | Anything Vert.x hands back is unsafe until converted; anything passed *into* Vert.x is a frozen snapshot. |

Full mandate list: see `AHE_Protocol.md` in the repository.

---

## The Ghost Codec

`AxiomGhostCodec` / `AxiomGhostFactory` are registered via  
`META-INF/services/io.vertx.core.spi.JsonFactory`.

They neutralise Vert.x’s default JSON layer (`fromString` / `fromBuffer` return `null`; `toString` / `toBuffer` pass values through). Combined with the explicit exclusion of Jackson, this is a deliberate statement:

> Axiom does not parse or produce JSON through a third-party codec.

JSON work belongs to `JsonParser` / `JsonEmitter` in `axiom-spec`.

---

## 2.0.0 notes

- **Vert.x** bumped from 5.1.5 → **5.1.6**; properties consolidated to a single `vertx.version`.
- **DefaultPipeline** now feeds `CsvRowParser` with `byte[]` (`line.getBytes()`), matching the rest of the stack’s zero-copy parsing contract.
- Query results are still fully materialised (not streamed row-by-row). Backpressure is real for the **CSV ingestion** pipeline via `RecordParser.pause()` / `resume()`.

---

## Design notes

- **Parallel execution** shares one `SqlClient` (appropriate for the event-loop model), unlike JDBC’s one-connection-per-task approach.
- **No retry / backoff / circuit-breaker** primitives yet — failures surface as `Result.failure`, optionally enriched with AHE forensic history.
- **No LISTEN/NOTIFY or CDC** yet — all operations remain pull-based (`read` / `write` / `parallel*`).

---

## Related modules

| Module | Relationship |
|--------|----------------|
| `axiom-spec` | Parsers, materializers, protocols |
| `axiom` | Core data structures & `Result` |
| `axiom-warp-jdbc` | Blocking counterpart |

---

## Legal

Limited Commercial License — free for evaluation, testing, and non-commercial development.  
Commercial or production use requires a paid annual contract from Ensemblu Corp.

See `LICENSE.md`. Contact: **contact@ensemblu.com**
