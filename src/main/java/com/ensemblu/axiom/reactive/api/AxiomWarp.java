package com.ensemblu.axiom.reactive.api;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.foundation.Nothing;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.config.ConfigSource;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.reactive.buffer.TemporalStreamBuffer;
import com.ensemblu.axiom.reactive.engine.Forge;
import com.ensemblu.axiom.reactive.engine.SovereignFlow;
import com.ensemblu.axiom.reactive.engine.dialect.Dialect;
import com.ensemblu.axiom.reactive.ingest.Pipeline;
import com.ensemblu.axiom.reactive.provision.RawProvisioner;
import com.ensemblu.axiom.reactive.scope.Async;
import com.ensemblu.axiom.reactive.scope.WarpScope;
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * <h1>AXIOM-WARP: The Sovereign Reactive Gateway</h1>
 * <p>
 * This class serves as the primary entry point for all database interactions. It abstracts
 * infrastructure (Vert.x SqlClient) behind explicit operational fingers, ensuring the
 * domain layer remains decoupled from raw drivers and transactional magic.
 * </p>
 *
 * <h3>Operational Fingers</h3>
 * <ul>
 *   <li><b>Strike:</b> Defines execution strategy for SQL (Dynamic, Bulk, Shot).</li>
 *   <li><b>Ingest:</b> Manages temporal instrumentation and data pipeline entry.</li>
 *   <li><b>Sync:</b> Aligns external state/tables with Axiom data structures.</li>
 * </ul>
 *
 * <h3>Architectural Guarantees</h3>
 * <ul>
 *   <li><b>Transactional Sovereignty:</b> Writes are forced through atomic factory paths.</li>
 *   <li><b>Temporal Integrity:</b> History bridges are enforced via buffer validation.</li>
 *   <li><b>Dialect Neutrality:</b> Operations carry an explicit Dialect context.</li>
 * </ul>
 */
public final class AxiomWarp implements AxiomWarpBehavior {

    private final RawProvisioner factory;
    private final TemporalStreamBuffer buffer;
    private final Dialect dialect;

    private final Strike strikeFinger;
    private final Ingest ingestFinger;
    private final Sync syncFinger;

    private AxiomWarp(RawProvisioner factory, TemporalStreamBuffer buffer, Dialect dialect) {
        Axiom.Check//
                .that(factory)//
                .is(Objects::nonNull, //
                        "RawProvisioner instance can't be null")//
                .will()//
                .thenApprovedOrElseThrowException();
        this.factory = factory;
        this.buffer = buffer;
        this.dialect = dialect;

        this.strikeFinger = new Strike();
        this.ingestFinger = new Ingest();
        this.syncFinger = new Sync();
    }

    // --- ACCESSORS ---

    public Dialect dialect() {
        return this.dialect;
    }

    public TemporalStreamBuffer buffer() {
        return this.buffer;
    }

    public Strike strike() {
        return strikeFinger;
    }

    public Ingest ingest() {
        return ingestFinger;
    }

    public Sync sync() {
        return syncFinger;
    }

    private sealed interface OperationalFingers permits Strike, Ingest, Sync {
    }

    // --- PROTOCOL BUILDER ---

    public static WithFactory protocol() {
        return factory -> buffer -> dialect -> new AxiomWarp(factory, buffer, dialect);
    }

    public interface WithFactory {
        WithCache withFactory(RawProvisioner factory);
    }

    public interface WithCache {
        WithDialect withCache(TemporalStreamBuffer buffer);

        default WithDialect withoutCache() {
            return withCache(null);
        }
    }

    public interface WithDialect {
        AxiomWarp withDialect(Dialect dialect);
    }

    public static RawProvisioner.ProvisionStep connect(ConfigSource config) {
        return RawProvisioner.basedOnConfig(config);
    }

    // --- CONTEXT SWITCHING ---

    /**
     * Establishes a History Bridge by applying temporal context to operations.
     *
     * @param lookback The duration to traverse.
     * @return A scoped WarpStrike instance.
     */
    @Override
    public WarpStrike withHistory(Duration lookback) {
        if (this.buffer == null) return this;

        final var window = buffer.getWindowDuration();
        if (lookback.compareTo(window) > 0) {
            throw new IllegalArgumentException("AXIOM BREACH: Lookback exceeds Buffer Window.");
        }

        return new WarpStrike() {
            @Override
            public <T> Future<Result<T>> read(Function<SqlClient, Future<Result<T>>> action) {
                return SovereignFlow.attachContextOnBreach(AxiomWarp.this.read(action), buffer, lookback);
            }

            @Override
            public <T> Future<Result<T>> write(Function<SqlClient, Future<Result<T>>> action) {
                return SovereignFlow.attachContextOnBreach(AxiomWarp.this.write(action), buffer, lookback);
            }

            @Override
            public Future<Result<PersistentList<PersistentMap<String, Object>>>> parallelRead(
                    PersistentList<StrikeInstruction> instructions) {
                return SovereignFlow.attachContextOnBreach(AxiomWarp.this.parallelRead(instructions), buffer, lookback);
            }

            @Override
            public Future<Result<PersistentList<PersistentMap<String, Object>>>> parallelWrite(
                    PersistentList<StrikeInstruction> instructions) {
                return SovereignFlow.attachContextOnBreach(AxiomWarp.this.parallelWrite(instructions), buffer, lookback);
            }
        };
    }

    // --- STANDARD STRIKES ---

    @Override
    public <T> Future<Result<T>> read(Function<SqlClient, Future<Result<T>>> action) {
        return factory.run(action);
    }

    @Override
    public <T> Future<Result<T>> write(Function<SqlClient, Future<Result<T>>> logic) {
        return factory.runAtomic(logic);
    }

    @Override
    public Future<Result<PersistentList<PersistentMap<String, Object>>>> parallelRead(
            PersistentList<StrikeInstruction> instructions) {
        return factory.run(conn -> WarpScope.forkAndJoin(conn, instructions, Async::useAsGateway, dialect));
    }

    @Override
    public Future<Result<PersistentList<PersistentMap<String, Object>>>> parallelWrite(
            PersistentList<StrikeInstruction> instructions) {
        return factory.runAtomic(conn -> WarpScope.forkAndJoin(conn, instructions, Async::useAsGateway, dialect));
    }

    // --- LIFECYCLE ---

    @Override
    public Nothing shutdown() {
        factory.shutdown();
        return Nothing.INSTANCE;
    }

    // --- OPERATIONAL FINGER IMPLEMENTATIONS ---

    public final class Strike implements OperationalFingers {
        public Forge.TypedStrike dynamic(String sql) {
            return Forge.withDialectForStrike(dialect).dynamicStrike(sql);
        }

        public Forge.AddBatchRequest bulk(String sql) {
            return Forge.withDialectForBulk(dialect).bulk(sql);
        }

        public ArmableStrike shot(String sql) {
            return Forge.shot(sql).apply(dialect);
        }
    }

    public final class Sync implements OperationalFingers {
        public Forge.AddDeleteCondition tableName(String tableName) {
            return Forge.withDialectForSync(dialect).tableName(tableName);
        }
    }

    public final class Ingest implements OperationalFingers {
        public Pipeline.AddPipeline stream(String path) {
            return Pipeline.withDialect(dialect).fromFilePath(path);
        }

        public <T> Future<Result<T>> record(Future<Result<T>> stream) {
            return buffer != null ? buffer.instrument(stream) : stream;
        }
    }
}