package com.ensemblu.axiom.reactive.provision;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.config.ConfigSource;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.reactive.scope.Async;
import com.ensemblu.axiom.reactive.syntax.BareVertx;
import com.ensemblu.axiom.spec.database.materializer.DefaultDataContract;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;

import java.util.Objects;
import java.util.function.Function;


public final class RawProvisioner {

    private final Pool pool;

    private RawProvisioner(Pool pool) {
        this.pool = pool;
    }

    public static ProvisionStep basedOnConfig(ConfigSource config) {//
        return poolProvider ->//
                validateEngineRules -> {//
                    return Axiom.Check//
                            .that(validateEngineRules)//
                            .isNull()//
                            .will()//
                            .mapTo(_ -> DefaultDataContract.validate(config))//
                            .orGet(() -> Axiom.Check.attempt(() -> validateEngineRules.apply(config))//
                                    .validate(Objects::nonNull,//
                                            "validatedRules provide null value"))//
                            .mapTry(poolProvider)//
                            .nameThrowingPredicate(Throwable::getMessage)//
                            .prependFailureMessage("Reactive Provisioning Initialization Failed:")//
                            .map(RawProvisioner::new);
                };
    }

    public <T> Future<Result<T>> run(Function<SqlClient, Future<Result<T>>> action) {
        return pool//
                .withConnection(conn -> Async.use(conn, action))//
                .recover(this::handleBreach);
    }

    public <T> Future<Result<T>> runAtomic(Function<SqlClient, Future<Result<T>>> businessLogic) {
        return pool//
                .withConnection(conn -> Async.transaction(conn, businessLogic))//
                .recover(this::handleBreach);
    }

    private <T> Future<Result<T>> handleBreach(Throwable err) {
        return BareVertx.fastSuccess(//
                Axiom.Check.failure("Execution Scope Breach: " + err.getMessage())//
        );
    }

    public void shutdown() {
        if (pool != null) pool.close();
    }


    public interface ProvisionSource {
        Future<Result<PersistentMap<String, Object>>> onSource(String catalogSource);
    }

    public interface ProvisionStep {
        RulesStep withPoolProvider(Function<PersistentMap<String, Object>, Pool> poolProvider);
    }

    public interface RulesStep {
        Result<RawProvisioner> validateRules(Function<ConfigSource, PersistentMap<String, Object>> rules);

        default Result<RawProvisioner> validateRules() {
            return validateRules(null);
        }
    }
}