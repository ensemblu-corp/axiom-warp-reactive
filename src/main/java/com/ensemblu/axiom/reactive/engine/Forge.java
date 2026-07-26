package com.ensemblu.axiom.reactive.engine;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.foundation.Nothing;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.MapDelta;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.foundation.Dop;
import com.ensemblu.axiom.reactive.api.ArmableStrike;
import com.ensemblu.axiom.reactive.engine.core.SovereignGate;
import com.ensemblu.axiom.reactive.engine.dialect.Dialect;
import com.ensemblu.axiom.reactive.syntax.BareVertx;
import com.ensemblu.axiom.spec.database.contract.AxiomProtocol;
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import com.ensemblu.axiom.spec.parser.SqlParser;
import io.vertx.core.Future;
import io.vertx.sqlclient.*;

import java.util.function.Function;


public interface Forge {

    static Function< Dialect ,ArmableStrike> shot(String sql) {
        return dialect -> withDialectForStrike(dialect).dynamicStrike(sql) .withContract(Axiom.Data.emptyMap()).withData(Axiom.Data.emptyMap());
    }
    
    static AddDynamicStrike withDialectForStrike( Dialect dialect) {
        return sqlTemplate -> //
                types ->//
                data -> //
                        client ->//
                {
                    final var plan = SqlParser.forge(sqlTemplate);
                    final var translate = dialect.translate(plan.sql());
                    final var instruction = StrikeInstruction//
                            .dynamic(translate)//
                            .withContract(types)//
                            .withData(data);
                    
                    return   SovereignGate//
                            .execute(instruction,plan,translate, new ReactiveExecutionEngine(client));
                }

       ;
    }

    static AddQuery withDialectForBulk( Dialect dialect) {
        return  sqlTemplate -> types -> dataList -> client ->  {
            if (dataList.isEmpty()) return BareVertx.fastSuccess(Axiom.Check.success(0L));
            final var plan = SqlParser.forge(sqlTemplate);
            final var finalSql = dialect.translate(plan.sql());
            final var batch = new java.util.ArrayList<Tuple>();

            dataList.forEach(row -> batch.add(bind(plan, types, row)));

            return client//
                    .preparedQuery(finalSql)//
                    .executeBatch(batch)//
                    .map(_ -> Axiom.Check.success((long) dataList.size()));
        };
    }

    private static String interpolate(String sql, PersistentMap<String, Object> data) {
        final var  pattern = java.util.regex.Pattern.compile(SqlParser.SIGNAL +"([a-zA-Z0-9_]+)");
        final var matcher = pattern.matcher(sql);
        final var sb = new StringBuilder();
        var lastEnd = 0;

        while (matcher.find()) {
            sb.append(sql, lastEnd, matcher.start());
            final var key = matcher.group(1);

            final var value = data.get(key);

            if (value instanceof String) {
                sb.append("'").append(value).append("'");
            } else {
                sb.append(value);
            }
            lastEnd = matcher.end();
        }
        sb.append(sql.substring(lastEnd));

        return sb.toString();
    }

    static AddSyncTable withDialectForSync(Dialect dialect) {
        return tableName -> deleteCond -> updateCond -> delta -> client ->
                BareVertx.fastSuccess(Axiom.Check.<Nothing>failure("delta is empty!"))
                        // 1. UPDATE FIRST
                        .flatMap(r -> delta.updated().isEmpty() //
                                ? BareVertx.fastSuccess(r)//
                                : prepareUpdate(dialect, client, tableName, interpolate(updateCond, delta.updated()), delta.updated())
                                    .map( s -> s.map(_ -> Nothing.INSTANCE))
                        )
                        // 2. DELETE SECOND
                        .flatMap(r -> delta.removed().isEmpty() //
                                ? BareVertx.fastSuccess(r) //
                                :shot(String.format("DELETE FROM %s WHERE %s", tableName, interpolate(deleteCond, delta.removed())))//
                                    .apply(dialect)//
                                    .arm(client)//
                                    .map( s -> s.map(_ -> Nothing.INSTANCE))//
                        )
                        // 3. INSERT LAST
                        .flatMap(r -> delta.added().isEmpty()
                                ? BareVertx.fastSuccess(r) //
                                : prepareInsert(dialect, client, tableName, delta.added())//
                                  .map( s -> s.map(_ -> Nothing.INSTANCE))
                        );
    };

    private static Future<Result<PersistentMap<String, Object>>> prepareInsert(Dialect dialect, SqlClient client, String table, PersistentMap<String, Object> added) {
        return Forge//
                    .withDialectForStrike(dialect)//
                    .dynamicStrike(generateInsertTemplate(table, added))//
                    .withContract(opaqueContract(added))//
                    .withData(added)//
                    .arm(client)//
                    .map(res -> res.map(list -> Axiom.Data.<String, Object>emptyMap()
                            .put("count", list.size())//
                            .put("snapshot", added) //
                    ));
    }

    private static Future<Result<PersistentMap<String, Object>>>
    prepareUpdate(Dialect dialect, SqlClient client, String table, String condition, PersistentMap<String, Object> updated) {
        return Forge//
                    .withDialectForStrike(dialect)//
                    .dynamicStrike(generateUpdateTemplate(table, condition, updated))//
                    .withContract(opaqueContract(updated))//
                    .withData(updated)//
                    .arm(client)//
                    .map(res -> res.map(list -> Axiom.Data.<String, Object>emptyMap()
                            .put("count", list.size())//
                            .put("snapshot", updated) //
                    ));
    }

    private static PersistentMap<String, AxiomProtocol> opaqueContract(PersistentMap<String, Object> data) {
        return Dop.project(data)//
                .mapValues(v -> AxiomProtocol.OPAQUE)//
                .deploy();//
    }

    static String generateInsertTemplate(String table, PersistentMap<String, Object> row) {
        final var cols = new StringBuilder();
        final var vals = new StringBuilder();
        row.forEach((k, v) -> {
            if (!cols.isEmpty()) {
                cols.append(", ");
                vals.append(", ");
            }
            cols.append(k);
            vals.append(SqlParser.SIGNAL).append(k);
        });
        return "INSERT INTO %s (%s) VALUES (%s)".formatted(table, cols, vals);
    }

    private static String generateUpdateTemplate(String table, String condition, PersistentMap<String, Object> row) {
        final var sets = new StringBuilder();
        row.forEach((k, v) -> {
            if (!sets.isEmpty()) //
                sets.append(", ");
            sets.append(k).append(" = ").append(SqlParser.SIGNAL).append(k);
        });
        return "UPDATE %s SET %s WHERE %s".formatted(table, sets, condition);
    }

    private static Tuple bind(SqlParser.ExecutionPlan plan,
                              PersistentMap<String, AxiomProtocol> contract,
                              PersistentMap<String, Object> data) {

        final var tuple = Tuple.tuple();
        for (var i = 0; i < plan.indexToKey().size(); i++) {
            final var key = plan.indexToKey().get(i);
            final var val = data.get(key);
            final var protocol = contract.get(key);

            if (protocol == null) {
                tuple.addValue(val);
            } else if (val != null) {
                protocol.getSetter().set(new ReactiveBinder(tuple), i, val);
            } else {
                new ReactiveBinder(tuple).bindNull(i, protocol);
            }
        }
        return tuple;
    }

    @FunctionalInterface interface TypedStrike { DataBinder withContract(PersistentMap<String, AxiomProtocol> types); }
    @FunctionalInterface interface DataBinder { ArmableStrike withData(PersistentMap<String, Object> data);}
    @FunctionalInterface interface AddDynamicStrike { TypedStrike dynamicStrike( String sqlTemplate); }
    @FunctionalInterface interface AddSyncTable { AddDeleteCondition tableName(String tableName); }
    @FunctionalInterface interface AddDeleteCondition { AddUpdateCondition whereDelete(String condition); }
    @FunctionalInterface interface AddUpdateCondition { AddDelta whereUpdate(String condition); }
    @FunctionalInterface interface AddDelta {ArmableSync withDelta(MapDelta<String, Object> delta); }
    @FunctionalInterface interface ArmableSync {Future<Result<Nothing>> arm(SqlClient client);}
    @FunctionalInterface interface AddQuery { AddBatchRequest bulk( String sqlTemplate); }
    @FunctionalInterface interface AddBatchRequest { DataBatchBinder withContract(PersistentMap<String, AxiomProtocol> types); }
    @FunctionalInterface interface DataBatchBinder { ArmableBatch withData(PersistentList<PersistentMap<String, Object>> dataList); }
    @FunctionalInterface interface ArmableBatch {Future<Result<Long>> arm(SqlClient client);}
}

