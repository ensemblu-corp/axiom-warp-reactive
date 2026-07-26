package com.ensemblu.axiom.reactive.ingest;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.foundation.Dop;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.reactive.engine.Forge;
import com.ensemblu.axiom.reactive.engine.dialect.Dialect;
import com.ensemblu.axiom.spec.database.contract.AxiomProtocol;
import com.ensemblu.axiom.spec.parser.CsvRowParser;
import com.ensemblu.axiom.spec.parser.SqlParser;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.sqlclient.SqlClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public final class DefaultPipeline implements Pipeline {
    private final String path;
    private final Dialect dialect;
    private final PersistentList<String> headers;
    private final PersistentList<Function<PersistentMap<String, Object>, Result<PersistentMap<String, Object>>>> ops;

    public DefaultPipeline(String path, PersistentList<String> headers, Dialect dialect) {
        this(path, headers, dialect, Axiom.Data.emptyList());
    }

    private DefaultPipeline(String path, PersistentList<String> headers, Dialect dialect,
                            PersistentList<Function<PersistentMap<String, Object>, Result<PersistentMap<String, Object>>>>  ops) {
        this.path = path;
        this.headers = headers;
        this.dialect = dialect;
        this.ops = ops;
    }

    @Override
    public Pipeline map(UnaryOperator<PersistentMap<String, Object>> transform) {
        return new DefaultPipeline(path, headers, dialect, //
                ops.append(map ->//
                Axiom.Check.success(map).map(transform)//
                ));
    }

    @Override
    public Pipeline filter(Predicate<PersistentMap<String, Object>> predicate) {
        return new DefaultPipeline(path, headers, dialect, ops.append(map -> predicate.test(map) ? Axiom.Check.success(map) : Axiom.Check.empty()));
    }

    @Override
    public ArmableFile onTableName(String tableName) {
        return client -> {//
            Promise<Result<Long>> promise = Promise.promise();//
            Vertx.currentContext()//
                    .owner()//
                    .fileSystem()//
                    .open(path, new OpenOptions())//
                    .onFailure(promise::fail)//
                    .onSuccess(file -> {//
                        final var parser = RecordParser.newDelimited("\n", file);
                        final var batch = new ArrayList<PersistentMap<String, Object>> ();
                        final var counter = new AtomicLong(0);
                        final var headerLoaded = new java.util.concurrent.atomic.AtomicBoolean(!headers.isEmpty());
                        final var currentHeaders = new java.util.concurrent.atomic.AtomicReference<>(headers);

                        parser.handler(line -> {//
                            parser.pause();//
                            final var strLine = line.toString().trim();//
                            if (strLine.isEmpty()) { parser.resume(); return; }

                            if (!headerLoaded.get()) {//
                                currentHeaders.set(CsvRowParser.scanLine(strLine));//
                                headerLoaded.set(true);//
                                parser.resume();//
                                return; //
                            }

                           final var row = Axiom.Check.success(
                                    CsvRowParser.takeLine(strLine).basedOnHeaders(currentHeaders.get())
                            );

                            final var resultRow = ops.fold(row, (acc, op) -> acc.flatMap(op));

                            resultRow.peekSuccess(processed -> {
                                batch.add(processed);
                                if (batch.size() >= Pipeline.BATCH_SIZE) {
                                    flushBatch(batch, client, tableName, counter, parser, promise, file, currentHeaders.get());
                                } else {
                                    parser.resume();
                                }
                            }).peekEmpty(parser::resume);
                        });

                        parser.endHandler(_ -> {
                            if (!batch.isEmpty()) {
                                flushFinalBatch(batch, client, tableName, counter, promise, file, currentHeaders.get());
                            } else {
                                file.close();
                                promise.complete(Axiom.Check.success(counter.get()));
                            }
                        });
                    });
            return promise.future();
        };
    }

    private void flushFinalBatch(//
                                 List<PersistentMap<String,Object>> batch,//
                                 SqlClient client,//
                                 String tableName,//
                                 AtomicLong counter,//
                                 Promise<Result<Long>> promise,//
                                 AsyncFile file,//
                                 PersistentList<String> activeHeaders) { //
        final var snapshot = new ArrayList<>(batch);
        batch.clear();

        Forge//
            .withDialectForBulk(dialect)//
            .bulk(forgeInsertSql(tableName, activeHeaders))//
            .withContract(forgeContract(activeHeaders))//
            .withData(Axiom.Data.fromJava(snapshot))//
            .arm(client)//
            .onSuccess(res -> {//
                counter.addAndGet(snapshot.size());//
                file.close();//
                promise.complete(Axiom.Check.success(counter.get()));//
            })//
            .onFailure(err -> {//
                file.close();//
                promise.fail(err);//
            });//
    }

    private void flushBatch(//
                            List<PersistentMap<String,Object>> batch,//
                            SqlClient client,//
                            String tableName,//
                            AtomicLong counter,//
                            RecordParser parser,//
                            Promise<Result<Long>> promise,//
                            AsyncFile file,//
                            PersistentList<String> activeHeaders) { //
        final var snapshot = new ArrayList<>(batch);
        batch.clear();

        Forge//
            .withDialectForBulk(dialect)//
            .bulk(forgeInsertSql(tableName, activeHeaders))//
            .withContract(forgeContract(activeHeaders))//
            .withData(Axiom.Data.fromJava(snapshot))//
            .arm(client)//
            .onSuccess(res -> {//
                counter.addAndGet(snapshot.size());//
                parser.resume();//
            })//
            .onFailure(err -> {//
                file.close();//
                promise.fail(err);//
            });//
    }

    private String forgeInsertSql(String tableName, PersistentList<String> headers) {
        final var columns = new StringBuilder();
        final var placeholders = new StringBuilder();

        headers.forEach(h -> {
            if (!columns.isEmpty()) { columns.append(", "); placeholders.append(", "); }
            columns.append(h);
            placeholders.append(SqlParser.SIGNAL).append(h);
        });

        return "INSERT INTO %s (%s) VALUES (%s)".formatted(tableName,columns,placeholders);
    }

    private PersistentMap<String, AxiomProtocol> forgeContract(PersistentList<String> headers) {
        return Dop.project(headers)//
                .indexBy(s -> s)//
                .mapValues(v -> AxiomProtocol.OPAQUE)//
                .deploy();//
    }
}