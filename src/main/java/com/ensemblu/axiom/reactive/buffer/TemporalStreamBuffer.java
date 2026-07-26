package com.ensemblu.axiom.reactive.buffer;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.foundation.Dop;
import com.ensemblu.axiom.reactive.syntax.BareVertx;
import io.vertx.core.Future;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 🌀 Axiom Temporal Buffer
 * THE GOAL: To provide a "Black Box Recorder" for your data.
 * It keeps a rolling history of the stream in memory so the system can look back
 * at the last 5 minutes (or any chosen window) to verify facts or detect attacks.
 */
public final class TemporalStreamBuffer<T>  {

    private final Duration windowDuration;
    private final ConcurrentLinkedQueue<StateEnvelope<T> > queue = new ConcurrentLinkedQueue<>();

    private TemporalStreamBuffer(Duration windowDuration) {
        this.windowDuration = windowDuration;
    }

    public static <T> TemporalStreamBuffer<T>  ofWindowDuration(Duration windowLimit) {
        return new TemporalStreamBuffer<T> (windowLimit);
    }

    /**
     * Instruments the stream and only snapshots if the result is a Success.
     */
    public  Future<Result<T>> instrument(Future<Result<T>> upstream) {
        return upstream.onSuccess(result -> {
            if (result.isSuccess()) {
                logEvent(result.getOrThrow());
            }
        });
    }
    
    public void logEvent(T data) {
        final var now = Instant.now();
        queue.offer(new StateEnvelope<T>(now, data));
        evictExpired(now);
    }
    
    /**
     * 🎯 The Deductive Query: Now returning Result-wrapped state.
     */
    public Future<Result<PersistentList<T>>> streamHistory(Duration ago) {
        final var threshold = Instant.now().minus(ago);

        return BareVertx.fastSuccess(Axiom.Check.success(//
                Dop.project(Axiom.Data.<T> emptyList())//
                        .pour(queue.iterator())//
                        .whileTrue(java.util.Iterator::hasNext)//
                        .extract(iterator -> {//
                            StateEnvelope<T> envelope = iterator.next();//
                            return !envelope.timestamp().isBefore(threshold)//
                                    ? envelope.state() //
                                    : (T) null;//
                        })//
                        .filter(java.util.Objects::nonNull)//
                        .deploy()
        ));
    }

    private void evictExpired(Instant now) {
        final var cutOff = now.minus(windowDuration);
        while (!queue.isEmpty() && queue.peek().timestamp().isBefore(cutOff)) {
            queue.poll();
        }
    }

    /**
     * 🧹 Flushes the buffer. Essential for isolation between test waves.
     */
    public void clear() {
        queue.clear();
    }

    public Duration getWindowDuration() {
        return windowDuration;
    }

    private record StateEnvelope<T> (Instant timestamp, T state) {}
}