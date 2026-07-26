package com.ensemblu.axiom.reactive.engine;

import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.foundation.Dop;
import com.ensemblu.axiom.reactive.buffer.TemporalStreamBuffer;
import com.ensemblu.axiom.reactive.syntax.BareVertx;
import io.vertx.core.Future;
import java.time.Duration;


public interface SovereignFlow {

    static <T> Future<Result<T>> attachContextOnBreach(//
            Future<Result<T>> strike,//
            TemporalStreamBuffer<T> buffer,//
            Duration lookback//
    ) {
        return strike.compose(result -> {
            if (result.isSuccess()) {
                if (buffer != null) buffer.logEvent(result.getOrThrow());
                return BareVertx.fastSuccess(result);
            }

            return buffer.streamHistory(lookback).map(historyResult -> {
                final var snapshot = historyResult.isSuccess()
                        ? Dop.toString(historyResult.getOrThrow())
                        : "Buffer Unavailable: " + historyResult.failureValue().getMessage();

                final var context = String.format("💥 AHE BREACH | PRE-FAILURE SNAPSHOT: %s", snapshot);

                return result.mapFailure(e -> """
                                            %s
                                            ---------------------------
                                            RAW BREACH EVIDENCE: %s
                                            """.formatted(context, e.getMessage()));
            });
        });
    }
}