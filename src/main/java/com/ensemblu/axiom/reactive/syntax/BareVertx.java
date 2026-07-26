package com.ensemblu.axiom.reactive.syntax;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.impl.future.SucceededFuture;

/**
 * 🛡️ BARE VERTX - AXIOM DIRECT-ACCESS GATEWAY
 * * This class strips away the "framework suits" (defensive API adapters) forced
 * by the public Vert.x interface to satisfy the Java community.
 * * We bypass list-to-array conversions, defensive null-checking, and state-machine
 * overhead. We talk directly to the engine implementations.
 * * No validation, no fluff, no redundant layers.
 */
public final class BareVertx {

    // --- PRIVATE CONSTRUCTOR ---
    // Ensures this class remains a static utility gateway.
    private BareVertx() {
        throw new UnsupportedOperationException("This is a direct access gateway, not an object factory.");
    }

    public static <T> Future<T> fastSuccess(T result) {
        return new SucceededFuture<>(result);
    }

    /**
     * Optimized coordination.
     * We bypass the public Future.all(List) which forces an ArrayList allocation
     * and array conversion. We pass the varargs directly to the implementation.
     */
    public static Future<CompositeFuture> fastAll(Future<?>... futures) {
        return CompositeFutureImpl.all(futures);
    }

    /**
     * Optimized coordination.
     * We bypass the public Future.join(List) and go straight to the internal impl.
     */
    public static Future<CompositeFuture> fastJoin(Future<?>... futures) {
        return CompositeFutureImpl.join(futures);
    }

    /**
     * FINAL REFINEMENT: THE "UNSAFE" CAST
     * If you are 100% certain you are operating within a known Axiom-controlled
     * context, use this to treat a raw object as a future without triggering
     * Vert.x's internal context-lookup logic.
     */
    @SuppressWarnings("unchecked")
    public static <T> Future<T> narrow(Object obj) {
        return (Future<T>) obj;
    }

    /**
     * Executes the task immediately if on the target context, otherwise
     * queues it. Bypasses the 'runOnContext' overhead when already on the
     * correct thread.
     */
    public static void run(io.vertx.core.Context context, Runnable task) {
        if (io.vertx.core.Vertx.currentContext() == context) {
            task.run();
        } else {
            context.runOnContext(v -> task.run());
        }
    }

}