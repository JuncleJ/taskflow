package dev.taskflow.task;

import io.vertx.core.*;

import java.util.function.IntFunction;

/**
 * Демонстрационная работа на таймерах: ожидание не занимает поток.
 */
public final class TimedTaskHandler implements TaskHandler {
    private final Vertx vertx;
    private final String type;
    private final int steps;
    private final long delay;

    public TimedTaskHandler(Vertx vertx, String type, int steps, long delay) {
        this.vertx = vertx;
        this.type = type;
        this.steps = steps;
        this.delay = delay;
    }

    public String type() {
        return type;
    }

    public Future<Void> execute(IntFunction<Future<Void>> progress) {
        return step(1, progress);
    }

    private Future<Void> step(int n, IntFunction<Future<Void>> progress) {
        if (n > steps) return Future.succeededFuture();
        Promise<Void> pause = Promise.promise();
        vertx.setTimer(delay, timer -> pause.complete());
        return pause.future().compose(v -> progress.apply(n * 100 / steps)).compose(v -> step(n + 1, progress));
    }
}
