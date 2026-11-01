package dev.taskflow.task;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * Таймеры не занимают поток; прогресс определяется монотонным временем, а не числом запросов к БД.
 */
public final class DemoTaskHandler implements TaskHandler {
    private final Vertx vertx;

    public DemoTaskHandler(Vertx vertx) {
        this.vertx = vertx;
    }

    public String type() {
        return "DEMO";
    }

    public JsonObject parameters(JsonObject input) {
        return TaskParameters.integer(input, "durationSeconds", 5, 1, 300);
    }

    public Future<Void> execute(Task task, IntFunction<Future<Void>> progress, CancellationToken cancellation) {
        int seconds = parameters(task.parameters()).getInteger("durationSeconds");
        return tick(System.nanoTime(), TimeUnit.SECONDS.toNanos(seconds), 0, progress, cancellation);
    }

    private Future<Void> tick(long start, long duration, int last, IntFunction<Future<Void>> progress, CancellationToken cancellation) {
        cancellation.check();
        long elapsed = System.nanoTime() - start;
        int percent = (int) Math.min(100, elapsed * 100 / duration);
        Future<Void> saved = percent > last ? progress.apply(percent) : Future.succeededFuture();
        if (percent == 100) return saved;
        return saved.compose(v -> {
            cancellation.check();
            Promise<Void> pause = Promise.promise();
            long remaining = Math.max(1, TimeUnit.NANOSECONDS.toMillis(duration - (System.nanoTime() - start)));
            vertx.setTimer(Math.min(250, remaining), id -> pause.complete());
            return pause.future();
        }).compose(v -> tick(start, duration, percent, progress, cancellation));
    }
}
