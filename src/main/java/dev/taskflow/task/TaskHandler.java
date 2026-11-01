package dev.taskflow.task;

import io.vertx.core.Future;

import java.util.function.IntFunction;

public interface TaskHandler {
    String type();

    /**
     * Future завершается только после остановки работы; блокировать Event Loop запрещено.
     */
    Future<Void> execute(IntFunction<Future<Void>> progress, CancellationToken cancellation);
}
