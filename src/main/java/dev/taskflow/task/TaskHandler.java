package dev.taskflow.task;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.function.IntFunction;

public interface TaskHandler {
    String type();

    default JsonObject parameters(JsonObject input) {
        if (!input.isEmpty()) throw new IllegalArgumentException("Этот тип задачи не поддерживает параметры");
        return new JsonObject();
    }

    /**
     * Future завершается только после остановки работы; блокировать Event Loop запрещено.
     * task содержит сохранённые параметры. Callback progress принадлежит вызывающему Vert.x context:
     * обработчик с worker-потоками должен явно передать вызов обратно и учитывать его Future.
     */
    Future<Void> execute(Task task, IntFunction<Future<Void>> progress, CancellationToken cancellation);
}
