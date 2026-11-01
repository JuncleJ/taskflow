package dev.taskflow.task;

import io.vertx.core.json.JsonObject;

import java.util.Set;

/**
 * Проверка JSON-чисел до записи задачи и отправки в очередь.
 */
final class TaskParameters {
    private TaskParameters() {
    }

    static JsonObject integer(JsonObject input, String name, Integer fallback, int min, int max) {
        if (!Set.of(name).containsAll(input.fieldNames()))
            throw new IllegalArgumentException("Неизвестные параметры задачи");
        Object value = input.containsKey(name) ? input.getValue(name) : fallback;
        if (!(value instanceof Integer || value instanceof Long)
                || ((Number) value).longValue() < min || ((Number) value).longValue() > max)
            throw new IllegalArgumentException(name + " должен быть целым числом от " + min + " до " + max);
        return new JsonObject().put(name, ((Number) value).intValue());
    }
}
