package dev.taskflow.task;

import java.util.*;

public final class TaskHandlerRegistry {
    private final Map<String, TaskHandler> handlers = new HashMap<>();
    public TaskHandlerRegistry(TaskHandler... handlers) {
        for (TaskHandler handler : handlers) {
            if (this.handlers.putIfAbsent(handler.type(), handler) != null) throw new IllegalArgumentException("Повторный тип задачи");
        }
    }
    public TaskHandler get(String type) {
        TaskHandler handler = handlers.get(type);
        if (handler == null) throw new IllegalArgumentException("Неизвестный тип задачи");
        return handler;
    }
}
