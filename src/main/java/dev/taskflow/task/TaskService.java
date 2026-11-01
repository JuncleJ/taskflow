package dev.taskflow.task;

import io.vertx.core.Future;
import java.util.*;

public final class TaskService {
    private final TaskStore store;
    private final TaskHandlerRegistry registry;
    private final TaskExecutor executor;
    public TaskService(TaskStore store, TaskHandlerRegistry registry, TaskExecutor executor) {
        this.store = store; this.registry = registry; this.executor = executor;
    }
    public Future<Task> create(UUID owner, String type) {
        registry.get(type);
        return store.create(owner, type).onSuccess(executor::submit);
    }
    public Future<Task> find(UUID owner, UUID id) { return store.find(owner, id); }
    public Future<List<Task>> list(UUID owner) { return store.list(owner); }
}
