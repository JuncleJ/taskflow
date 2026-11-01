package dev.taskflow.task;

import io.vertx.core.Future;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskService {
    private static final Logger LOG = LoggerFactory.getLogger(TaskService.class);
    private final TaskStore store;
    private final TaskHandlerRegistry registry;
    private final TaskExecutor executor;

    public TaskService(TaskStore store, TaskHandlerRegistry registry, TaskExecutor executor) {
        this.store = store;
        this.registry = registry;
        this.executor = executor;
    }

    public Future<Task> create(UUID owner, String type) {
        registry.get(type);
        return store.create(owner, type).onSuccess(task -> {
            LOG.info("Задача создана taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
            executor.submit(task);
        });
    }

    public Future<Task> find(UUID owner, UUID id) {
        return store.find(owner, id);
    }

    public Future<List<Task>> list(UUID owner) {
        return store.list(owner);
    }
}
