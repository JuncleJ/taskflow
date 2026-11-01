package dev.taskflow.task;

import io.vertx.core.Future;

import java.util.*;
import java.util.function.Consumer;

import dev.taskflow.http.ApiException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskService {
    private static final Logger LOG = LoggerFactory.getLogger(TaskService.class);
    private final TaskStore store;
    private final TaskHandlerRegistry registry;
    private final TaskExecutor executor;
    private final Consumer<Task> deletedNotifications;

    public TaskService(TaskStore store, TaskHandlerRegistry registry, TaskExecutor executor, Consumer<Task> deletedNotifications) {
        this.store = store;
        this.registry = registry;
        this.executor = executor;
        this.deletedNotifications = deletedNotifications;
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

    public Future<Task> cancel(UUID owner, UUID id) {
        return requireOwned(owner, id).compose(executor::cancel)
                .onSuccess(task -> LOG.info("Отмена подтверждена taskId={} userId={} taskType={}", id, owner, task.type()))
                .onFailure(error -> logRejected("cancel", owner, id, error));
    }

    public Future<Void> delete(UUID owner, UUID id) {
        return requireOwned(owner, id).compose(task -> {
            if (!task.terminal())
                return Future.<Void>failedFuture(new ApiException(409, "Нельзя удалить активную задачу"));
            return store.delete(owner, id).compose(deleted -> {
                if (!deleted) return Future.<Void>failedFuture(new ApiException(404, "Задача не найдена"));
                LOG.info("Задача удалена taskId={} userId={} taskType={}", id, owner, task.type());
                try {
                    deletedNotifications.accept(task);
                } catch (RuntimeException error) {
                    LOG.error("Ошибка уведомления об удалении taskId={} userId={} errorType={}", id, owner, error.getClass().getName());
                }
                return Future.<Void>succeededFuture();
            });
        }).onFailure(error -> logRejected("delete", owner, id, error));
    }

    private Future<Task> requireOwned(UUID owner, UUID id) {
        return store.find(owner, id).compose(task -> task == null
                ? Future.failedFuture(new ApiException(404, "Задача не найдена")) : Future.succeededFuture(task));
    }

    private void logRejected(String operation, UUID owner, UUID id, Throwable error) {
        if (error instanceof ApiException api)
            LOG.warn("Операция задачи отклонена operation={} taskId={} userId={} status={}", operation, id, owner, api.status());
        else
            LOG.error("Сбой операции задачи operation={} taskId={} userId={} errorType={}", operation, id, owner, error.getClass().getName());
    }
}
