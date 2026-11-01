package dev.taskflow.task;

import io.vertx.core.*;

import java.util.*;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Очередь и счётчик используются только из одного Vert.x context.
 */
public final class TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);
    private final TaskStore store;
    private final TaskHandlerRegistry registry;
    private final Consumer<Task> notifications;
    private final int parallelism;
    private final Queue<Task> queue = new ArrayDeque<>();
    private int active;
    private boolean stopped;

    public TaskExecutor(TaskStore store, TaskHandlerRegistry registry, Consumer<Task> notifications, int parallelism) {
        if (parallelism < 1) throw new IllegalArgumentException("Параллелизм должен быть положительным");
        this.store = store;
        this.registry = registry;
        this.notifications = notifications;
        this.parallelism = parallelism;
    }

    public void submit(Task task) {
        queue.add(task);
        drain();
    }

    public void stop() {
        stopped = true;
        queue.clear();
    }

    private void drain() {
        while (!stopped && active < parallelism && !queue.isEmpty()) {
            Task task = queue.remove();
            active++;
            int[] latest = {task.progress()};
            store.update(task.id(), Task.Status.RUNNING, 0, null)
                    .compose(started -> {
                        LOG.info("Задача запущена taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
                        notifications.accept(started);
                        return registry.get(task.type()).execute(value -> {
                            if (value < latest[0] || value > 100) return Future.failedFuture("Некорректный прогресс");
                            return store.update(task.id(), Task.Status.RUNNING, value, null).map(updated -> {
                                latest[0] = value;
                                notifications.accept(updated);
                                return (Void) null;
                            });
                        });
                    })
                    .compose(v -> store.update(task.id(), Task.Status.COMPLETED, 100, null).onSuccess(completed ->
                            LOG.info("Задача завершена taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type())))
                    .recover(error -> {
                        LOG.error("Ошибка выполнения задачи taskId={} userId={} taskType={} errorType={} stack={}",
                                task.id(), task.ownerId(), task.type(), error.getClass().getName(), Arrays.toString(error.getStackTrace()));
                        return store.update(task.id(), Task.Status.FAILED, latest[0], "Ошибка выполнения задачи");
                    })
                    .onSuccess(notifications::accept)
                    .onFailure(error -> LOG.error("Не удалось сохранить результат задачи taskId={} userId={} taskType={} errorType={} stack={}",
                            task.id(), task.ownerId(), task.type(), error.getClass().getName(), Arrays.toString(error.getStackTrace())))
                    .onComplete(result -> {
                        active--;
                        drain();
                    });
        }
    }
}
