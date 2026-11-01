package dev.taskflow.task;

import dev.taskflow.http.ApiException;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

/**
 * Очередь, активные выполнения и выбор победителя гонки используются из одного Vert.x context.
 */
public final class TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);
    private final TaskStore store;
    private final TaskHandlerRegistry registry;
    private final Consumer<Task> notifications;
    private final int parallelism;
    private final int queueCapacity;
    private int reservations;
    private final Queue<Task> queue = new ArrayDeque<>();
    private final Map<UUID, Execution> running = new HashMap<>();
    private final Map<UUID, Future<Task>> pendingCancellations = new HashMap<>();
    private boolean stopped;

    private static final class Execution {
        final Task task;
        final CancellationToken cancellation = new CancellationToken();
        final Promise<Task> result = Promise.promise();
        Future<Void> progressWrites = Future.succeededFuture();
        int progress;
        boolean finishing;

        Execution(Task task) {
            this.task = task;
            this.progress = task.progress();
        }
    }

    public TaskExecutor(TaskStore store, TaskHandlerRegistry registry, Consumer<Task> notifications, int parallelism) {
        this(store, registry, notifications, parallelism, 100);
    }

    public TaskExecutor(TaskStore store, TaskHandlerRegistry registry, Consumer<Task> notifications, int parallelism, int queueCapacity) {
        if (parallelism < 1) throw new IllegalArgumentException("Параллелизм должен быть положительным");
        if (queueCapacity < 1 || queueCapacity > 10_000)
            throw new IllegalArgumentException("Размер очереди должен быть от 1 до 10000");
        this.store = store;
        this.registry = registry;
        this.notifications = notifications;
        this.parallelism = parallelism;
        this.queueCapacity = queueCapacity;
    }

    public void submit(Task task) {
        reserve();
        submitReserved(task);
    }

    void reserve() {
        if (stopped || running.size() + queue.size() + reservations >= (long) parallelism + queueCapacity)
            throw new ApiException(429, "Очередь задач заполнена или исполнитель остановлен");
        reservations++;
    }

    void releaseReservation() {
        reservations--;
    }

    void submitReserved(Task task) {
        releaseReservation();
        queue.add(task);
        drain();
    }

    public void stop() {
        stopped = true;
        queue.clear();
        running.values().forEach(execution -> execution.cancellation.request());
    }

    public Future<Task> cancel(Task task) {
        Future<Task> pending = pendingCancellations.get(task.id());
        if (pending != null) return pending;
        Execution execution = running.get(task.id());
        if (execution != null) {
            // После завершения Future обработчика исход уже выбран, даже если запись результата ещё идёт.
            if (!execution.finishing && execution.cancellation.request())
                LOG.info("Запрошена отмена taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
            return execution.result.future().compose(this::cancelledResult);
        }
        Task queued = queue.stream().filter(item -> item.id().equals(task.id())).findFirst().orElse(null);
        if (queued != null) {
            queue.remove(queued);
            Promise<Task> result = Promise.promise();
            pendingCancellations.put(task.id(), result.future());
            LOG.info("Запрошена отмена ожидающей задачи taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
            store.update(task.id(), Task.Status.CANCELLED, queued.progress(), null).onComplete(outcome -> {
                pendingCancellations.remove(task.id());
                if (outcome.succeeded()) {
                    logCancelled(outcome.result());
                    notifyTask(outcome.result());
                }
                result.handle(outcome);
            });
            return result.future();
        }
        return store.find(task.ownerId(), task.id()).compose(current -> current == null
                ? Future.failedFuture(new ApiException(404, "Задача не найдена")) : cancelledResult(current));
    }

    private Future<Task> cancelledResult(Task task) {
        return task.status() == Task.Status.CANCELLED ? Future.succeededFuture(task)
                : Future.failedFuture(new ApiException(409, "Задача уже завершена или не выполняется этим экземпляром"));
    }

    private void drain() {
        while (!stopped && running.size() < parallelism && !queue.isEmpty()) {
            Execution execution = new Execution(queue.remove());
            Task task = execution.task;
            running.put(task.id(), execution);
            store.update(task.id(), Task.Status.RUNNING, 0, null).compose(started -> {
                LOG.info("Задача запущена taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
                notifyTask(started);
                execution.cancellation.check();
                return registry.get(task.type()).execute(task, value -> report(execution, value), execution.cancellation);
            }).transform(work -> {
                execution.finishing = true;
                // Даже обработчик, не дождавшийся callback прогресса, не оставит запись после терминального статуса.
                return execution.progressWrites.transform(writes -> finish(execution,
                        work.failed() ? work.cause() : writes.failed() ? writes.cause() : null));
            }).onComplete(outcome -> {
                if (outcome.succeeded()) notifyTask(outcome.result());
                else LOG.error("Не удалось сохранить результат задачи taskId={} userId={} taskType={} errorType={}",
                        task.id(), task.ownerId(), task.type(), outcome.cause().getClass().getName());
                running.remove(task.id());
                drain();
                execution.result.handle(outcome);
            });
        }
    }

    private Future<Void> report(Execution execution, int value) {
        if (execution.finishing) return Future.failedFuture("Обработчик уже завершён");
        if (execution.cancellation.isRequested())
            return Future.failedFuture(new java.util.concurrent.CancellationException());
        execution.progressWrites = execution.progressWrites.compose(v -> {
            if (value < execution.progress || value > 100) return Future.failedFuture("Некорректный прогресс");
            return store.update(execution.task.id(), Task.Status.RUNNING, value, null).map(updated -> {
                execution.progress = value;
                notifyTask(updated);
                return (Void) null;
            });
        });
        return execution.progressWrites;
    }

    private Future<Task> finish(Execution execution, Throwable error) {
        Task task = execution.task;
        Task.Status status = execution.cancellation.isRequested() ? Task.Status.CANCELLED
                : error == null ? Task.Status.COMPLETED : Task.Status.FAILED;
        if (status == Task.Status.FAILED)
            LOG.error("Ошибка выполнения задачи taskId={} userId={} taskType={} errorType={} stack={}",
                    task.id(), task.ownerId(), task.type(), error.getClass().getName(), Arrays.toString(error.getStackTrace()));
        return store.update(task.id(), status, status == Task.Status.COMPLETED ? 100 : execution.progress,
                status == Task.Status.FAILED ? "Ошибка выполнения задачи" : null).onSuccess(finished -> {
            if (status == Task.Status.CANCELLED) logCancelled(finished);
            else if (status == Task.Status.COMPLETED)
                LOG.info("Задача завершена taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
        });
    }

    private void logCancelled(Task task) {
        LOG.info("Задача отменена taskId={} userId={} taskType={}", task.id(), task.ownerId(), task.type());
    }

    private void notifyTask(Task task) {
        try {
            notifications.accept(task);
        } catch (RuntimeException error) {
            LOG.error("Ошибка уведомления taskId={} userId={} taskType={} errorType={}", task.id(), task.ownerId(), task.type(), error.getClass().getName());
        }
    }
}
