package dev.taskflow.http;

import dev.taskflow.task.Task;
import io.vertx.core.*;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * После проверки владельца отправляет сохранённые снимки; медленные клиенты отключаются.
 */
public final class SseNotifications {
    private static final Logger LOG = LoggerFactory.getLogger(SseNotifications.class);
    private final Vertx vertx;
    private final long heartbeatInterval;
    private final Map<UUID, Set<HttpServerResponse>> clients = new HashMap<>();
    private final Map<HttpServerResponse, Task> lastSent = new HashMap<>();
    private final Map<HttpServerResponse, Runnable> cleanups = new HashMap<>();

    public SseNotifications(Vertx vertx) {
        this(vertx, 15_000);
    }

    SseNotifications(Vertx vertx, long heartbeatInterval) {
        this.vertx = vertx;
        this.heartbeatInterval = heartbeatInterval;
    }

    public void subscribe(Task task, RoutingContext ctx) {
        HttpServerResponse response = ctx.response();
        response.setChunked(true).putHeader("Content-Type", "text/event-stream; charset=utf-8")
                .putHeader("Cache-Control", "no-cache").putHeader("X-Accel-Buffering", "no");
        clients.computeIfAbsent(task.id(), id -> new HashSet<>()).add(response);
        long heartbeat = vertx.setPeriodic(heartbeatInterval, id -> {
            if (response.closed() || response.ended()) {
                vertx.cancelTimer(id);
                return;
            }
            if (response.writeQueueFull()) response.end();
            else response.write(": heartbeat\n\n").onFailure(e -> response.end());
        });
        Runnable cleanup = () -> {
            vertx.cancelTimer(heartbeat);
            lastSent.remove(response);
            cleanups.remove(response);
            Set<HttpServerResponse> set = clients.get(task.id());
            if (set != null) {
                set.remove(response);
                if (set.isEmpty()) clients.remove(task.id());
            }
        };
        cleanups.put(response, cleanup);
        ctx.addEndHandler(result -> cleanup.run());
        send(response, task);
    }

    public void publish(Task task) {
        for (HttpServerResponse response : new HashSet<>(clients.getOrDefault(task.id(), Set.of())))
            send(response, task);
    }

    public void deleted(Task task) {
        for (HttpServerResponse response : new HashSet<>(clients.getOrDefault(task.id(), Set.of()))) {
            Runnable cleanup = cleanups.get(response);
            if (cleanup != null) cleanup.run();
            try {
                if (response.closed() || response.ended()) continue;
                if (response.writeQueueFull()) response.reset();
                else
                    response.write("event: deleted\ndata: " + new io.vertx.core.json.JsonObject().put("id", task.id().toString()).encode() + "\n\n")
                            .onComplete(result -> response.end());
            } catch (RuntimeException error) {
                // Удаление строки уже завершено; сбой одного клиента не мешает очистить остальных.
                LOG.warn("Ошибка SSE при удалении taskId={} userId={} errorType={}", task.id(), task.ownerId(), error.getClass().getName());
                try {
                    response.reset();
                } catch (RuntimeException resetError) {
                    LOG.warn("Не удалось сбросить SSE taskId={} userId={} errorType={}", task.id(), task.ownerId(), resetError.getClass().getName());
                }
            }
        }
        clients.remove(task.id());
    }

    private void send(HttpServerResponse response, Task task) {
        if (response.closed() || response.ended()) return;
        Task last = lastSent.get(response);
        if (last != null && (last.terminal() || task.progress() < last.progress()
                || last.status() == Task.Status.RUNNING && task.status() == Task.Status.PENDING)) return;
        lastSent.put(response, task);
        if (response.writeQueueFull()) {
            response.end();
            return;
        }
        String event = task.status() == Task.Status.COMPLETED ? "completed" : task.status() == Task.Status.FAILED ? "failed"
                : task.status() == Task.Status.CANCELLED ? "cancelled" : "progress";
        response.write("event: " + event + "\ndata: " + task.json().encode() + "\n\n").onComplete(result -> {
            if (result.failed() || task.terminal()) response.end();
        });
    }
}
