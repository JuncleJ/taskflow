package dev.taskflow.http;

import dev.taskflow.task.Task;
import io.vertx.core.*;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.*;

/**
 * После проверки владельца отправляет сохранённые снимки; медленные клиенты отключаются.
 */
public final class SseNotifications {
    private final Vertx vertx;
    private final long heartbeatInterval;
    private final Map<UUID, Set<HttpServerResponse>> clients = new HashMap<>();
    private final Map<HttpServerResponse, Task> lastSent = new HashMap<>();

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
            Set<HttpServerResponse> set = clients.get(task.id());
            if (set != null) {
                set.remove(response);
                if (set.isEmpty()) clients.remove(task.id());
            }
        };
        ctx.addEndHandler(result -> cleanup.run());
        send(response, task);
    }

    public void publish(Task task) {
        for (HttpServerResponse response : new HashSet<>(clients.getOrDefault(task.id(), Set.of())))
            send(response, task);
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
        String event = task.status() == Task.Status.COMPLETED ? "completed" : task.status() == Task.Status.FAILED ? "failed" : "progress";
        response.write("event: " + event + "\ndata: " + task.json().encode() + "\n\n").onComplete(result -> {
            if (result.failed() || task.terminal()) response.end();
        });
    }
}
