package dev.taskflow.http;

import dev.taskflow.task.Task;
import io.vertx.core.*;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.http.*;
import java.time.OffsetDateTime;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class SseNotificationsTest {
    @Test void terminalResponseCancelsHeartbeatWithoutUnhandledExceptions() throws Exception {
        Vertx vertx = Vertx.vertx();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        vertx.exceptionHandler(errors::add);
        try {
            Promise<Integer> port = Promise.promise();
            vertx.runOnContext(v -> {
                Vertx.currentContext().exceptionHandler(errors::add);
                SseNotifications sse = new SseNotifications(vertx, 10);
                Task completed = new Task(UUID.randomUUID(), UUID.randomUUID(), "DEMO", Task.Status.COMPLETED,
                    100, OffsetDateTime.now(), OffsetDateTime.now(), OffsetDateTime.now(), null);
                vertx.createHttpServer().requestHandler(request -> sse.subscribe(completed, request.response()))
                    .listen(0).onSuccess(server -> port.complete(server.actualPort())).onFailure(port::fail);
            });
            int actualPort = port.future().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + actualPort))
                .timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString());
            assertTrue(response.body().contains("event: completed"));
            Promise<Void> elapsed = Promise.promise();
            vertx.setTimer(100, id -> elapsed.complete());
            elapsed.future().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertTrue(errors.isEmpty(), errors.toString());
        } finally { vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS); }
    }
}
