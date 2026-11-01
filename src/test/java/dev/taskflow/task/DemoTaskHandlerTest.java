package dev.taskflow.task;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.*;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class DemoTaskHandlerTest {
    private Vertx vertx;

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void close() throws Exception {
        await(vertx.close());
    }

    @Test
    @DisplayName("DEMO по умолчанию длится 5 секунд, прогресс идёт по времени")
    void defaultDurationIsActuallyFiveSeconds() throws Exception {
        Promise<Void> finished = Promise.promise();
        List<Integer> progress = new CopyOnWriteArrayList<>();
        long start = System.nanoTime();
        vertx.runOnContext(v -> new DemoTaskHandler(vertx).execute(task(new JsonObject()), value -> {
            assertTrue(Context.isOnEventLoopThread());
            progress.add(value);
            return io.vertx.core.Future.succeededFuture();
        }, new CancellationToken()).onComplete(finished));
        await(finished.future());
        assertTrue(System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(5));
        assertEquals(100, progress.getLast());
        assertEquals(progress.stream().distinct().sorted().toList(), progress);
        assertTrue(progress.size() <= 100);
    }

    @Test
    @DisplayName("Диапазон durationSeconds включает 1 и 300, значение по умолчанию — 5")
    void durationBoundsAndDefaultAreValidated() {
        DemoTaskHandler demo = new DemoTaskHandler(vertx);
        assertEquals(5, demo.parameters(new JsonObject()).getInteger("durationSeconds"));
        for (int seconds : new int[]{1, 300})
            assertEquals(seconds, demo.parameters(new JsonObject().put("durationSeconds", seconds)).getInteger("durationSeconds"));
        for (Object invalid : Arrays.asList(0, -1, 301, 1.5, "5", true, null, Long.MAX_VALUE, new JsonObject()))
            assertThrows(IllegalArgumentException.class, () -> demo.parameters(new JsonObject().put("durationSeconds", invalid)));
        assertThrows(IllegalArgumentException.class, () -> demo.parameters(new JsonObject().put("limit", 100)));
    }

    @Test
    @DisplayName("Длинный DEMO обнаруживает отмену на ближайшем тике, не ожидая все 300 секунд")
    void longDemoCooperatesWithCancellation() throws Exception {
        Promise<Void> finished = Promise.promise();
        CancellationToken token = new CancellationToken();
        List<Integer> progress = new CopyOnWriteArrayList<>();
        vertx.runOnContext(v -> {
            new DemoTaskHandler(vertx).execute(task(new JsonObject().put("durationSeconds", 300)), value -> {
                progress.add(value);
                return io.vertx.core.Future.succeededFuture();
            }, token).onComplete(finished);
            token.request();
        });
        assertThrows(CancellationException.class, () -> await(finished.future()));
        assertInstanceOf(CancellationException.class, finished.future().cause());
        assertTrue(progress.isEmpty());
    }

    private static Task task(JsonObject parameters) {
        return new Task(UUID.randomUUID(), UUID.randomUUID(), "DEMO", Task.Status.RUNNING, 0,
                OffsetDateTime.now(), OffsetDateTime.now(), null, null, parameters);
    }

    private static <T> T await(io.vertx.core.Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
