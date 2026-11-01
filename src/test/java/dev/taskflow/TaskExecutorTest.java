package dev.taskflow;

import dev.taskflow.task.*;
import io.vertx.core.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

class TaskExecutorTest {
    private Vertx vertx;

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void close() throws Exception {
        await(vertx.close());
    }

    static <T> T await(io.vertx.core.Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
    }

    @Test
    void lifecycleAndProgressArePersistedBeforeNotification() throws Exception {
        Promise<List<Task>> done = Promise.promise();
        vertx.runOnContext(v -> {
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            List<Task> events = new ArrayList<>();
            TaskHandlerRegistry registry = new TaskHandlerRegistry(new TimedTaskHandler(vertx, "DEMO", 4, 10));
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
                assertEquals(task, store.tasks.get(task.id()));
                events.add(task);
                if (task.terminal()) done.complete(events);
            }, 2);
            store.create(UUID.randomUUID(), "DEMO").onSuccess(task -> {
                assertEquals(Task.Status.PENDING, task.status());
                executor.submit(task);
            });
        });
        List<Task> events = await(done.future());
        assertEquals(List.of(0, 25, 50, 75, 100, 100), events.stream().map(Task::progress).toList());
        assertEquals(Task.Status.RUNNING, events.getFirst().status());
        Task end = events.getLast();
        assertEquals(Task.Status.COMPLETED, end.status());
        assertNotNull(end.startedAt());
        assertNotNull(end.finishedAt());
    }

    @Test
    void parallelismIsBoundedAndQueueContinuesAfterFailure() throws Exception {
        Promise<Void> done = Promise.promise();
        vertx.runOnContext(v -> {
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            List<Promise<Void>> running = new ArrayList<>();
            int[] completed = {0};
            TaskHandler handler = new TaskHandler() {
                public String type() {
                    return "CONTROLLED";
                }

                public io.vertx.core.Future<Void> execute(IntFunction<io.vertx.core.Future<Void>> progress) {
                    Promise<Void> gate = Promise.promise();
                    running.add(gate);
                    return gate.future();
                }
            };
            TaskExecutor executor = new TaskExecutor(store, new TaskHandlerRegistry(handler), task -> {
                if (task.terminal() && ++completed[0] == 3) done.complete();
            }, 2);
            for (int i = 0; i < 3; i++) store.create(UUID.randomUUID(), handler.type()).onSuccess(executor::submit);
            assertEquals(2, running.size());
            assertEquals(1, store.tasks.values().stream().filter(t -> t.status() == Task.Status.PENDING).count());
            running.getFirst().fail("Сбой обработчика");
            assertEquals(3, running.size());
            running.get(1).complete();
            running.get(2).complete();
            assertEquals(1, store.tasks.values().stream().filter(t -> t.status() == Task.Status.FAILED).count());
            assertEquals(2, store.tasks.values().stream().filter(t -> t.status() == Task.Status.COMPLETED).count());
        });
        await(done.future());
    }

    @Test
    void invalidProgressFailsTaskAndUnknownTypeIsRejected() throws Exception {
        Promise<Task> done = Promise.promise();
        vertx.runOnContext(v -> {
            TaskHandler handler = new TaskHandler() {
                public String type() {
                    return "BAD";
                }

                public io.vertx.core.Future<Void> execute(IntFunction<io.vertx.core.Future<Void>> progress) {
                    return progress.apply(101);
                }
            };
            TaskHandlerRegistry registry = new TaskHandlerRegistry(handler);
            assertThrows(IllegalArgumentException.class, () -> registry.get("UNKNOWN"));
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
                if (task.terminal()) done.complete(task);
            }, 1);
            store.create(UUID.randomUUID(), "BAD").onSuccess(executor::submit);
        });
        Task failed = await(done.future());
        assertEquals(Task.Status.FAILED, failed.status());
        assertNotNull(failed.error());
    }
}
