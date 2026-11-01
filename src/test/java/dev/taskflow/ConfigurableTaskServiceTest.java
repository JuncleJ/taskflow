package dev.taskflow;

import dev.taskflow.http.ApiException;
import dev.taskflow.task.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.function.*;

import static dev.taskflow.TaskExecutorTest.await;
import static org.junit.jupiter.api.Assertions.*;

class ConfigurableTaskServiceTest {
    private Vertx vertx;

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void close() throws Exception {
        await(vertx.close());
    }

    private void check(Runnable test) throws Exception {
        Promise<Void> done = Promise.promise();
        vertx.runOnContext(v -> {
            try {
                test.run();
                done.complete();
            } catch (Throwable error) {
                done.fail(error);
            }
        });
        await(done.future());
    }

    @Test
    @DisplayName("DEMO сохраняет нормализованные параметры; Task защищён от внешнего изменения JSON")
    void parametersArePersistedAndDefensivelyCopied() throws Exception {
        check(() -> {
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            TaskHandlerRegistry registry = new TaskHandlerRegistry(new DemoTaskHandler(vertx));
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
            }, 1, 1);
            TaskService service = new TaskService(store, registry, executor, task -> {
            });
            UUID owner = UUID.randomUUID();
            Task defaults = service.create(owner, "DEMO").result();
            assertEquals(5, defaults.parameters().getInteger("durationSeconds"));
            JsonObject input = new JsonObject().put("durationSeconds", 300);
            Task configured = service.create(owner, "DEMO", input).result();
            input.put("durationSeconds", 1);
            configured.parameters().put("durationSeconds", 2);
            assertEquals(300, store.tasks.get(configured.id()).parameters().getInteger("durationSeconds"));
            service.cancel(owner, configured.id());
            executor.stop();
        });
    }

    @Test
    @DisplayName("Невалидные параметры не создают строку задачи и не занимают место в очереди")
    void invalidParametersAreRejectedBeforeInsert() throws Exception {
        check(() -> {
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            TaskHandlerRegistry registry = new TaskHandlerRegistry(new DemoTaskHandler(vertx));
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
            }, 1, 1);
            TaskService service = new TaskService(store, registry, executor, task -> {
            });
            for (Object invalid : Arrays.asList(0, 301, null, "5", 1.5))
                assertThrows(IllegalArgumentException.class, () -> service.create(UUID.randomUUID(), "DEMO", new JsonObject().put("durationSeconds", invalid)));
            assertTrue(store.tasks.isEmpty());
            assertNotNull(service.create(UUID.randomUUID(), "DEMO").result());
            executor.stop();
        });
    }

    @Test
    @DisplayName("Ограничение общей очереди учитывает создания, ещё ожидающие ответа БД")
    void queueReservationsBoundConcurrentCreation() throws Exception {
        check(() -> {
            Promise<Void> inserted = Promise.promise();
            MemoryStores.Tasks store = new MemoryStores.Tasks() {
                @Override
                public Future<Task> create(UUID owner, String type, JsonObject parameters) {
                    return inserted.future().compose(v -> super.create(owner, type, parameters));
                }
            };
            Promise<Void> work = Promise.promise();
            TaskHandler handler = controlled(work);
            TaskHandlerRegistry registry = new TaskHandlerRegistry(handler);
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
            }, 1, 1);
            TaskService service = new TaskService(store, registry, executor, task -> {
            });
            Future<Task> first = service.create(UUID.randomUUID(), handler.type());
            Future<Task> second = service.create(UUID.randomUUID(), handler.type());
            ApiException rejected = assertThrows(ApiException.class, () -> service.create(UUID.randomUUID(), handler.type()));
            assertEquals(429, rejected.status());
            assertTrue(store.tasks.isEmpty());
            inserted.complete();
            assertTrue(first.succeeded());
            assertTrue(second.succeeded());
            assertEquals(2, store.tasks.size());
            executor.stop();
            work.complete();
        });
    }

    @Test
    @DisplayName("Асинхронная и синхронная ошибки создания в БД освобождают резерв общей очереди")
    void failedInsertReleasesReservation() throws Exception {
        check(() -> {
            int[] attempts = {0};
            MemoryStores.Tasks store = new MemoryStores.Tasks() {
                @Override
                public Future<Task> create(UUID owner, String type, JsonObject parameters) {
                    if (attempts[0]++ == 0) return Future.failedFuture("Ошибка БД");
                    if (attempts[0] == 2) throw new IllegalStateException("Синхронная ошибка БД");
                    return super.create(owner, type, parameters);
                }
            };
            Promise<Void> work = Promise.promise();
            TaskHandler handler = controlled(work);
            TaskHandlerRegistry registry = new TaskHandlerRegistry(handler);
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
            }, 1, 1);
            TaskService service = new TaskService(store, registry, executor, task -> {
            });
            assertTrue(service.create(UUID.randomUUID(), handler.type()).failed());
            assertTrue(service.create(UUID.randomUUID(), handler.type()).failed());
            assertTrue(service.create(UUID.randomUUID(), handler.type()).succeeded());
            assertTrue(service.create(UUID.randomUUID(), handler.type()).succeeded());
            assertEquals(2, store.tasks.size());
            executor.stop();
            work.complete();
        });
    }

    private static TaskHandler controlled(Promise<Void> work) {
        return new TaskHandler() {
            public String type() {
                return "CONTROLLED";
            }

            public Future<Void> execute(Task task, IntFunction<Future<Void>> progress, CancellationToken cancellation) {
                return work.future();
            }
        };
    }
}
