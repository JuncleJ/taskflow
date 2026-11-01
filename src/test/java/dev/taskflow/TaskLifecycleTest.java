package dev.taskflow;

import dev.taskflow.http.ApiException;
import dev.taskflow.task.*;
import io.vertx.core.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.function.*;

import static dev.taskflow.TaskExecutorTest.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Управляемые обработчики позволяют проверить обе стороны гонок без sleep и случайных таймингов.
 */
class TaskLifecycleTest {
    private Vertx vertx;

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void close() throws Exception {
        await(vertx.close());
    }

    private static final class Gate {
        final Promise<Void> work = Promise.promise();
        final IntFunction<Future<Void>> progress;
        final CancellationToken cancellation;

        Gate(IntFunction<Future<Void>> progress, CancellationToken cancellation) {
            this.progress = progress;
            this.cancellation = cancellation;
        }
    }

    private static final class Harness {
        final UUID owner = UUID.randomUUID();
        final MemoryStores.Tasks store;
        final List<Gate> gates = new ArrayList<>();
        final List<Task> events = new ArrayList<>();
        final List<Task> deleted = new ArrayList<>();
        final TaskService service;

        Harness(int parallelism) {
            this(parallelism, new MemoryStores.Tasks());
        }

        Harness(int parallelism, MemoryStores.Tasks store) {
            this.store = store;
            TaskHandler handler = new TaskHandler() {
                public String type() {
                    return "CONTROLLED";
                }

                public Future<Void> execute(IntFunction<Future<Void>> progress, CancellationToken cancellation) {
                    Gate gate = new Gate(progress, cancellation);
                    gates.add(gate);
                    return gate.work.future();
                }
            };
            TaskHandlerRegistry registry = new TaskHandlerRegistry(handler);
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
                assertEquals(task, store.tasks.get(task.id()), "Событие отправляется после сохранения");
                events.add(task);
            }, parallelism);
            service = new TaskService(store, registry, executor, deleted::add);
        }

        Task create() {
            return service.create(owner, "CONTROLLED").result();
        }

        Task current(Task task) {
            return store.tasks.get(task.id());
        }

        Future<Task> cancel(Task task) {
            return service.cancel(owner, task.id());
        }
    }

    private void check(int parallelism, Consumer<Harness> test) throws Exception {
        Promise<Void> result = Promise.promise();
        vertx.runOnContext(v -> {
            try {
                test.accept(new Harness(parallelism));
                result.complete();
            } catch (Throwable error) {
                result.fail(error);
            }
        });
        await(result.future());
    }

    private static void rejected(Future<?> result, int status) {
        assertTrue(result.isComplete());
        assertTrue(result.failed());
        assertEquals(status, assertInstanceOf(ApiException.class, result.cause()).status());
    }

    @Test
    @DisplayName("Отмена PENDING удаляет задачу из очереди, не запуская обработчик")
    void cancelPendingNeverExecutesIt() throws Exception {
        check(1, h -> {
            Task first = h.create(), pending = h.create(), next = h.create();
            Task cancelled = h.cancel(pending).result();
            assertEquals(Task.Status.CANCELLED, cancelled.status());
            assertNull(cancelled.startedAt());
            assertNotNull(cancelled.finishedAt());
            h.gates.getFirst().work.complete();
            assertEquals(2, h.gates.size());
            assertEquals(Task.Status.RUNNING, h.current(next).status());
            assertEquals(Task.Status.CANCELLED, h.current(pending).status());
            h.gates.get(1).work.complete();
            assertEquals(Task.Status.COMPLETED, h.current(first).status());
        });
    }

    @Test
    @DisplayName("RUNNING получает сигнал, но CANCELLED сохраняется лишь после остановки")
    void cancellationWaitsForHandlerAcknowledgement() throws Exception {
        check(1, h -> {
            Task task = h.create();
            Gate gate = h.gates.getFirst();
            assertTrue(gate.progress.apply(30).succeeded());
            Future<Task> cancellation = h.cancel(task);
            assertTrue(gate.cancellation.isRequested());
            assertFalse(cancellation.isComplete());
            assertEquals(Task.Status.RUNNING, h.current(task).status());
            assertTrue(gate.progress.apply(40).failed());
            gate.work.complete();
            assertEquals(Task.Status.CANCELLED, cancellation.result().status());
            assertEquals(30, cancellation.result().progress());
            assertNotNull(cancellation.result().finishedAt());
            for (Task.Status status : List.of(Task.Status.RUNNING, Task.Status.COMPLETED, Task.Status.FAILED))
                assertTrue(h.store.update(task.id(), status, 100, null).failed());
        });
    }

    @Test
    @DisplayName("Повторная отмена ожидает тот же исход и не меняет время завершения")
    void repeatedCancellationIsIdempotent() throws Exception {
        check(1, h -> {
            Task task = h.create();
            Future<Task> first = h.cancel(task), second = h.cancel(task);
            assertFalse(first.isComplete());
            assertFalse(second.isComplete());
            h.gates.getFirst().work.complete();
            assertEquals(first.result(), second.result());
            assertEquals(first.result(), h.cancel(task).result());
            assertEquals(1, h.events.stream().filter(t -> t.status() == Task.Status.CANCELLED).count());
        });
    }

    @Test
    @DisplayName("Отмена COMPLETED и FAILED возвращает конфликт, не меняя результат")
    void completedAndFailedTasksCannotBeCancelled() throws Exception {
        check(1, h -> {
            Task completed = h.create();
            h.gates.getFirst().work.complete();
            rejected(h.cancel(completed), 409);
            Task failed = h.create();
            h.gates.get(1).work.fail("Ошибка обработчика");
            rejected(h.cancel(failed), 409);
            assertEquals(Task.Status.COMPLETED, h.current(completed).status());
            assertEquals(Task.Status.FAILED, h.current(failed).status());
        });
    }

    @Test
    @DisplayName("Гонка: первым обрабатывается либо сигнал отмены, либо завершение обработчика")
    void bothRaceOrdersProduceOneTerminalOutcome() throws Exception {
        check(1, h -> {
            Task cancelled = h.create();
            Future<Task> request = h.cancel(cancelled);
            h.gates.getFirst().work.complete();
            assertEquals(Task.Status.CANCELLED, request.result().status());
            Task completed = h.create();
            h.gates.get(1).work.complete();
            rejected(h.cancel(completed), 409);
            for (Task task : List.of(cancelled, completed))
                assertEquals(1, h.events.stream().filter(t -> t.id().equals(task.id()) && t.terminal()).count());
        });
    }

    @Test
    @DisplayName("Другой пользователь не может отменить или удалить задачу")
    void operationsAreOwnerScoped() throws Exception {
        check(1, h -> {
            Task task = h.create();
            UUID stranger = UUID.randomUUID();
            rejected(h.service.cancel(stranger, task.id()), 404);
            rejected(h.service.delete(stranger, task.id()), 404);
            assertFalse(h.gates.getFirst().cancellation.isRequested());
            h.gates.getFirst().work.complete();
            rejected(h.service.delete(stranger, task.id()), 404);
            assertNotNull(h.current(task));
        });
    }

    @Test
    @DisplayName("Физическое удаление разрешено для всех трёх терминальных статусов")
    void terminalTasksCanBeDeletedAndMissingTasksReturn404() throws Exception {
        check(1, h -> {
            for (Task.Status status : List.of(Task.Status.COMPLETED, Task.Status.FAILED, Task.Status.CANCELLED)) {
                Task task = h.create();
                Gate gate = h.gates.getLast();
                if (status == Task.Status.CANCELLED) h.cancel(task);
                if (status == Task.Status.FAILED) gate.work.fail("Ошибка");
                else gate.work.complete();
                assertEquals(status, h.current(task).status());
                assertTrue(h.service.delete(h.owner, task.id()).succeeded());
                assertNull(h.current(task));
                rejected(h.service.delete(h.owner, task.id()), 404);
                rejected(h.cancel(task), 404);
            }
            assertEquals(3, h.deleted.size());
        });
    }

    @Test
    @DisplayName("Удаление PENDING и RUNNING запрещено")
    void activeTasksCannotBeDeleted() throws Exception {
        check(1, h -> {
            Task running = h.create(), pending = h.create();
            rejected(h.service.delete(h.owner, running.id()), 409);
            rejected(h.service.delete(h.owner, pending.id()), 409);
            assertTrue(h.deleted.isEmpty());
            h.cancel(pending);
            h.gates.getFirst().work.complete();
        });
    }

    @Test
    @DisplayName("Слот остаётся занят до подтверждения остановки, затем запускается следующая задача")
    void cancellationReleasesSlotOnlyAfterWorkStops() throws Exception {
        check(1, h -> {
            Task running = h.create(), queued = h.create();
            h.cancel(running);
            assertEquals(1, h.gates.size());
            assertEquals(Task.Status.PENDING, h.current(queued).status());
            h.gates.getFirst().work.complete();
            assertEquals(2, h.gates.size());
            assertEquals(Task.Status.RUNNING, h.current(queued).status());
            h.gates.getLast().work.complete();
        });
    }

    @Test
    @DisplayName("Демонстрационный таймер обнаруживает отмену и не публикует новые шаги")
    void timedHandlerCooperatesWithCancellation() throws Exception {
        Promise<Task> finished = Promise.promise();
        vertx.runOnContext(v -> {
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            TaskHandlerRegistry registry = new TaskHandlerRegistry(new TimedTaskHandler(vertx, "DEMO", 10, 10));
            TaskExecutor executor = new TaskExecutor(store, registry, task -> {
                if (task.terminal()) finished.complete(task);
            }, 1);
            TaskService service = new TaskService(store, registry, executor, task -> {
            });
            service.create(UUID.randomUUID(), "DEMO").onSuccess(task -> service.cancel(task.ownerId(), task.id()));
        });
        Task cancelled = await(finished.future());
        assertEquals(Task.Status.CANCELLED, cancelled.status());
        assertEquals(0, cancelled.progress());
    }

    @Test
    @DisplayName("CANCELLED ожидает незавершённую запись прогресса даже после остановки обработчика")
    void cancellationWaitsForProgressAlreadyInFlight() throws Exception {
        Promise<Void> done = Promise.promise();
        vertx.runOnContext(v -> {
            try {
                Promise<Void> write = Promise.promise();
                MemoryStores.Tasks store = new MemoryStores.Tasks() {
                    @Override
                    public Future<Task> update(UUID id, Task.Status status, int progress, String error) {
                        if (status == Task.Status.RUNNING && progress == 50)
                            return write.future().compose(ignored -> super.update(id, status, progress, error));
                        return super.update(id, status, progress, error);
                    }
                };
                Harness h = new Harness(1, store);
                Task task = h.create();
                Gate gate = h.gates.getFirst();
                gate.progress.apply(50);
                Future<Task> cancellation = h.cancel(task);
                gate.work.complete();
                assertFalse(cancellation.isComplete());
                assertEquals(Task.Status.RUNNING, h.current(task).status());
                write.complete();
                assertEquals(Task.Status.CANCELLED, cancellation.result().status());
                assertEquals(50, cancellation.result().progress());
                assertEquals(Task.Status.CANCELLED, h.events.getLast().status());
                done.complete();
            } catch (Throwable error) {
                done.fail(error);
            }
        });
        await(done.future());
    }

    @Test
    @DisplayName("Завершение, уже выбранное перед записью в БД, выигрывает у поздней отмены")
    void naturalCompletionWinsWhileFinalPersistenceIsPending() throws Exception {
        Promise<Void> done = Promise.promise();
        vertx.runOnContext(v -> {
            try {
                Promise<Void> write = Promise.promise();
                MemoryStores.Tasks store = new MemoryStores.Tasks() {
                    @Override
                    public Future<Task> update(UUID id, Task.Status status, int progress, String error) {
                        if (status == Task.Status.COMPLETED)
                            return write.future().compose(ignored -> super.update(id, status, progress, error));
                        return super.update(id, status, progress, error);
                    }
                };
                Harness h = new Harness(1, store);
                Task task = h.create();
                h.gates.getFirst().work.complete();
                Future<Task> cancellation = h.cancel(task);
                assertFalse(h.gates.getFirst().cancellation.isRequested());
                assertFalse(cancellation.isComplete());
                write.complete();
                rejected(cancellation, 409);
                assertEquals(Task.Status.COMPLETED, h.current(task).status());
                done.complete();
            } catch (Throwable error) {
                done.fail(error);
            }
        });
        await(done.future());
    }

    @Test
    @DisplayName("Отмена во время стартовой записи в БД не допускает запуска обработчика")
    void cancellationDuringStartDoesNotEnterHandler() throws Exception {
        Promise<Void> done = Promise.promise();
        vertx.runOnContext(v -> {
            try {
                Promise<Void> start = Promise.promise();
                MemoryStores.Tasks store = new MemoryStores.Tasks() {
                    @Override
                    public Future<Task> update(UUID id, Task.Status status, int progress, String error) {
                        if (status == Task.Status.RUNNING)
                            return start.future().compose(ignored -> super.update(id, status, progress, error));
                        return super.update(id, status, progress, error);
                    }
                };
                Harness h = new Harness(1, store);
                Task task = h.create();
                Future<Task> cancellation = h.cancel(task);
                assertFalse(cancellation.isComplete());
                assertTrue(h.gates.isEmpty());
                start.complete();
                assertEquals(Task.Status.CANCELLED, cancellation.result().status());
                assertTrue(h.gates.isEmpty());
                done.complete();
            } catch (Throwable error) {
                done.fail(error);
            }
        });
        await(done.future());
    }

    @Test
    @DisplayName("Повторная отмена PENDING во время записи CANCELLED не создаёт второй переход")
    void repeatedPendingCancellationSharesPersistence() throws Exception {
        Promise<Void> done = Promise.promise();
        vertx.runOnContext(v -> {
            try {
                Promise<Void> write = Promise.promise();
                int[] writes = {0};
                MemoryStores.Tasks store = new MemoryStores.Tasks() {
                    @Override
                    public Future<Task> update(UUID id, Task.Status status, int progress, String error) {
                        if (status == Task.Status.CANCELLED) {
                            writes[0]++;
                            return write.future().compose(ignored -> super.update(id, status, progress, error));
                        }
                        return super.update(id, status, progress, error);
                    }
                };
                Harness h = new Harness(1, store);
                h.create();
                Task pending = h.create();
                Future<Task> first = h.cancel(pending), second = h.cancel(pending);
                assertEquals(1, writes[0]);
                assertFalse(first.isComplete());
                assertFalse(second.isComplete());
                write.complete();
                assertEquals(Task.Status.CANCELLED, first.result().status());
                assertEquals(first.result(), second.result());
                h.gates.getFirst().work.complete();
                assertEquals(1, h.gates.size());
                done.complete();
            } catch (Throwable error) {
                done.fail(error);
            }
        });
        await(done.future());
    }
}
