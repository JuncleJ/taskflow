package dev.taskflow.task;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.vertx.core.*;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.*;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class CpuTaskHandlerTest {
    private Vertx vertx;
    private Context context;
    private CpuTaskHandler cpu;
    private Logger logger;
    private Level previousLevel;
    private AppenderBase<ILoggingEvent> capture;
    private final List<String> computationThreads = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() {
        vertx = Vertx.vertx();
        context = vertx.getOrCreateContext();
        logger = (Logger) LoggerFactory.getLogger(CpuTaskHandler.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        capture = new AppenderBase<>() {
            protected void append(ILoggingEvent event) {
                if (event.getMessage().startsWith("Начаты CPU-вычисления"))
                    computationThreads.add(event.getThreadName());
            }
        };
        capture.start();
        logger.addAppender(capture);
    }

    @AfterEach
    void close() throws Exception {
        try {
            if (cpu != null) await(inContext(cpu::close));
        } finally {
            await(vertx.close());
            logger.detachAppender(capture);
            capture.stop();
            logger.setLevel(previousLevel);
        }
    }

    @Test
    @DisplayName("Подсчёт простых чисел корректен, прогресс ограничен двадцатью событиями")
    void primeCountsAndProgressAreCorrect() {
        int[] limits = {2, 3, 10, 100, 10_000, 1_000_000};
        long[] counts = {1, 2, 4, 25, 1229, 78498};
        for (int i = 0; i < limits.length; i++) {
            List<Integer> progress = new ArrayList<>();
            assertEquals(counts[i], CpuTaskHandler.countPrimes(limits[i], new CancellationToken(), progress::add));
            assertTrue(progress.size() <= 20);
            assertEquals(100, progress.getLast());
            assertEquals(progress.stream().distinct().sorted().toList(), progress);
        }
    }

    @Test
    @DisplayName("Два CPU-worker работают одновременно, callback прогресса возвращается в Vert.x context")
    void computationsAreParallelAndOutsideEventLoop() throws Exception {
        cpu = new CpuTaskHandler(vertx, 2, 1);
        Promise<Void> release = Promise.promise();
        CountDownLatch checkpoints = new CountDownLatch(2);
        java.util.function.IntFunction<Future<Void>> report = value -> {
            assertSame(context, Vertx.currentContext());
            assertTrue(Context.isOnEventLoopThread());
            if (value == 5) {
                checkpoints.countDown();
                return release.future();
            }
            return Future.succeededFuture();
        };
        Future<Void> first = inContext(() -> cpu.execute(task(100_000), report, new CancellationToken()));
        Future<Void> second = inContext(() -> cpu.execute(task(100_000), report, new CancellationToken()));
        assertTrue(checkpoints.await(5, TimeUnit.SECONDS));
        assertEquals(2, pool().getActiveCount());
        assertEquals(2, new HashSet<>(computationThreads).size());
        assertTrue(computationThreads.stream().allMatch(name -> name.startsWith("taskflow-cpu-")));
        assertFalse(first.isComplete());
        assertFalse(second.isComplete());
        inContext(() -> {
            release.complete();
            return null;
        });
        await(first);
        await(second);
    }

    @Test
    @DisplayName("Пул ограничен одним worker и одной ожидающей задачей; переполнение не выполняет работу в вызывающем потоке")
    void workerAndQueueLimitsAreEnforced() throws Exception {
        cpu = new CpuTaskHandler(vertx, 1, 1);
        Promise<Void> release = Promise.promise();
        CountDownLatch checkpoint = new CountDownLatch(1);
        Future<Void> first = inContext(() -> cpu.execute(task(100_000), value -> {
            if (value == 5) {
                checkpoint.countDown();
                return release.future();
            }
            return Future.succeededFuture();
        }, new CancellationToken()));
        assertTrue(checkpoint.await(5, TimeUnit.SECONDS));
        Future<Void> queued = inContext(() -> cpu.execute(task(10), value -> Future.succeededFuture(), new CancellationToken()));
        Future<Void> rejected = inContext(() -> cpu.execute(task(10), value -> Future.succeededFuture(), new CancellationToken()));
        assertTrue(rejected.failed());
        assertInstanceOf(RejectedExecutionException.class, rejected.cause());
        assertEquals(1, pool().getMaximumPoolSize());
        assertEquals(1, pool().getQueue().size());
        assertEquals(1, computationThreads.size());
        inContext(() -> {
            release.complete();
            return null;
        });
        await(first);
        await(queued);
        assertEquals(1, new HashSet<>(computationThreads).size());
    }

    @Test
    @DisplayName("Отмена останавливает CPU-worker даже при ожидающем подтверждении прогресса")
    void cancellationStopsActualComputation() throws Exception {
        cpu = new CpuTaskHandler(vertx, 1, 1);
        CancellationToken cancellation = new CancellationToken();
        Promise<Void> heldProgress = Promise.promise();
        CountDownLatch checkpoint = new CountDownLatch(1);
        List<Integer> progress = new CopyOnWriteArrayList<>();
        Future<Void> work = inContext(() -> cpu.execute(task(CpuTaskHandler.MAX_LIMIT), value -> {
            progress.add(value);
            checkpoint.countDown();
            return heldProgress.future();
        }, cancellation));
        assertTrue(checkpoint.await(5, TimeUnit.SECONDS));
        inContext(() -> {
            cancellation.request();
            return null;
        });
        assertThrows(CancellationException.class, () -> await(work));
        assertInstanceOf(CancellationException.class, work.cause());
        assertEquals(List.of(5), progress);
        inContext(() -> {
            heldProgress.complete();
            return null;
        });
    }

    @Test
    @DisplayName("Отмена задачи из очереди CPU-пула не ждёт освобождения занятого worker")
    void queuedCpuWorkCanBeCancelledBeforeItStarts() throws Exception {
        cpu = new CpuTaskHandler(vertx, 1, 1);
        Promise<Void> held = Promise.promise();
        CountDownLatch checkpoint = new CountDownLatch(1);
        Future<Void> first = inContext(() -> cpu.execute(task(100), value -> {
            checkpoint.countDown();
            return held.future();
        }, new CancellationToken()));
        assertTrue(checkpoint.await(5, TimeUnit.SECONDS));
        CancellationToken queuedToken = new CancellationToken();
        Future<Void> queued = inContext(() -> cpu.execute(task(100), value -> Future.succeededFuture(), queuedToken));
        inContext(() -> {
            queuedToken.request();
            return null;
        });
        assertThrows(CancellationException.class, () -> await(queued));
        assertInstanceOf(CancellationException.class, queued.cause());
        assertFalse(first.isComplete());
        assertTrue(pool().getQueue().isEmpty());
        assertEquals(1, computationThreads.size());
        inContext(() -> {
            held.complete();
            return null;
        });
        await(first);
    }

    @Test
    @DisplayName("Остановка пула отменяет активные и ожидающие задачи и завершает потоки")
    void shutdownReleasesActiveAndQueuedWork() throws Exception {
        cpu = new CpuTaskHandler(vertx, 1, 2);
        Promise<Void> held = Promise.promise();
        CountDownLatch checkpoint = new CountDownLatch(1);
        Future<Void> first = inContext(() -> cpu.execute(task(100), value -> {
            checkpoint.countDown();
            return held.future();
        }, new CancellationToken()));
        assertTrue(checkpoint.await(5, TimeUnit.SECONDS));
        Future<Void> queued = inContext(() -> cpu.execute(task(100), value -> Future.succeededFuture(), new CancellationToken()));
        await(inContext(cpu::close));
        assertTrue(pool().isTerminated());
        assertTrue(first.failed());
        assertTrue(queued.failed());
        Future<Void> rejected = inContext(() -> cpu.execute(task(100), value -> Future.succeededFuture(), new CancellationToken()));
        assertInstanceOf(RejectedExecutionException.class, rejected.cause());
        inContext(() -> {
            held.complete();
            return null;
        });
    }

    @Test
    @DisplayName("Ошибка записи прогресса завершает CPU-задачу, не оставляя worker зависшим")
    void progressFailureStopsWorker() throws Exception {
        cpu = new CpuTaskHandler(vertx, 1, 1);
        Future<Void> work = inContext(() -> cpu.execute(task(100), value -> Future.failedFuture("Ошибка прогресса"), new CancellationToken()));
        assertThrows(ExecutionException.class, () -> await(work));
        assertTrue(work.failed());
        await(inContext(cpu::close));
        assertTrue(pool().isTerminated());
    }

    @Test
    @DisplayName("Некорректные размеры CPU-пула и очереди отклоняются")
    void invalidPoolConfigurationIsRejected() {
        for (int workers : new int[]{0, -1, 33})
            assertThrows(IllegalArgumentException.class, () -> new CpuTaskHandler(vertx, workers, 1));
        for (int queue : new int[]{0, -1, 257})
            assertThrows(IllegalArgumentException.class, () -> new CpuTaskHandler(vertx, 1, queue));
    }

    @Test
    @DisplayName("CPU limit обязателен, целочисленен и ограничен диапазоном 2–10000000")
    void cpuLimitIsValidatedBeforeScheduling() {
        cpu = new CpuTaskHandler(vertx, 1, 1);
        for (int limit : new int[]{2, CpuTaskHandler.MAX_LIMIT})
            assertEquals(limit, cpu.parameters(new JsonObject().put("limit", limit)).getInteger("limit"));
        assertThrows(IllegalArgumentException.class, () -> cpu.parameters(new JsonObject()));
        for (Object invalid : Arrays.asList(-1, 0, 1, CpuTaskHandler.MAX_LIMIT + 1, 2.5, "100", true, null, Long.MAX_VALUE))
            assertThrows(IllegalArgumentException.class, () -> cpu.parameters(new JsonObject().put("limit", invalid)));
        assertThrows(IllegalArgumentException.class, () -> cpu.parameters(new JsonObject().put("limit", 100).put("durationSeconds", 5)));
    }

    private ThreadPoolExecutor pool() throws Exception {
        Field field = CpuTaskHandler.class.getDeclaredField("workers");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(cpu);
    }

    private static Task task(int limit) {
        return new Task(UUID.randomUUID(), UUID.randomUUID(), "CPU_TASK", Task.Status.RUNNING, 0,
                OffsetDateTime.now(), OffsetDateTime.now(), null, null, new JsonObject().put("limit", limit));
    }

    private <T> T inContext(Supplier<T> action) throws Exception {
        Promise<T> value = Promise.promise();
        context.runOnContext(v -> {
            try {
                value.complete(action.get());
            } catch (Throwable error) {
                value.fail(error);
            }
        });
        return await(value.future());
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
