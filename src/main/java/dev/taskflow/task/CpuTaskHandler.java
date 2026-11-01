package dev.taskflow.task;

import io.vertx.core.*;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/**
 * Вычисления выполняются только в фиксированном CPU-пуле; SQL и SSE остаются в исходном Vert.x context.
 */
public final class CpuTaskHandler implements TaskHandler {
    public static final int MAX_LIMIT = 10_000_000;
    private static final Logger LOG = LoggerFactory.getLogger(CpuTaskHandler.class);
    private final Vertx vertx;
    private final ThreadPoolExecutor workers;
    private final Set<Work> jobs = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private Future<Void> closing;

    public CpuTaskHandler(Vertx vertx, int workerCount, int queueCapacity) {
        if (workerCount < 1 || workerCount > 32)
            throw new IllegalArgumentException("CPU_WORKERS должен быть от 1 до 32");
        if (queueCapacity < 1 || queueCapacity > 256)
            throw new IllegalArgumentException("CPU_QUEUE_CAPACITY должен быть от 1 до 256");
        this.vertx = vertx;
        AtomicInteger sequence = new AtomicInteger();
        workers = new ThreadPoolExecutor(workerCount, workerCount, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> new Thread(runnable, "taskflow-cpu-" + sequence.incrementAndGet()),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public String type() {
        return "CPU_TASK";
    }

    public JsonObject parameters(JsonObject input) {
        return TaskParameters.integer(input, "limit", null, 2, MAX_LIMIT);
    }

    public Future<Void> execute(Task task, IntFunction<Future<Void>> progress, CancellationToken cancellation) {
        Context context = Vertx.currentContext();
        if (context == null) return Future.failedFuture("CPU_TASK требует Vert.x context");
        int limit = parameters(task.parameters()).getInteger("limit");
        if (closed.get()) return Future.failedFuture(new RejectedExecutionException("CPU-пул остановлен"));
        Work work = new Work(task, limit, context, progress, cancellation);
        jobs.add(work);
        cancellation.onCancellation(work::cancelIfQueued);
        try {
            workers.execute(work);
            if (cancellation.isRequested()) work.cancelIfQueued();
        } catch (RejectedExecutionException error) {
            jobs.remove(work);
            work.result.fail(error);
        }
        return work.result.future();
    }

    /**
     * Вызов из context приложения. Ожидание остановки пула выполняется в worker Vert.x, не в Event Loop.
     */
    public Future<Void> close() {
        if (closing != null) return closing;
        closed.set(true);
        jobs.forEach(work -> work.cancellation.request());
        workers.shutdown();
        closing = vertx.executeBlocking(() -> {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow().forEach(runnable -> ((Work) runnable).discard());
                if (!workers.awaitTermination(5, TimeUnit.SECONDS))
                    throw new IllegalStateException("CPU-пул не остановился");
            }
            return (Void) null;
        }, false);
        return closing;
    }

    private final class Work implements Runnable {
        final Task task;
        final int limit;
        final Context context;
        final IntFunction<Future<Void>> progress;
        final CancellationToken cancellation;
        final Promise<Void> result = Promise.promise();

        Work(Task task, int limit, Context context, IntFunction<Future<Void>> progress, CancellationToken cancellation) {
            this.task = task;
            this.limit = limit;
            this.context = context;
            this.progress = progress;
            this.cancellation = cancellation;
        }

        public void run() {
            try {
                if (closed.get()) cancellation.request();
                cancellation.check();
                LOG.info("Начаты CPU-вычисления taskId={} userId={} taskType=CPU_TASK limit={}", task.id(), task.ownerId(), limit);
                long count = countPrimes(limit, cancellation, this::report);
                context.runOnContext(v -> {
                    if (cancellation.isRequested()) result.tryFail(new CancellationException());
                    else {
                        LOG.info("Простые числа подсчитаны taskId={} userId={} taskType=CPU_TASK limit={} primeCount={}", task.id(), task.ownerId(), limit, count);
                        result.tryComplete();
                    }
                });
            } catch (Throwable error) {
                context.runOnContext(v -> result.tryFail(error));
            } finally {
                jobs.remove(this);
            }
        }

        void report(int value) {
            cancellation.check();
            CompletableFuture<Void> acknowledged = new CompletableFuture<>();
            context.runOnContext(v -> {
                try {
                    cancellation.check();
                    progress.apply(value).onComplete(outcome -> {
                        if (outcome.succeeded()) acknowledged.complete(null);
                        else acknowledged.completeExceptionally(outcome.cause());
                    });
                } catch (Throwable error) {
                    acknowledged.completeExceptionally(error);
                }
            });
            // Только CPU-worker ожидает подтверждения: не более одного события в полёте на задачу.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true) {
                cancellation.check();
                try {
                    acknowledged.get(100, TimeUnit.MILLISECONDS);
                    return;
                } catch (TimeoutException error) {
                    if (System.nanoTime() >= deadline)
                        throw new CompletionException("Истекло ожидание записи прогресса", error);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("CPU-worker прерван");
                } catch (ExecutionException error) {
                    throw new CompletionException(error.getCause());
                }
            }
        }

        void discard() {
            jobs.remove(this);
            cancellation.request();
            context.runOnContext(v -> result.tryFail(new CancellationException()));
        }

        void cancelIfQueued() {
            if (workers.remove(this)) discard();
        }
    }

    /**
     * Подсчёт на включительном диапазоне [2, limit], O(1) дополнительной памяти.
     */
    static long countPrimes(int limit, CancellationToken cancellation, IntConsumer progress) {
        if (limit < 2 || limit > MAX_LIMIT) throw new IllegalArgumentException("Некорректный limit");
        long count = 0;
        int lastProgress = 0;
        for (int candidate = 2; candidate <= limit; candidate++) {
            cancellation.check();
            boolean prime = candidate == 2 || candidate % 2 != 0;
            for (int divisor = 3; prime && divisor <= candidate / divisor; divisor += 2) {
                if (candidate % divisor == 0) prime = false;
            }
            if (prime) count++;
            int percent = (int) ((long) candidate * 100 / limit) / 5 * 5;
            if (percent > lastProgress) {
                progress.accept(percent);
                lastProgress = percent;
            }
        }
        cancellation.check();
        return count;
    }
}
