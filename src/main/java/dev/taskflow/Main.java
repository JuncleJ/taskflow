package dev.taskflow;

import dev.taskflow.auth.*;
import dev.taskflow.http.*;
import dev.taskflow.persistence.*;
import dev.taskflow.task.*;
import io.vertx.core.*;
import io.vertx.pgclient.*;
import io.vertx.sqlclient.*;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.LoggerContext;

public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Application()).onFailure(error -> {
            LOG.error("Запуск не удался errorType={} stack={}", error.getClass().getName(), Arrays.toString(error.getStackTrace()));
            vertx.close().onComplete(v -> System.exit(1));
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                vertx.close().toCompletionStage().toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error("Не удалось завершить приложение errorType={} stack={}", e.getClass().getName(), Arrays.toString(e.getStackTrace()));
            } finally {
                ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
            }
        }));
    }

    public static final class Application extends AbstractVerticle {
        private Pool pool;
        private TaskExecutor executor;
        private CpuTaskHandler cpuHandler;

        @Override
        public void start(Promise<Void> promise) {
            String password = System.getenv("DB_PASSWORD");
            if (password == null || password.isBlank()) {
                promise.fail("Задайте DB_PASSWORD");
                return;
            }
            PgConnectOptions options = new PgConnectOptions().setHost(env("DB_HOST", "localhost"))
                    .setPort(number("DB_PORT", 5432)).setDatabase(env("DB_NAME", "taskflow"))
                    .setUser(env("DB_USER", "taskflow")).setPassword(password);
            pool = PgBuilder.pool().using(vertx).with(new PoolOptions().setMaxSize(10)).connectingTo(options).build();
            // Файловая операция также асинхронна; схема поставляется в classpath.
            Database.initialize(vertx, pool)
                    .compose(v -> vertx.executeBlocking(() -> new PasswordHasher().hash(PasswordHasher.newToken()), false))
                    .compose(dummyHash -> {
                        SseNotifications notifications = new SseNotifications(vertx);
                        cpuHandler = new CpuTaskHandler(vertx, number("CPU_WORKERS", 2), number("CPU_QUEUE_CAPACITY", 16));
                        TaskHandlerRegistry registry = new TaskHandlerRegistry(new DemoTaskHandler(vertx), new TimedTaskHandler(vertx, "REPORT", 4, 1000), cpuHandler);
                        PgTaskStore store = new PgTaskStore(pool);
                        executor = new TaskExecutor(store, registry, notifications::publish, number("TASK_PARALLELISM", 3), number("TASK_QUEUE_CAPACITY", 100));
                        return vertx.createHttpServer().requestHandler(Routes.create(vertx,
                                        new AuthService(vertx, new PgAuthStore(pool), dummyHash), new TaskService(store, registry, executor, notifications::deleted), notifications))
                                .listen(number("HTTP_PORT", 8080));
                    }).onSuccess(server -> {
                        LOG.info("TaskFlow запущен port={}", server.actualPort());
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        }

        @Override
        public void stop(Promise<Void> promise) {
            if (executor != null) executor.stop();
            Future<Void> stopped = cpuHandler == null ? Future.succeededFuture() : cpuHandler.close();
            stopped.transform(cpuResult -> {
                Future<Void> closePool = pool == null ? Future.succeededFuture() : pool.close();
                return closePool.transform(poolResult -> cpuResult.failed() ? Future.<Void>failedFuture(cpuResult.cause())
                        : poolResult.failed() ? Future.<Void>failedFuture(poolResult.cause()) : Future.<Void>succeededFuture());
            }).onComplete(promise);
        }
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }

    private static int number(String name, int fallback) {
        int value = Integer.parseInt(env(name, String.valueOf(fallback)));
        if (value < 1) throw new IllegalArgumentException(name + " должен быть положительным");
        return value;
    }
}
