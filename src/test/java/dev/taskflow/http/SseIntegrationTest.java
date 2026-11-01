package dev.taskflow.http;

import dev.taskflow.auth.*;
import dev.taskflow.persistence.*;
import dev.taskflow.task.*;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.*;
import io.vertx.sqlclient.Pool;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.*;
import java.lang.reflect.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Реальные HTTP/SSE, авторизация, исполнитель и PostgreSQL в отдельной случайной схеме.
 * CONTROLLED используется для детерминированных сценариев; DEMO и CPU_TASK — настоящие обработчики приложения.
 */
@EnabledIfEnvironmentVariable(named = "TEST_DB_PORT", matches = ".+")
class SseIntegrationTest {
    private Vertx vertx;
    private Context context;
    private Pool admin;
    private Pool pool;
    private String schema;
    private HttpServer server;
    private TaskExecutor executor;
    private CpuTaskHandler cpu;
    private Promise<Void> cpuCheckpointGate;
    private SseNotifications notifications;
    private PgTaskStore store;
    private final ControlledHandler handler = new ControlledHandler();
    private final Set<Long> heartbeatTimers = new HashSet<>();
    private final List<SseClient> subscriptions = new ArrayList<>();
    private final List<Throwable> runtimeErrors = new CopyOnWriteArrayList<>();
    private HttpClient client;
    private String base;
    private String token;
    private UUID owner;
    private boolean holdTerminalNotifications;

    @BeforeEach
    void start() throws Exception {
        vertx = Vertx.vertx();
        vertx.exceptionHandler(runtimeErrors::add);
        context = vertx.getOrCreateContext();
        context.exceptionHandler(runtimeErrors::add);
        PgConnectOptions options = new PgConnectOptions()
                .setHost(System.getenv().getOrDefault("TEST_DB_HOST", "localhost"))
                .setPort(Integer.parseInt(System.getenv("TEST_DB_PORT")))
                .setDatabase(System.getenv().getOrDefault("TEST_DB_NAME", "postgres"))
                .setUser(System.getenv().getOrDefault("TEST_DB_USER", "postgres"))
                .setPassword(System.getenv().getOrDefault("TEST_DB_PASSWORD", ""));
        admin = PgBuilder.pool().using(vertx).connectingTo(options).build();
        schema = "sse_" + UUID.randomUUID().toString().replace("-", "");
        await(admin.query("CREATE SCHEMA " + schema).execute());
        pool = PgBuilder.pool().using(vertx)
                .connectingTo(new PgConnectOptions(options).addProperty("search_path", schema)).build();
        await(Database.initialize(vertx, pool));
        String dummyHash = new PasswordHasher().hash(PasswordHasher.newToken());
        server = onContext(() -> {
            // Прокси только наблюдает реальные setPeriodic/cancelTimer, не заменяя таймеры моками.
            Vertx trackedTimers = (Vertx) Proxy.newProxyInstance(Vertx.class.getClassLoader(), new Class<?>[]{Vertx.class},
                    (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(vertx, args);
                            if (method.getName().equals("setPeriodic")) heartbeatTimers.add((Long) result);
                            if (method.getName().equals("cancelTimer") && Boolean.TRUE.equals(result))
                                heartbeatTimers.remove((Long) args[0]);
                            return result;
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            notifications = new SseNotifications(trackedTimers, 50);
            store = new PgTaskStore(pool);
            cpu = new CpuTaskHandler(vertx, 2, 2);
            TaskHandler observedCpu = new TaskHandler() {
                public String type() {
                    return cpu.type();
                }

                public JsonObject parameters(JsonObject input) {
                    return cpu.parameters(input);
                }

                public io.vertx.core.Future<Void> execute(Task task, IntFunction<io.vertx.core.Future<Void>> progress, CancellationToken cancellation) {
                    // Тест может задержать подтверждение первого сохранённого checkpoint, не подменяя вычисления/SQL.
                    return cpu.execute(task, value -> progress.apply(value).compose(v ->
                            cpuCheckpointGate != null && value == 5 ? cpuCheckpointGate.future() : io.vertx.core.Future.succeededFuture()), cancellation);
                }
            };
            TaskHandlerRegistry registry = new TaskHandlerRegistry(handler, new DemoTaskHandler(vertx), observedCpu);
            executor = new TaskExecutor(store, registry, task -> {
                // Управляемое окно между сохранением терминального статуса и его SSE-доставкой.
                if (!holdTerminalNotifications || !task.terminal()) notifications.publish(task);
            }, 3);
            return vertx.createHttpServer().requestHandler(Routes.create(vertx,
                    new AuthService(vertx, new PgAuthStore(pool), dummyHash),
                    new TaskService(store, registry, executor, notifications::deleted), notifications)).listen(0);
        });
        base = "http://localhost:" + server.actualPort();
        client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5)).build();
        JsonObject registration = json("POST", "/api/auth/register",
                new JsonObject().put("login", "sse_user").put("password", PasswordHasher.newToken()), 201);
        token = registration.getString("token");
        owner = await(new PgAuthStore(pool).authenticate(PasswordHasher.tokenHash(token)));
    }

    @AfterEach
    void close() throws Exception {
        try {
            for (SseClient subscription : subscriptions) subscription.close();
            if (server != null) await(server.close());
            if (executor != null) onContext(() -> {
                executor.stop();
                return io.vertx.core.Future.succeededFuture();
            });
            if (cpu != null) await(cpu.close());
            if (cpuCheckpointGate != null) cpuCheckpointGate.tryComplete();
            if (pool != null) await(pool.close());
            if (admin != null && schema != null) await(admin.query("DROP SCHEMA " + schema + " CASCADE").execute());
        } finally {
            if (client != null) client.close();
            if (vertx != null) await(vertx.close());
        }
        assertTrue(runtimeErrors.isEmpty(), runtimeErrors.toString());
    }

    @Test
    @DisplayName("1. Позднее подключение получает текущие 40% и следующие шаги")
    void subscriptionDuringExecutionReceivesCurrentAndFutureProgress() throws Exception {
        // Пользователь, открывший страницу позже, должен сразу увидеть сохранённое состояние.
        UUID id = createTask();
        advance(40);
        SseClient sse = subscribe(id);
        assertEvent(sse.next(), "progress", "RUNNING", 40);
        for (int progress = 50; progress <= 100; progress += 10) {
            advance(progress);
            sse.expect("progress", "RUNNING", progress);
        }
        finish(id);
        sse.expect("completed", "COMPLETED", 100);
        sse.expectEnd();
    }

    @Test
    @DisplayName("2. Два подписчика одной задачи получают одинаковые обновления")
    void twoSubscribersReceiveTheSameSnapshots() throws Exception {
        // Проверяет рассылку всем вкладкам, а не замену предыдущего подписчика новым.
        UUID id = createTask();
        try (SseClient first = subscribe(id); SseClient second = subscribe(id)) {
            assertEquals(first.next(), second.next());
            for (int progress : new int[]{30, 60, 100}) {
                advance(progress);
                assertEquals(first.expect("progress", "RUNNING", progress), second.expect("progress", "RUNNING", progress));
            }
            finish(id);
            assertEquals(first.expect("completed", "COMPLETED", 100), second.expect("completed", "COMPLETED", 100));
            first.expectEnd();
            second.expectEnd();
        }
    }

    @Test
    @DisplayName("3. Закрытие клиента не останавливает фоновую задачу")
    void taskCompletesWithoutAnOpenSubscription() throws Exception {
        // Жизненный цикл задачи не должен зависеть от наличия зрителя.
        UUID id = createTask();
        advance(30);
        SseClient sse = subscribe(id);
        assertEvent(sse.next(), "progress", "RUNNING", 30);
        sse.close();
        awaitResources(new Resources(0, 0, 0, 0));
        advance(70);
        finish(id);
        JsonObject task = json("GET", "/api/tasks/" + id, null, 200);
        assertEquals("COMPLETED", task.getString("status"));
        assertEquals(100, task.getInteger("progress"));
        assertEquals(Task.Status.COMPLETED, await(store.find(owner, id)).status());
    }

    @Test
    @DisplayName("4. Переподключение получает актуальные 70%, затем завершение")
    void reconnectionReceivesCurrentSnapshotInsteadOfReplayingOldSteps() throws Exception {
        // Проверяет серверное восстановление подписки; автоматический retry браузера здесь не тестируется.
        UUID id = createTask();
        advance(30);
        SseClient original = subscribe(id);
        assertEvent(original.next(), "progress", "RUNNING", 30);
        original.close();
        awaitResources(new Resources(0, 0, 0, 0));
        advance(70);
        try (SseClient reconnected = subscribe(id)) {
            assertEvent(reconnected.next(), "progress", "RUNNING", 70);
            advance(80);
            reconnected.expect("progress", "RUNNING", 80);
            finish(id);
            reconnected.expect("completed", "COMPLETED", 100);
            reconnected.expectEnd();
        }
    }

    @Test
    @DisplayName("5. Подписка после завершения сразу получает COMPLETED и закрывается")
    void subscriptionAfterCompletionReceivesFinalStateAndEof() throws Exception {
        // Устраняет зависимость результата от момента открытия SSE-соединения.
        UUID id = createTask();
        advance(100);
        finish(id);
        try (SseClient sse = subscribe(id)) {
            assertEvent(sse.next(), "completed", "COMPLETED", 100);
            sse.expectEnd();
        }
        awaitResources(new Resources(0, 0, 0, 0));
    }

    @Test
    @DisplayName("6. Отключение удаляет подписчиков, снимки и heartbeat-таймеры")
    void disconnectionReleasesResourcesAndDoesNotAffectRemainingClients() throws Exception {
        // Проверяет фактическую очистку коллекций и отмену таймеров до завершения самой задачи.
        UUID id = createTask();
        List<SseClient> clients = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SseClient sse = subscribe(id);
            assertEvent(sse.next(), "progress", "RUNNING", 0);
            clients.add(sse);
        }
        awaitResources(new Resources(1, 4, 4, 4));
        clients.get(0).close();
        clients.get(1).close();
        awaitResources(new Resources(1, 2, 2, 2));
        advance(50);
        clients.get(2).expect("progress", "RUNNING", 50);
        clients.get(3).expect("progress", "RUNNING", 50);
        clients.get(2).close();
        clients.get(3).close();
        awaitResources(new Resources(0, 0, 0, 0));
        finish(id);
    }

    @Test
    @DisplayName("Отмена по HTTP ждёт остановки, проверяет владельца и доставляет SSE cancelled")
    void cancellationIsOwnerScopedCooperativeAndDeliveredThroughSse() throws Exception {
        UUID id = createTask();
        advance(30);
        String originalToken = token;
        token = json("POST", "/api/auth/register", new JsonObject().put("login", "other_user")
                .put("password", PasswordHasher.newToken()), 201).getString("token");
        json("POST", "/api/tasks/" + id + "/cancel", null, 404);
        json("DELETE", "/api/tasks/" + id, null, 404);
        token = originalToken;
        json("DELETE", "/api/tasks/" + id, null, 409);
        try (SseClient sse = subscribe(id)) {
            assertEvent(sse.next(), "progress", "RUNNING", 30);
            CompletableFuture<HttpResponse<String>> cancellation = requestCancellation(id);
            awaitCondition(() -> onContext(() -> io.vertx.core.Future.succeededFuture(handler.cancellation.isRequested())));
            assertFalse(cancellation.isDone(), "Ответ не должен подтверждать отмену ещё работающего обработчика");
            assertEquals(Task.Status.RUNNING, await(store.find(owner, id)).status());
            onContext(() -> {
                handler.result.complete();
                return io.vertx.core.Future.succeededFuture();
            });
            HttpResponse<String> response = cancellation.get(5, TimeUnit.SECONDS);
            assertEquals(200, response.statusCode(), response.body());
            JsonObject cancelled = new JsonObject(response.body());
            assertEquals("CANCELLED", cancelled.getString("status"));
            sse.expect("cancelled", "CANCELLED", 30);
            sse.expectEnd();
            assertEquals(cancelled, json("POST", "/api/tasks/" + id + "/cancel", null, 200));
        }
        awaitResources(new Resources(0, 0, 0, 0));
        assertEquals(Task.Status.CANCELLED, await(store.find(owner, id)).status());
        json("DELETE", "/api/tasks/" + id, null, 204);
        assertNull(await(store.find(owner, id)));
        json("GET", "/api/tasks/" + id, null, 404);
        json("POST", "/api/tasks/" + id + "/cancel", null, 404);
        json("DELETE", "/api/tasks/" + id, null, 404);
    }

    @Test
    @DisplayName("Гонка HTTP-отмены и естественного завершения сохраняет один согласованный результат")
    void cancellationRacingWithCompletionHasConsistentResult() throws Exception {
        UUID id = createTask();
        advance(50);
        try (SseClient sse = subscribe(id)) {
            assertEvent(sse.next(), "progress", "RUNNING", 50);
            CompletableFuture<HttpResponse<String>> cancellation = requestCancellation(id);
            onContext(() -> {
                handler.result.complete();
                return io.vertx.core.Future.succeededFuture();
            });
            HttpResponse<String> response = cancellation.get(5, TimeUnit.SECONDS);
            assertTrue(response.statusCode() == 200 || response.statusCode() == 409);
            Task.Status expected = response.statusCode() == 200 ? Task.Status.CANCELLED : Task.Status.COMPLETED;
            awaitCondition(() -> await(store.find(owner, id)).terminal());
            Task finalTask = await(store.find(owner, id));
            assertEquals(expected, finalTask.status());
            sse.expect(expected == Task.Status.CANCELLED ? "cancelled" : "completed", expected.name(), finalTask.progress());
            sse.expectEnd();
            assertTrue(json("GET", "/api/tasks/" + id, null, 200).getString("status").equals(expected.name()));
        }
    }

    @Test
    @DisplayName("Удаление COMPLETED физически удаляет строку и закрывает ещё активную SSE-подписку")
    void deletionClosesSubscriptionInFinalNotificationWindow() throws Exception {
        UUID id = createTask();
        advance(50);
        try (SseClient sse = subscribe(id)) {
            assertEvent(sse.next(), "progress", "RUNNING", 50);
            onContext(() -> {
                holdTerminalNotifications = true;
                return io.vertx.core.Future.succeededFuture();
            });
            finish(id);
            json("POST", "/api/tasks/" + id + "/cancel", null, 409);
            awaitResources(new Resources(1, 1, 1, 1));
            json("DELETE", "/api/tasks/" + id, null, 204);
            sse.expectDeleted(id);
            sse.expectEnd();
            assertNull(await(store.find(owner, id)));
        }
        awaitResources(new Resources(0, 0, 0, 0));
    }

    @Test
    @DisplayName("HTTP валидирует параметры DEMO/CPU_TASK до создания задач и сохраняет значения по умолчанию/границы")
    void configurableParametersAreValidatedAndPersisted() throws Exception {
        for (JsonObject request : List.of(new JsonObject().put("type", "DEMO"),
                new JsonObject().put("type", "DEMO").put("durationSeconds", 1),
                new JsonObject().put("type", "DEMO").put("durationSeconds", 300))) {
            JsonObject created = json("POST", "/api/tasks", request, 202);
            UUID id = UUID.fromString(created.getString("id"));
            int duration = request.getInteger("durationSeconds", 5);
            assertEquals(duration, created.getJsonObject("parameters").getInteger("durationSeconds"));
            assertEquals(duration, await(store.find(owner, id)).parameters().getInteger("durationSeconds"));
            json("POST", "/api/tasks/" + id + "/cancel", null, 200);
        }
        int before = await(store.list(owner)).size();
        for (Object invalid : Arrays.asList(0, -1, 301, 1.5, "5", true, null, Long.MAX_VALUE))
            json("POST", "/api/tasks", new JsonObject().put("type", "DEMO").put("durationSeconds", invalid), 400);
        for (Object invalid : Arrays.asList(1, 10_000_001, 2.5, "100", false, null, Long.MAX_VALUE))
            json("POST", "/api/tasks", new JsonObject().put("type", "CPU_TASK").put("limit", invalid), 400);
        json("POST", "/api/tasks", new JsonObject().put("type", "CPU_TASK"), 400);
        json("POST", "/api/tasks", new JsonObject().put("type", "DEMO").put("limit", 100), 400);
        assertEquals(before, await(store.list(owner)).size());
    }

    @Test
    @DisplayName("Настроенный DEMO длится одну секунду и доставляет завершение через SSE")
    void oneSecondDemoCompletesWithPersistedParametersAndSse() throws Exception {
        long start = System.nanoTime();
        UUID id = UUID.fromString(json("POST", "/api/tasks", new JsonObject().put("type", "DEMO").put("durationSeconds", 1), 202).getString("id"));
        try (SseClient sse = subscribe(id)) {
            Event completed = sse.expect("completed", "COMPLETED", 100);
            assertEquals(1, completed.data().getJsonObject("parameters").getInteger("durationSeconds"));
            sse.expectEnd();
        }
        assertTrue(System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(1));
        assertEquals(Task.Status.COMPLETED, await(store.find(owner, id)).status());
        awaitResources(new Resources(0, 0, 0, 0));
    }

    @Test
    @DisplayName("CPU_TASK выполняет вычисления, сохраняет прогресс/параметры в PostgreSQL и завершает SSE")
    void cpuTaskCompletesThroughRealHttpDatabaseAndSse() throws Exception {
        UUID id = UUID.fromString(json("POST", "/api/tasks", new JsonObject().put("type", "CPU_TASK").put("limit", 1_000_000), 202).getString("id"));
        try (SseClient sse = subscribe(id)) {
            Event completed = sse.expect("completed", "COMPLETED", 100);
            assertEquals(1_000_000, completed.data().getJsonObject("parameters").getInteger("limit"));
            assertNull(completed.data().getString("error"));
            sse.expectEnd();
        }
        Task saved = await(store.find(owner, id));
        assertEquals(Task.Status.COMPLETED, saved.status());
        assertEquals(100, saved.progress());
        assertNotNull(saved.finishedAt());
        assertEquals(1_000_000, saved.parameters().getInteger("limit"));
        awaitResources(new Resources(0, 0, 0, 0));
    }

    @Test
    @DisplayName("Две CPU_TASK активны одновременно; HTTP доступен, отмена действительно останавливает worker и сообщает SSE")
    void httpStaysResponsiveAndCpuCancellationIsDelivered() throws Exception {
        onContext(() -> {
            cpuCheckpointGate = Promise.promise();
            return io.vertx.core.Future.succeededFuture();
        });
        UUID first = UUID.fromString(json("POST", "/api/tasks", new JsonObject().put("type", "CPU_TASK").put("limit", CpuTaskHandler.MAX_LIMIT), 202).getString("id"));
        UUID second = UUID.fromString(json("POST", "/api/tasks", new JsonObject().put("type", "CPU_TASK").put("limit", CpuTaskHandler.MAX_LIMIT), 202).getString("id"));
        awaitCondition(() -> await(store.find(owner, first)).progress() == 5 && await(store.find(owner, second)).progress() == 5);
        try (SseClient a = subscribe(first); SseClient b = subscribe(second)) {
            assertEvent(a.next(), "progress", "RUNNING", 5);
            assertEvent(b.next(), "progress", "RUNNING", 5);
            long start = System.nanoTime();
            HttpResponse<String> list = client.send(HttpRequest.newBuilder(URI.create(base + "/api/tasks"))
                    .header("Authorization", "Bearer " + token).timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, list.statusCode());
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
            assertEquals("sse_user", json("GET", "/api/auth/me", null, 200).getString("login"));
            assertEquals("CANCELLED", json("POST", "/api/tasks/" + first + "/cancel", null, 200).getString("status"));
            assertEquals("CANCELLED", json("POST", "/api/tasks/" + second + "/cancel", null, 200).getString("status"));
            a.expect("cancelled", "CANCELLED", 5);
            a.expectEnd();
            b.expect("cancelled", "CANCELLED", 5);
            b.expectEnd();
        }
        onContext(() -> {
            cpuCheckpointGate.complete();
            return io.vertx.core.Future.succeededFuture();
        });
        assertEquals(Task.Status.CANCELLED, await(store.find(owner, first)).status());
        assertEquals(Task.Status.CANCELLED, await(store.find(owner, second)).status());
        awaitResources(new Resources(0, 0, 0, 0));
    }

    private CompletableFuture<HttpResponse<String>> requestCancellation(UUID id) {
        return client.sendAsync(HttpRequest.newBuilder(URI.create(base + "/api/tasks/" + id + "/cancel"))
                .header("Authorization", "Bearer " + token).timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }

    private UUID createTask() throws Exception {
        UUID id = UUID.fromString(json("POST", "/api/tasks", new JsonObject().put("type", "CONTROLLED"), 202).getString("id"));
        await(handler.started.future());
        return id;
    }

    private void advance(int progress) throws Exception {
        onContext(() -> handler.progress.apply(progress));
    }

    private void finish(UUID id) throws Exception {
        onContext(() -> {
            handler.result.complete();
            return io.vertx.core.Future.succeededFuture();
        });
        awaitCondition(() -> await(store.find(owner, id)).status() == Task.Status.COMPLETED);
    }

    private JsonObject json(String method, String path, JsonObject body, int status) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5));
        if (token != null) request.header("Authorization", "Bearer " + token);
        if (body != null) request.header("Content-Type", "application/json");
        HttpResponse<String> response = client.send(request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body.encode())).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(status, response.statusCode(), response.body());
        return status == 204 ? null : new JsonObject(response.body());
    }

    private SseClient subscribe(UUID id) throws Exception {
        HttpResponse<InputStream> response = client.send(HttpRequest.newBuilder(URI.create(base + "/api/tasks/" + id + "/events"))
                .header("Authorization", "Bearer " + token).timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/event-stream"));
        SseClient sse = new SseClient(response.body());
        subscriptions.add(sse);
        return sse;
    }

    private record Resources(int tasks, int subscribers, int snapshots, int timers) {
    }

    private void awaitResources(Resources expected) throws Exception {
        awaitCondition(() -> expected.equals(onContext(() -> {
            // Только тестовый доступ: production API не расширяется ради внутренних счётчиков.
            Map<?, ?> clients = field("clients");
            int subscribers = clients.values().stream().mapToInt(value -> ((Set<?>) value).size()).sum();
            assertEquals(subscribers, field("cleanups").size(), "Обработчики очистки не должны удерживать закрытые ответы");
            return io.vertx.core.Future.succeededFuture(new Resources(clients.size(), subscribers, field("lastSent").size(), heartbeatTimers.size()));
        })));
    }

    private Map<?, ?> field(String name) {
        try {
            Field field = SseNotifications.class.getDeclaredField(name);
            field.setAccessible(true);
            return (Map<?, ?>) field.get(notifications);
        } catch (ReflectiveOperationException error) {
            throw new AssertionError("Не удалось проверить ресурсы SSE", error);
        }
    }

    private <T> T onContext(Supplier<io.vertx.core.Future<T>> action) throws Exception {
        Promise<T> result = Promise.promise();
        context.runOnContext(v -> {
            try {
                action.get().onComplete(result);
            } catch (Throwable error) {
                result.fail(error);
            }
        });
        return await(result.future());
    }

    private static void awaitCondition(Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.call()) {
            if (System.nanoTime() > deadline) fail("Условие не выполнено за 5 секунд");
            // Опрос условия только в потоке JUnit; прогресс задачи никогда не зависит от sleep.
            Thread.sleep(10);
        }
    }

    private static <T> T await(io.vertx.core.Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static final class ControlledHandler implements TaskHandler {
        final Promise<Void> started = Promise.promise();
        final Promise<Void> result = Promise.promise();
        IntFunction<io.vertx.core.Future<Void>> progress;
        CancellationToken cancellation;

        public String type() {
            return "CONTROLLED";
        }

        public io.vertx.core.Future<Void> execute(Task task, IntFunction<io.vertx.core.Future<Void>> progress, CancellationToken cancellation) {
            this.progress = progress;
            this.cancellation = cancellation;
            started.complete();
            return result.future();
        }
    }

    private record Event(String name, JsonObject data) {
    }

    private static void assertEvent(Event event, String name, String status, int progress) {
        assertEquals(name, event.name());
        assertNotNull(event.data());
        assertEquals(status, event.data().getString("status"));
        assertEquals(progress, event.data().getInteger("progress"));
    }

    /**
     * Поток читается вне Event Loop. Ожидания ограничены, heartbeat-комментарии игнорируются.
     */
    private static final class SseClient implements AutoCloseable {
        private static final Event EOF = new Event("EOF", null);
        private final InputStream input;
        private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
        private final Thread reader;
        private volatile boolean closed;
        private volatile Throwable error;
        private int lastProgress = -1;

        SseClient(InputStream input) {
            this.input = input;
            reader = Thread.startVirtualThread(() -> {
                try (BufferedReader lines = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                    String name = null;
                    String data = null;
                    String line;
                    while ((line = lines.readLine()) != null) {
                        if (line.startsWith("event: ")) name = line.substring(7);
                        else if (line.startsWith("data: ")) data = line.substring(6);
                        else if (line.isEmpty()) {
                            if (data != null) events.add(new Event(name, new JsonObject(data)));
                            name = null;
                            data = null;
                        }
                    }
                } catch (Throwable failure) {
                    if (!closed) error = failure;
                } finally {
                    events.add(EOF);
                }
            });
        }

        Event next() throws Exception {
            Event event = events.poll(5, TimeUnit.SECONDS);
            assertNotNull(event, "SSE-событие не пришло за 5 секунд");
            assertNull(error, "Ошибка чтения SSE: " + error);
            if (event != EOF && event.data().containsKey("progress")) {
                int progress = event.data().getInteger("progress");
                assertTrue(progress >= lastProgress, "Прогресс не должен уменьшаться");
                lastProgress = progress;
            }
            return event;
        }

        Event expect(String name, String status, int progress) throws Exception {
            for (int i = 0; i < 30; i++) {
                Event event = next();
                assertNotSame(EOF, event, "Соединение завершилось до ожидаемого события");
                if (event.name().equals(name) && event.data().getString("status").equals(status)
                        && event.data().getInteger("progress") == progress) return event;
                // Повторный снимок после подключения допустим, но будущие шаги терять нельзя.
                assertTrue(event.data().getInteger("progress") <= progress);
            }
            throw new AssertionError("Слишком много событий до ожидаемого состояния");
        }

        void expectEnd() throws Exception {
            assertSame(EOF, next(), "После финального события ожидается EOF");
        }

        void expectDeleted(UUID id) throws Exception {
            for (int i = 0; i < 10; i++) {
                Event event = next();
                assertNotSame(EOF, event);
                if (event.name().equals("deleted")) {
                    assertEquals(id.toString(), event.data().getString("id"));
                    return;
                }
                assertEquals("progress", event.name());
            }
            fail("Не пришло событие deleted");
        }

        public void close() throws Exception {
            if (closed) return;
            closed = true;
            input.close();
            reader.join(2000);
            assertFalse(reader.isAlive(), "Поток чтения SSE не завершился");
        }
    }
}
