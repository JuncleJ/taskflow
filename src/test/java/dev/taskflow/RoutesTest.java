package dev.taskflow;

import dev.taskflow.auth.*;
import dev.taskflow.http.*;
import dev.taskflow.task.*;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.*;
import org.junit.jupiter.api.*;

import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static dev.taskflow.TaskExecutorTest.await;
import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

class RoutesTest {
    private Vertx vertx;
    private HttpServer server;
    private String base;
    private Logger appLogger;
    private Level previousLevel;
    private final List<ILoggingEvent> logEvents = new CopyOnWriteArrayList<>();
    private AppenderBase<ILoggingEvent> capture;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @BeforeEach
    void start() throws Exception {
        appLogger = (Logger) LoggerFactory.getLogger("dev.taskflow");
        previousLevel = appLogger.getLevel();
        appLogger.setLevel(Level.INFO);
        capture = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                event.prepareForDeferredProcessing();
                logEvents.add(event);
            }
        };
        capture.start();
        appLogger.addAppender(capture);
        vertx = Vertx.vertx();
        String dummy = new PasswordHasher().hash("dummy-password");
        Promise<HttpServer> ready = Promise.promise();
        vertx.runOnContext(v -> {
            SseNotifications sse = new SseNotifications(vertx);
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            TaskHandler failing = new TaskHandler() {
                public String type() {
                    return "FAIL";
                }

                public io.vertx.core.Future<Void> execute(Task task, java.util.function.IntFunction<io.vertx.core.Future<Void>> p, CancellationToken cancellation) {
                    return io.vertx.core.Future.failedFuture("sensitive-error-marker");
                }
            };
            TaskHandlerRegistry registry = new TaskHandlerRegistry(new TimedTaskHandler(vertx, "DEMO", 4, 30), new TimedTaskHandler(vertx, "REPORT", 4, 30), failing);
            TaskExecutor executor = new TaskExecutor(store, registry, sse::publish, 2);
            vertx.createHttpServer().requestHandler(Routes.create(vertx, new AuthService(vertx, new MemoryStores.Auth(), dummy),
                    new TaskService(store, registry, executor, sse::deleted), sse)).listen(0).onComplete(ready);
        });
        server = await(ready.future());
        base = "http://localhost:" + server.actualPort();
    }

    @AfterEach
    void close() throws Exception {
        try {
            await(vertx.close());
        } finally {
            appLogger.detachAppender(capture);
            capture.stop();
            appLogger.setLevel(previousLevel);
        }
    }

    private HttpResponse<String> request(String method, String path, String token, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (body != null) builder.header("Content-Type", "application/json");
        return client.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private String register(String login) throws Exception {
        HttpResponse<String> response = request("POST", "/api/auth/register", null, "{\"login\":\"" + login + "\",\"password\":\"password123\"}");
        assertEquals(201, response.statusCode(), response.body());
        return new JsonObject(response.body()).getString("token");
    }

    @Test
    void registrationLoginValidationAndStaticPage() throws Exception {
        assertEquals(200, request("GET", "/", null, null).statusCode());
        assertEquals(401, request("GET", "/api/tasks", null, null).statusCode());
        register("alice");
        assertEquals(409, request("POST", "/api/auth/register", null, "{\"login\":\"alice\",\"password\":\"password123\"}").statusCode());
        assertEquals(200, request("POST", "/api/auth/login", null, "{\"login\":\"alice\",\"password\":\"password123\"}").statusCode());
        assertEquals(401, request("POST", "/api/auth/login", null, "{\"login\":\"alice\",\"password\":\"wrongpass\"}").statusCode());
        assertEquals(400, request("POST", "/api/auth/register", null, "{\"login\":\"x\",\"password\":\"short\"}").statusCode());
        assertEquals(400, request("POST", "/api/auth/login", null, "{").statusCode());
        assertEquals(400, request("POST", "/api/auth/login", null, "{\"login\":1,\"password\":true}").statusCode());
    }

    @Test
    void endpointsIsolateOwnersIncludingSseAndReportCompletion() throws Exception {
        String alice = register("alice"), bob = register("bob");
        HttpResponse<String> created = request("POST", "/api/tasks", alice, "{\"type\":\"REPORT\"}");
        assertEquals(202, created.statusCode());
        String path = "/api/tasks/" + new JsonObject(created.body()).getString("id");
        assertEquals(404, request("GET", path, bob, null).statusCode());
        assertEquals(404, request("GET", path + "/events", bob, null).statusCode());
        assertEquals(0, new JsonArray(request("GET", "/api/tasks", bob, null).body()).size());
        assertEquals(1, new JsonArray(request("GET", "/api/tasks", alice, null).body()).size());
        HttpResponse<String> events = request("GET", path + "/events", alice, null);
        assertEquals(200, events.statusCode());
        assertTrue(events.body().contains("event: completed"));
        assertTrue(events.body().contains("\"progress\":100"));
        assertEquals("COMPLETED", new JsonObject(request("GET", path, alice, null).body()).getString("status"));
        assertEquals(400, request("GET", "/api/tasks/not-a-uuid", alice, null).statusCode());
        assertEquals(400, request("POST", "/api/tasks", alice, "{\"type\":\"UNKNOWN\"}").statusCode());
    }

    @Test
    void sseReturnsCurrentTerminalFailure() throws Exception {
        String token = register("alice");
        String id = new JsonObject(request("POST", "/api/tasks", token, "{\"type\":\"FAIL\"}").body()).getString("id");
        HttpResponse<String> response = request("GET", "/api/tasks/" + id + "/events", token, null);
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("event: failed"));
        assertTrue(response.body().contains("\"error\":\"Ошибка выполнения задачи\""));
    }

    @Test
    void loggingCapturesLifecycleAndHttpWithoutSecrets() throws Exception {
        String token = register("alice");
        String id = new JsonObject(request("POST", "/api/tasks", token, "{\"type\":\"DEMO\"}").body()).getString("id");
        request("GET", "/api/tasks/" + id + "/events?token=query-secret-marker", token, null);
        String failedId = new JsonObject(request("POST", "/api/tasks", token, "{\"type\":\"FAIL\"}").body()).getString("id");
        request("GET", "/api/tasks/" + failedId + "/events", token, null);
        request("GET", "/api/tasks/path-secret-marker", token, null);
        request("GET", "/unknown-secret-marker?token=query-secret-marker", null, null);
        // Барьер после обработки HTTP-ответов: обработчики завершения также успевают записать access log.
        Promise<Void> barrier = Promise.promise();
        vertx.setTimer(50, timer -> barrier.complete());
        await(barrier.future());
        List<String> messages = logEvents.stream().map(ILoggingEvent::getFormattedMessage).toList();
        String logs = String.join("\n", messages);
        for (String secret : List.of(token, "password123", "query-secret-marker", "path-secret-marker", "unknown-secret-marker", "sensitive-error-marker"))
            assertFalse(logs.contains(secret), "В журнал попали чувствительные данные");
        for (String event : List.of("Задача создана", "Задача запущена", "Задача завершена")) {
            assertTrue(messages.stream().anyMatch(m -> m.startsWith(event) && m.contains("taskId=" + id) && m.contains("userId=") && m.contains("taskType=DEMO")), event);
        }
        assertTrue(logEvents.stream().anyMatch(e -> e.getLevel() == Level.ERROR && e.getFormattedMessage().contains("taskId=" + failedId)
                && e.getFormattedMessage().contains("errorType=") && e.getFormattedMessage().contains("stack=")));
        assertEquals(2, messages.stream().filter(m -> m.contains("taskId=" + id) && !m.startsWith("Задача создана")).count(), "Не должно быть INFO на каждый шаг прогресса");
        assertTrue(messages.stream().anyMatch(m -> m.matches("HTTP method=GET route=/api/tasks/:id/events status=200 durationMs=\\d+")));
        assertEquals(2, messages.stream().filter(m -> m.startsWith("HTTP method=GET route=/api/tasks/:id/events status=200")).count());
        assertTrue(messages.stream().anyMatch(m -> m.contains("route=/api/tasks/:id status=400")));
        assertTrue(messages.stream().anyMatch(m -> m.contains("route=<unmatched> status=404")));
    }

    @Test
    void disconnectedSseRequestIsLoggedOnce() throws Exception {
        String token = register("alice");
        String id = new JsonObject(request("POST", "/api/tasks", token, "{\"type\":\"DEMO\"}").body()).getString("id");
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/api/tasks/" + id + "/events"))
                .header("Authorization", "Bearer " + token).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<java.io.InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode());
        response.body().close();
        Promise<Void> barrier = Promise.promise();
        vertx.setTimer(250, timer -> barrier.complete());
        await(barrier.future());
        assertEquals(1, logEvents.stream().map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("HTTP method=GET route=/api/tasks/:id/events status=200 durationMs=")).count());
    }

    @Test
    void authenticationOperationsAreLoggedWithoutCredentials() throws Exception {
        appLogger.setLevel(Level.DEBUG);
        String token = register("private_login");
        assertEquals(200, request("POST", "/api/auth/login", null, "{\"login\":\"private_login\",\"password\":\"password123\"}").statusCode());
        assertEquals(401, request("POST", "/api/auth/login", null, "{\"login\":\"private_login\",\"password\":\"wrong-password\"}").statusCode());
        assertEquals(401, request("POST", "/api/auth/login", null, "{\"login\":\"unknown_login\",\"password\":\"wrong-password\"}").statusCode());
        assertEquals(409, request("POST", "/api/auth/register", null, "{\"login\":\"private_login\",\"password\":\"password123\"}").statusCode());
        assertEquals(400, request("POST", "/api/auth/register", null, "{\"login\":\"x\",\"password\":\"short\"}").statusCode());
        assertEquals(200, request("GET", "/api/tasks", token, null).statusCode());
        assertEquals(401, request("GET", "/api/tasks", null, null).statusCode());
        assertEquals(401, request("GET", "/api/tasks", "invalid-token-marker", null).statusCode());
        String unknownToken = PasswordHasher.newToken();
        assertEquals(401, request("GET", "/api/tasks", unknownToken, null).statusCode());
        List<ILoggingEvent> events = logEvents.stream().filter(e -> e.getLoggerName().equals(AuthService.class.getName())).toList();
        assertEquals(1, events.stream().filter(e -> e.getLevel() == Level.INFO && e.getFormattedMessage().contains("operation=register userId=")).count());
        assertEquals(1, events.stream().filter(e -> e.getLevel() == Level.INFO && e.getFormattedMessage().contains("operation=login userId=")).count());
        assertEquals(2, events.stream().filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("reason=invalid_credentials")).count());
        for (String reason : List.of("login_taken", "validation_failed", "missing_token", "invalid_token_format", "invalid_session")) {
            assertTrue(events.stream().anyMatch(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("reason=" + reason)), reason);
        }
        assertTrue(events.stream().anyMatch(e -> e.getLevel() == Level.DEBUG && e.getFormattedMessage().contains("operation=authenticate userId=")));
        String logs = String.join("\n", logEvents.stream().map(ILoggingEvent::getFormattedMessage).toList());
        for (String secret : List.of(token, unknownToken, PasswordHasher.tokenHash(token), "private_login", "unknown_login", "password123", "wrong-password", "invalid-token-marker")) {
            assertFalse(logs.contains(secret), "В журнал попали учётные данные");
        }
        assertTrue(events.stream().allMatch(e -> e.getThrowableProxy() == null));
    }

    @Test
    void authenticationInfrastructureFailureIsLoggedAsErrorWithoutMessage() throws Exception {
        AuthStore failingStore = new AuthStore() {
            public io.vertx.core.Future<Void> register(UUID id, String login, String hash) {
                return io.vertx.core.Future.failedFuture("db-secret-marker");
            }

            public io.vertx.core.Future<User> user(String login) {
                return io.vertx.core.Future.failedFuture("db-secret-marker");
            }

            public io.vertx.core.Future<Profile> profile(UUID id) {
                return io.vertx.core.Future.failedFuture("db-secret-marker");
            }

            public io.vertx.core.Future<Void> session(String hash, UUID user) {
                return io.vertx.core.Future.failedFuture("db-secret-marker");
            }

            public io.vertx.core.Future<UUID> authenticate(String hash) {
                return io.vertx.core.Future.failedFuture("db-secret-marker");
            }
        };
        AuthService service = new AuthService(vertx, failingStore, "unused");
        assertThrows(Exception.class, () -> await(service.login("private_login", "password123")));
        assertTrue(logEvents.stream().anyMatch(e -> e.getLevel() == Level.ERROR && e.getFormattedMessage().contains("operation=login reason=internal_error errorType=")));
        assertFalse(logEvents.stream().anyMatch(e -> e.getFormattedMessage().contains("db-secret-marker") || e.getThrowableProxy() != null));
    }

    @Test
    void currentUserRequiresValidSessionAndReturnsOnlyItsPublicProfile() throws Exception {
        assertEquals(401, request("GET", "/api/auth/me", null, null).statusCode());
        assertEquals(401, request("GET", "/api/auth/me", "invalid-token", null).statusCode());
        assertEquals(401, request("GET", "/api/auth/me", PasswordHasher.newToken(), null).statusCode());
        String alice = register("alice"), bob = register("bob");
        HttpResponse<String> response = request("GET", "/api/auth/me?login=bob", alice, null);
        assertEquals(200, response.statusCode());
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow());
        JsonObject profile = new JsonObject(response.body());
        assertEquals(Set.of("id", "login"), profile.fieldNames());
        assertEquals("alice", profile.getString("login"));
        UUID.fromString(profile.getString("id"));
        assertEquals(profile, new JsonObject(request("GET", "/api/auth/me", alice, null).body()));
        JsonObject otherProfile = new JsonObject(request("GET", "/api/auth/me", bob, null).body());
        assertEquals("bob", otherProfile.getString("login"));
        assertNotEquals(profile.getString("id"), otherProfile.getString("id"));
    }

}
