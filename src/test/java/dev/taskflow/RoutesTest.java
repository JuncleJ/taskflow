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

class RoutesTest {
    private Vertx vertx;
    private HttpServer server;
    private String base;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    @BeforeEach void start() throws Exception {
        vertx = Vertx.vertx();
        String dummy = new PasswordHasher().hash("dummy-password");
        Promise<HttpServer> ready = Promise.promise();
        vertx.runOnContext(v -> {
            SseNotifications sse = new SseNotifications(vertx);
            MemoryStores.Tasks store = new MemoryStores.Tasks();
            TaskHandler failing = new TaskHandler() {
                public String type() { return "FAIL"; }
                public io.vertx.core.Future<Void> execute(java.util.function.IntFunction<io.vertx.core.Future<Void>> p) { return io.vertx.core.Future.failedFuture("Ошибка"); }
            };
            TaskHandlerRegistry registry = new TaskHandlerRegistry(new TimedTaskHandler(vertx, "DEMO", 4, 30), new TimedTaskHandler(vertx, "REPORT", 4, 30), failing);
            TaskExecutor executor = new TaskExecutor(store, registry, sse::publish, 2);
            vertx.createHttpServer().requestHandler(Routes.create(vertx, new AuthService(vertx, new MemoryStores.Auth(), dummy),
                new TaskService(store, registry, executor), sse)).listen(0).onComplete(ready);
        });
        server = await(ready.future()); base = "http://localhost:" + server.actualPort();
    }
    @AfterEach void close() throws Exception { await(vertx.close()); }
    private HttpResponse<String> request(String method, String path, String token, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (body != null) builder.header("Content-Type", "application/json");
        return client.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private String register(String login) throws Exception {
        HttpResponse<String> response = request("POST", "/api/auth/register", null, "{\"login\":\"" + login + "\",\"password\":\"password123\"}");
        assertEquals(201, response.statusCode(), response.body()); return new JsonObject(response.body()).getString("token");
    }
    @Test void registrationLoginValidationAndStaticPage() throws Exception {
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
    @Test void endpointsIsolateOwnersIncludingSseAndReportCompletion() throws Exception {
        String alice = register("alice"), bob = register("bob");
        HttpResponse<String> created = request("POST", "/api/tasks", alice, "{\"type\":\"REPORT\"}");
        assertEquals(202, created.statusCode());
        String path = "/api/tasks/" + new JsonObject(created.body()).getString("id");
        assertEquals(404, request("GET", path, bob, null).statusCode());
        assertEquals(404, request("GET", path + "/events", bob, null).statusCode());
        assertEquals(0, new JsonArray(request("GET", "/api/tasks", bob, null).body()).size());
        assertEquals(1, new JsonArray(request("GET", "/api/tasks", alice, null).body()).size());
        HttpResponse<String> events = request("GET", path + "/events", alice, null);
        assertEquals(200, events.statusCode()); assertTrue(events.body().contains("event: completed"));
        assertTrue(events.body().contains("\"progress\":100"));
        assertEquals("COMPLETED", new JsonObject(request("GET", path, alice, null).body()).getString("status"));
        assertEquals(400, request("GET", "/api/tasks/not-a-uuid", alice, null).statusCode());
        assertEquals(400, request("POST", "/api/tasks", alice, "{\"type\":\"UNKNOWN\"}").statusCode());
    }
    @Test void sseReturnsCurrentTerminalFailure() throws Exception {
        String token = register("alice");
        String id = new JsonObject(request("POST", "/api/tasks", token, "{\"type\":\"FAIL\"}").body()).getString("id");
        HttpResponse<String> response = request("GET", "/api/tasks/" + id + "/events", token, null);
        assertEquals(200, response.statusCode()); assertTrue(response.body().contains("event: failed"));
        assertTrue(response.body().contains("\"error\":\"Ошибка выполнения задачи\""));
    }
}
