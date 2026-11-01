package dev.taskflow.http;

import dev.taskflow.auth.AuthService;
import dev.taskflow.task.*;
import io.vertx.core.*;
import io.vertx.core.json.*;
import io.vertx.ext.web.*;
import io.vertx.ext.web.handler.*;

import java.util.UUID;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Routes {
    private static final Logger LOG = LoggerFactory.getLogger(Routes.class);

    public static Router create(Vertx vertx, AuthService auth, TaskService tasks, SseNotifications sse) {
        Router router = Router.router(vertx);
        router.route().handler(HttpRequestLogging::handle);
        router.route().handler(ctx -> {
            ctx.response().putHeader("X-Content-Type-Options", "nosniff").putHeader("Referrer-Policy", "no-referrer")
                    .putHeader("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'");
            ctx.next();
        });
        router.route("/api/*").handler(BodyHandler.create().setBodyLimit(4096));
        router.post("/api/auth/register").handler(ctx -> credentials(ctx, auth, true));
        router.post("/api/auth/login").handler(ctx -> credentials(ctx, auth, false));
        router.route("/api/tasks*").handler(ctx -> {
            String header = ctx.request().getHeader("Authorization");
            auth.authenticate(header != null && header.startsWith("Bearer ") ? header.substring(7) : null)
                    .onSuccess(owner -> {
                        ctx.put("owner", owner);
                        ctx.next();
                    }).onFailure(ctx::fail);
        });
        router.post("/api/tasks").handler(ctx -> {
            JsonObject body = body(ctx);
            tasks.create(ctx.get("owner"), string(body, "type"))
                    .onSuccess(task -> {
                        ctx.response().putHeader("Location", "/api/tasks/" + task.id());
                        json(ctx, 202, task.json());
                    })
                    .onFailure(ctx::fail);
        });
        router.get("/api/tasks").handler(ctx -> tasks.list(ctx.get("owner"))
                .onSuccess(list -> json(ctx, 200, new JsonArray(list.stream().map(Task::json).toList()))).onFailure(ctx::fail));
        router.get("/api/tasks/:id").handler(ctx -> find(ctx, tasks).onSuccess(task -> json(ctx, 200, task.json())).onFailure(ctx::fail));
        router.get("/api/tasks/:id/events").handler(ctx -> find(ctx, tasks).onSuccess(task -> {
            // Первая подписка создаётся в том же context, что и обновления движка.
            sse.subscribe(task, ctx);
            // Повторное чтение закрывает окно между SELECT и добавлением подписчика.
            tasks.find(ctx.get("owner"), task.id()).onSuccess(sse::publish).onFailure(e -> ctx.response().end());
        }).onFailure(ctx::fail));
        router.route("/api/*").handler(ctx -> ctx.fail(new ApiException(404, "Маршрут не найден")));
        router.route().handler(StaticHandler.create("webroot").setCachingEnabled(false));
        router.route().failureHandler(ctx -> {
            Throwable error = ctx.failure();
            int code = error instanceof ApiException api ? api.status() : error instanceof IllegalArgumentException || error instanceof DecodeException ? 400 : ctx.statusCode() > 0 ? ctx.statusCode() : 500;
            if (code >= 500) LOG.error("Ошибка HTTP status={} errorType={} stack={}", code,
                    error == null ? "unknown" : error.getClass().getName(),
                    error == null ? "[]" : Arrays.toString(error.getStackTrace()));
            if (!ctx.response().headWritten())
                json(ctx, code, new JsonObject().put("error", code >= 500 ? "Внутренняя ошибка сервера" : error == null ? "Ошибка запроса" : error.getMessage()));
            else ctx.response().end();
        });
        return router;
    }

    private static JsonObject body(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null) throw new ApiException(400, "Ожидается JSON объект");
        return body;
    }

    private static void credentials(RoutingContext ctx, AuthService auth, boolean register) {
        JsonObject body = body(ctx);
        Future<String> result = register ? auth.register(string(body, "login"), string(body, "password")) : auth.login(string(body, "login"), string(body, "password"));
        result.onSuccess(token -> json(ctx, register ? 201 : 200, new JsonObject().put("token", token).put("expiresIn", 86400))).onFailure(ctx::fail);
    }

    private static Future<Task> find(RoutingContext ctx, TaskService tasks) {
        UUID id = UUID.fromString(ctx.pathParam("id"));
        return tasks.find(ctx.get("owner"), id).compose(task -> task == null ? Future.failedFuture(new ApiException(404, "Задача не найдена")) : Future.succeededFuture(task));
    }

    private static String string(JsonObject body, String name) {
        Object value = body.getValue(name);
        if (!(value instanceof String text)) throw new ApiException(400, "Поле " + name + " должно быть строкой");
        return text;
    }

    private static void json(RoutingContext ctx, int status, Object value) {
        ctx.response().setStatusCode(status).putHeader("Content-Type", "application/json; charset=utf-8")
                .putHeader("Cache-Control", "no-store").end(value.toString());
    }
}
