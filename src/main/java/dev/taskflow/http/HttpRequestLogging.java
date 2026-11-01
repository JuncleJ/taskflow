package dev.taskflow.http;

import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Не записывает URI, параметры, заголовки, тело запроса и ответа.
 */
final class HttpRequestLogging {
    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestLogging.class);
    private static final Set<String> PUBLIC_PATHS = Set.of("/", "/index.html", "/style.css", "/app.js",
            "/api/auth/register", "/api/auth/login", "/api/auth/me", "/api/tasks");

    static void handle(RoutingContext ctx) {
        long start = System.nanoTime();
        String route = route(ctx.request().path());
        boolean[] logged = {false};
        Runnable log = () -> {
            if (logged[0]) return;
            logged[0] = true;
            LOG.info("HTTP method={} route={} status={} durationMs={}",
                    ctx.request().method().name(), route, ctx.response().getStatusCode(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        };
        // Одна запись при завершении тела либо при обрыве соединения.
        ctx.addBodyEndHandler(v -> log.run());
        ctx.addEndHandler(result -> log.run());
        ctx.next();
    }

    private static String route(String path) {
        if (PUBLIC_PATHS.contains(path)) return path;
        if (path.matches("/api/tasks/[^/]+/cancel")) return "/api/tasks/:id/cancel";
        if (path.matches("/api/tasks/[^/]+/events")) return "/api/tasks/:id/events";
        if (path.matches("/api/tasks/[^/]+")) return "/api/tasks/:id";
        return "<unmatched>";
    }
}
