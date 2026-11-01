package dev.taskflow.auth;

import dev.taskflow.http.ApiException;
import io.vertx.core.*;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AuthService {
    private static final Logger LOG = LoggerFactory.getLogger(AuthService.class);
    private final Vertx vertx;
    private final AuthStore store;
    private final PasswordHasher hasher = new PasswordHasher();
    private final String dummyHash;

    public AuthService(Vertx vertx, AuthStore store, String dummyHash) {
        this.vertx = vertx;
        this.store = store;
        this.dummyHash = dummyHash;
    }

    public Future<String> register(String login, String password) {
        validate("register", login, password);
        UUID id = UUID.randomUUID();
        return vertx.executeBlocking(() -> hasher.hash(password), false)
                .compose(hash -> store.register(id, login, hash)).compose(v -> issue(id))
                .onSuccess(token -> LOG.info("Регистрация успешна operation=register userId={}", id))
                .onFailure(error -> logFailure("register", error));
    }

    public Future<String> login(String login, String password) {
        validate("login", login, password);
        return store.user(login).compose(user -> vertx.executeBlocking(
                                () -> hasher.verify(password, user == null ? dummyHash : user.hash()), false)
                        .compose(valid -> valid && user != null
                                ? issue(user.id()).onSuccess(token -> LOG.info("Вход выполнен operation=login userId={}", user.id()))
                                : Future.failedFuture(new ApiException(401, "Неверный логин или пароль"))))
                .onFailure(error -> logFailure("login", error));
    }

    public Future<UUID> authenticate(String token) {
        if (token == null || !token.matches("[A-Za-z0-9_-]{43}")) {
            LOG.warn("Проверка токена отклонена operation=authenticate reason={}", token == null ? "missing_token" : "invalid_token_format");
            return Future.failedFuture(new ApiException(401, "Требуется токен авторизации"));
        }
        return store.authenticate(PasswordHasher.tokenHash(token)).compose(id -> id == null
                        ? Future.failedFuture(new ApiException(401, "Токен истёк или недействителен")) : Future.succeededFuture(id))
                .onSuccess(id -> LOG.debug("Токен проверен operation=authenticate userId={}", id))
                .onFailure(error -> logFailure("authenticate", error));
    }

    public Future<AuthStore.Profile> currentUser(String token) {
        return authenticate(token).compose(store::profile).compose(profile -> profile == null
                ? Future.failedFuture(new ApiException(401, "Пользователь сессии не найден"))
                : Future.succeededFuture(profile));
    }

    private Future<String> issue(UUID user) {
        String token = PasswordHasher.newToken();
        return store.session(PasswordHasher.tokenHash(token), user).map(token);
    }

    private void validate(String operation, String login, String password) {
        if (login == null || !login.matches("[A-Za-z0-9_]{3,64}") || password == null || password.length() < 8 || password.length() > 128) {
            LOG.warn("Операция аутентификации отклонена operation={} reason=validation_failed status=400", operation);
            throw new ApiException(400, "Логин: 3–64 латинских букв, цифр или _. Пароль: 8–128 символов");
        }
    }

    private void logFailure(String operation, Throwable error) {
        if (error instanceof ApiException api && api.status() < 500) {
            String reason = switch (api.status()) {
                case 401 -> operation.equals("login") ? "invalid_credentials" : "invalid_session";
                case 409 -> "login_taken";
                default -> "request_rejected";
            };
            LOG.warn("Операция аутентификации отклонена operation={} reason={} status={}", operation, reason, api.status());
        } else {
            // Сообщение исключения может содержать параметры БД или другие секреты.
            LOG.error("Сбой аутентификации operation={} reason=internal_error errorType={}", operation, error.getClass().getName());
        }
    }
}
