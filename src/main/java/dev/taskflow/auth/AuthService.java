package dev.taskflow.auth;

import dev.taskflow.http.ApiException;
import io.vertx.core.*;
import java.util.UUID;

public final class AuthService {
    private final Vertx vertx;
    private final AuthStore store;
    private final PasswordHasher hasher = new PasswordHasher();
    private final String dummyHash;
    public AuthService(Vertx vertx, AuthStore store, String dummyHash) {
        this.vertx = vertx; this.store = store; this.dummyHash = dummyHash;
    }
    public Future<String> register(String login, String password) {
        validate(login, password);
        UUID id = UUID.randomUUID();
        return vertx.executeBlocking(() -> hasher.hash(password), false)
            .compose(hash -> store.register(id, login, hash)).compose(v -> issue(id));
    }
    public Future<String> login(String login, String password) {
        validate(login, password);
        return store.user(login).compose(user -> vertx.executeBlocking(
            () -> hasher.verify(password, user == null ? dummyHash : user.hash()), false)
            .compose(valid -> valid && user != null ? issue(user.id()) : Future.failedFuture(new ApiException(401, "Неверный логин или пароль"))));
    }
    public Future<UUID> authenticate(String token) {
        if (token == null || !token.matches("[A-Za-z0-9_-]{43}")) return Future.failedFuture(new ApiException(401, "Требуется токен"));
        return store.authenticate(PasswordHasher.tokenHash(token)).compose(id -> id == null
            ? Future.failedFuture(new ApiException(401, "Токен истёк или недействителен")) : Future.succeededFuture(id));
    }
    private Future<String> issue(UUID user) {
        String token = PasswordHasher.newToken();
        return store.session(PasswordHasher.tokenHash(token), user).map(token);
    }
    private void validate(String login, String password) {
        if (login == null || !login.matches("[A-Za-z0-9_]{3,64}") || password == null || password.length() < 8 || password.length() > 128)
            throw new ApiException(400, "Логин: 3–64 латинских букв, цифр или _. Пароль: 8–128 символов");
    }
}
