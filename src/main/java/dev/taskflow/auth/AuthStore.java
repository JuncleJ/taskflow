package dev.taskflow.auth;

import io.vertx.core.Future;

import java.util.UUID;

public interface AuthStore {
    record User(UUID id, String hash) {
    }

    record Profile(UUID id, String login) {
    }

    Future<Void> register(UUID id, String login, String hash);

    Future<User> user(String login);

    Future<Profile> profile(UUID id);

    Future<Void> session(String tokenHash, UUID user);

    Future<UUID> authenticate(String tokenHash);
}
