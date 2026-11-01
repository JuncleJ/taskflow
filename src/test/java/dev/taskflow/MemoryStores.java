package dev.taskflow;

import dev.taskflow.auth.AuthStore;
import dev.taskflow.task.*;
import dev.taskflow.http.ApiException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.time.OffsetDateTime;
import java.util.*;

final class MemoryStores {
    static class Tasks implements TaskStore {
        final Map<UUID, Task> tasks = new LinkedHashMap<>();

        public Future<Task> create(UUID owner, String type, JsonObject parameters) {
            Task task = new Task(UUID.randomUUID(), owner, type, Task.Status.PENDING, 0, OffsetDateTime.now(), null, null, null, parameters);
            tasks.put(task.id(), task);
            return Future.succeededFuture(task);
        }

        public Future<Task> find(UUID owner, UUID id) {
            Task task = tasks.get(id);
            return Future.succeededFuture(task != null && task.ownerId().equals(owner) ? task : null);
        }

        public Future<List<Task>> list(UUID owner) {
            return Future.succeededFuture(tasks.values().stream().filter(t -> t.ownerId().equals(owner)).toList());
        }

        public Future<Task> update(UUID id, Task.Status status, int progress, String error) {
            Task old = tasks.get(id);
            if (old == null || old.terminal() || status == Task.Status.PENDING
                    || old.status() == Task.Status.PENDING && status == Task.Status.COMPLETED
                    || progress < old.progress() || progress > 100 || status == Task.Status.COMPLETED && progress != 100)
                return Future.failedFuture("Недопустимый переход задачи");
            Task task = new Task(id, old.ownerId(), old.type(), status, progress, old.createdAt(),
                    old.startedAt() == null && status == Task.Status.RUNNING ? OffsetDateTime.now() : old.startedAt(),
                    status == Task.Status.COMPLETED || status == Task.Status.FAILED || status == Task.Status.CANCELLED ? OffsetDateTime.now() : null, error, old.parameters());
            tasks.put(id, task);
            return Future.succeededFuture(task);
        }

        public Future<Boolean> delete(UUID owner, UUID id) {
            Task task = tasks.get(id);
            if (task == null || !task.ownerId().equals(owner) || !task.terminal()) return Future.succeededFuture(false);
            tasks.remove(id);
            return Future.succeededFuture(true);
        }
    }

    static final class Auth implements AuthStore {
        final Map<String, User> users = new HashMap<>();
        final Map<String, UUID> sessions = new HashMap<>();

        public Future<Void> register(UUID id, String login, String hash) {
            if (users.containsKey(login)) return Future.failedFuture(new ApiException(409, "Логин уже занят"));
            users.put(login, new User(id, hash));
            return Future.succeededFuture();
        }

        public Future<User> user(String login) {
            return Future.succeededFuture(users.get(login));
        }

        public Future<Profile> profile(UUID id) {
            return Future.succeededFuture(users.entrySet().stream()
                    .filter(entry -> entry.getValue().id().equals(id))
                    .map(entry -> new Profile(id, entry.getKey())).findFirst().orElse(null));
        }

        public Future<Void> session(String hash, UUID user) {
            sessions.put(hash, user);
            return Future.succeededFuture();
        }

        public Future<UUID> authenticate(String hash) {
            return Future.succeededFuture(sessions.get(hash));
        }
    }
}
