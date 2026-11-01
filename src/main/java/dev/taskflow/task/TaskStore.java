package dev.taskflow.task;

import io.vertx.core.Future;
import java.util.List;
import java.util.UUID;

public interface TaskStore {
    Future<Task> create(UUID owner, String type);
    Future<Task> find(UUID owner, UUID id);
    Future<List<Task>> list(UUID owner);
    Future<Task> update(UUID id, Task.Status status, int progress, String error);
}
