package dev.taskflow.task;

import io.vertx.core.json.JsonObject;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Task(UUID id, UUID ownerId, String type, Status status, int progress,
                   OffsetDateTime createdAt, OffsetDateTime startedAt, OffsetDateTime finishedAt, String error,
                   JsonObject parameters) {
    public Task {
        parameters = parameters.copy();
    }

    public Task(UUID id, UUID ownerId, String type, Status status, int progress,
                OffsetDateTime createdAt, OffsetDateTime startedAt, OffsetDateTime finishedAt, String error) {
        this(id, ownerId, type, status, progress, createdAt, startedAt, finishedAt, error, new JsonObject());
    }

    @Override
    public JsonObject parameters() {
        return parameters.copy();
    }

    public enum Status {PENDING, RUNNING, COMPLETED, FAILED, CANCELLED}

    public boolean terminal() {
        return status == Status.COMPLETED || status == Status.FAILED || status == Status.CANCELLED;
    }

    public JsonObject json() {
        return new JsonObject().put("id", id.toString()).put("ownerId", ownerId.toString())
                .put("type", type).put("status", status.name()).put("progress", progress)
                .put("createdAt", createdAt.toString()).put("startedAt", startedAt == null ? null : startedAt.toString())
                .put("finishedAt", finishedAt == null ? null : finishedAt.toString()).put("error", error)
                .put("parameters", parameters.copy());
    }
}
