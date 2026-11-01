package dev.taskflow.persistence;

import dev.taskflow.task.*;
import io.vertx.core.Future;
import io.vertx.sqlclient.*;

import java.util.*;

public final class PgTaskStore implements TaskStore {
    private final Pool pool;

    public PgTaskStore(Pool pool) {
        this.pool = pool;
    }

    public Future<Task> create(UUID owner, String type) {
        return pool.preparedQuery("INSERT INTO tasks(id,owner_id,type,status) VALUES($1,$2,$3,'PENDING') RETURNING *")
                .execute(Tuple.of(UUID.randomUUID(), owner, type)).map(rows -> map(rows.iterator().next()));
    }

    public Future<Task> find(UUID owner, UUID id) {
        return pool.preparedQuery("SELECT * FROM tasks WHERE id=$1 AND owner_id=$2")
                .execute(Tuple.of(id, owner)).map(rows -> rows.size() == 0 ? null : map(rows.iterator().next()));
    }

    public Future<List<Task>> list(UUID owner) {
        return pool.preparedQuery("SELECT * FROM tasks WHERE owner_id=$1 ORDER BY created_at DESC LIMIT 200")
                .execute(Tuple.of(owner)).map(rows -> {
                    List<Task> result = new ArrayList<>();
                    rows.forEach(row -> result.add(map(row)));
                    return result;
                });
    }

    public Future<Task> update(UUID id, Task.Status status, int progress, String error) {
        return pool.preparedQuery("""
                        UPDATE tasks SET status=$2, progress=$3, error=$4,
                        started_at=CASE WHEN $2='RUNNING' THEN COALESCE(started_at,now()) ELSE started_at END,
                        finished_at=CASE WHEN $2 IN ('COMPLETED','FAILED') THEN now() ELSE finished_at END
                        WHERE id=$1 AND (status='PENDING' AND $2 IN ('RUNNING','FAILED')
                          OR status='RUNNING' AND $2 IN ('RUNNING','COMPLETED','FAILED')) AND progress <= $3 RETURNING *
                        """).execute(Tuple.of(id, status.name(), progress, error))
                .compose(rows -> rows.size() == 0 ? Future.failedFuture("Недопустимый переход задачи") : Future.succeededFuture(map(rows.iterator().next())));
    }

    private Task map(Row r) {
        return new Task(r.getUUID("id"), r.getUUID("owner_id"), r.getString("type"), Task.Status.valueOf(r.getString("status")),
                r.getInteger("progress"), r.getOffsetDateTime("created_at"), r.getOffsetDateTime("started_at"), r.getOffsetDateTime("finished_at"), r.getString("error"));
    }
}
