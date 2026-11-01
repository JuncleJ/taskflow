package dev.taskflow.persistence;

import io.vertx.core.*;
import io.vertx.sqlclient.Pool;

public final class Database {
    private Database() {}
    public static Future<Void> initialize(Vertx vertx, Pool pool) {
        return vertx.fileSystem().readFile("db/schema.sql").compose(buffer -> pool.query(buffer.toString()).execute())
            .compose(v -> pool.query("UPDATE tasks SET status='FAILED',finished_at=now(),error='Приложение перезапущено' WHERE status IN ('PENDING','RUNNING')").execute())
            .compose(v -> pool.query("DELETE FROM sessions WHERE expires_at<=now()").execute()).mapEmpty();
    }
}
