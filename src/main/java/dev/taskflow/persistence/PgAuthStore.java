package dev.taskflow.persistence;

import dev.taskflow.auth.AuthStore;
import dev.taskflow.http.ApiException;
import io.vertx.core.Future;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.*;

import java.util.UUID;

public final class PgAuthStore implements AuthStore {
    private final Pool pool;

    public PgAuthStore(Pool pool) {
        this.pool = pool;
    }

    public Future<Void> register(UUID id, String login, String hash) {
        return pool.preparedQuery("INSERT INTO users(id,login,password_hash) VALUES($1,$2,$3)")
                .execute(Tuple.of(id, login, hash)).<Void>mapEmpty().recover(e -> Future.failedFuture(
                        e instanceof PgException pg && "23505".equals(pg.getSqlState()) ? new ApiException(409, "Логин уже занят") : e));
    }

    public Future<User> user(String login) {
        return pool.preparedQuery("SELECT id,password_hash FROM users WHERE login=$1").execute(Tuple.of(login))
                .map(rows -> rows.size() == 0 ? null : new User(rows.iterator().next().getUUID("id"), rows.iterator().next().getString("password_hash")));
    }

    public Future<Void> session(String hash, UUID user) {
        return pool.preparedQuery("INSERT INTO sessions(token_hash,user_id,expires_at) VALUES($1,$2,now()+interval '24 hours')")
                .execute(Tuple.of(hash, user)).mapEmpty();
    }

    public Future<Profile> profile(UUID id) {
        return pool.preparedQuery("SELECT id,login FROM users WHERE id=$1").execute(Tuple.of(id))
                .map(rows -> {
                    if (rows.size() == 0) return null;
                    Row row = rows.iterator().next();
                    return new Profile(row.getUUID("id"), row.getString("login"));
                });
    }

    public Future<UUID> authenticate(String hash) {
        return pool.preparedQuery("SELECT user_id FROM sessions WHERE token_hash=$1 AND expires_at>now()")
                .execute(Tuple.of(hash)).map(rows -> rows.size() == 0 ? null : rows.iterator().next().getUUID("user_id"));
    }
}
