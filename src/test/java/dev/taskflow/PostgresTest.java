package dev.taskflow;

import dev.taskflow.auth.PasswordHasher;
import dev.taskflow.persistence.*;
import dev.taskflow.task.*;
import io.vertx.core.Vertx;
import io.vertx.pgclient.*;
import io.vertx.sqlclient.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.UUID;

import static dev.taskflow.TaskExecutorTest.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Использует отдельную случайную схему, не затрагивая таблицы существующего приложения.
 */
@EnabledIfEnvironmentVariable(named = "TEST_DB_PORT", matches = ".+")
class PostgresTest {
    private Vertx vertx;
    private Pool admin;
    private Pool pool;
    private String schema;

    @BeforeEach
    void start() throws Exception {
        vertx = Vertx.vertx();
        PgConnectOptions options = new PgConnectOptions().setHost(System.getenv().getOrDefault("TEST_DB_HOST", "localhost"))
                .setPort(Integer.parseInt(System.getenv("TEST_DB_PORT"))).setDatabase(System.getenv().getOrDefault("TEST_DB_NAME", "postgres"))
                .setUser(System.getenv().getOrDefault("TEST_DB_USER", "postgres")).setPassword(System.getenv().getOrDefault("TEST_DB_PASSWORD", ""));
        admin = PgBuilder.pool().using(vertx).connectingTo(options).build();
        schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        await(admin.query("CREATE SCHEMA " + schema).execute());
        pool = PgBuilder.pool().using(vertx).connectingTo(new PgConnectOptions(options).addProperty("search_path", schema)).build();
        await(Database.initialize(vertx, pool));
    }

    @AfterEach
    void close() throws Exception {
        try {
            if (pool != null) await(pool.close());
            if (admin != null && schema != null) await(admin.query("DROP SCHEMA " + schema + " CASCADE").execute());
        } finally {
            if (vertx != null) await(vertx.close());
        }
    }

    @Test
    void persistedLifecycleIsolationAndRestartRecovery() throws Exception {
        PgAuthStore auth = new PgAuthStore(pool);
        UUID alice = UUID.randomUUID(), bob = UUID.randomUUID();
        await(auth.register(alice, "alice", "test-hash"));
        await(auth.register(bob, "bob", "test-hash"));
        PgTaskStore tasks = new PgTaskStore(pool);
        Task first = await(tasks.create(alice, "DEMO"));
        Task pending = await(tasks.create(alice, "REPORT"));
        assertNull(await(tasks.find(bob, first.id())));
        assertTrue(await(tasks.list(bob)).isEmpty());
        Task running = await(tasks.update(first.id(), Task.Status.RUNNING, 25, null));
        assertNotNull(running.startedAt());
        assertThrows(Exception.class, () -> await(tasks.update(first.id(), Task.Status.RUNNING, 10, null)));
        await(Database.initialize(vertx, pool));
        Task failed = await(tasks.find(alice, first.id()));
        assertEquals(Task.Status.FAILED, failed.status());
        assertEquals(25, failed.progress());
        assertNotNull(failed.finishedAt());
        assertEquals("Приложение перезапущено", failed.error());
        assertEquals(Task.Status.FAILED, await(tasks.find(alice, pending.id())).status());
        Task completed = await(tasks.create(alice, "DEMO"));
        await(tasks.update(completed.id(), Task.Status.RUNNING, 0, null));
        await(tasks.update(completed.id(), Task.Status.COMPLETED, 100, null));
        assertThrows(Exception.class, () -> await(tasks.update(completed.id(), Task.Status.RUNNING, 100, null)));
        await(Database.initialize(vertx, pool));
        assertEquals(Task.Status.COMPLETED, await(tasks.find(alice, completed.id())).status());
    }

    @Test
    void sessionsExpireAndDuplicateLoginIsRejected() throws Exception {
        PgAuthStore auth = new PgAuthStore(pool);
        UUID owner = UUID.randomUUID();
        await(auth.register(owner, "alice", "test-hash"));
        assertThrows(Exception.class, () -> await(auth.register(UUID.randomUUID(), "alice", "other-hash")));
        String hash = PasswordHasher.tokenHash(PasswordHasher.newToken());
        await(auth.session(hash, owner));
        assertEquals(owner, await(auth.authenticate(hash)));
        await(pool.query("UPDATE sessions SET expires_at=now()-interval '1 second'").execute());
        assertNull(await(auth.authenticate(hash)));
    }
}
