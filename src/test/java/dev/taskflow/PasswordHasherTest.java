package dev.taskflow;

import dev.taskflow.auth.PasswordHasher;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PasswordHasherTest {
    @Test void passwordsAreSaltedAndVerified() {
        PasswordHasher hasher = new PasswordHasher();
        String first = hasher.hash("correct-password");
        assertNotEquals(first, hasher.hash("correct-password"));
        assertTrue(hasher.verify("correct-password", first));
        assertFalse(hasher.verify("wrong-password", first));
    }
    @Test void tokensAreRandomAndStoredAsDigests() {
        String token = PasswordHasher.newToken();
        assertEquals(43, token.length()); assertNotEquals(token, PasswordHasher.newToken());
        assertEquals(64, PasswordHasher.tokenHash(token).length()); assertNotEquals(token, PasswordHasher.tokenHash(token));
    }
}
