package dev.taskflow;

import ch.qos.logback.classic.*;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.*;

class LoggingConfigurationTest {
    @Test
    void configurationUsesNonBlockingAsyncConsoleAndConfigurableLevel() throws Exception {
        LoggerContext context = new LoggerContext();
        try {
            context.putProperty("LOG_LEVEL", "ERROR");
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            configurator.doConfigure(getClass().getResource("/logback.xml"));
            assertEquals(Level.ERROR, context.getLogger("dev.taskflow").getEffectiveLevel());
            assertFalse(context.getLogger("dev.taskflow").isInfoEnabled());
            assertEquals(Level.WARN, context.getLogger("io.vertx").getEffectiveLevel());
            AsyncAppender async = (AsyncAppender) context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("ASYNC");
            assertNotNull(async);
            assertTrue(async.isStarted());
            assertTrue(async.isNeverBlock());
            assertEquals(4096, async.getQueueSize());
            assertEquals(0, async.getDiscardingThreshold());
            ConsoleAppender<?> console = (ConsoleAppender<?>) async.getAppender("CONSOLE");
            assertNotNull(console);
            assertTrue(console.isStarted());
            String pattern = ((PatternLayoutEncoder) console.getEncoder()).getPattern();
            for (String field : new String[]{"%d{yyyy-MM-dd HH:mm:ss.SSS}", "%thread", "%logger", "%msg", "level"})
                assertTrue(pattern.contains(field));
        } finally {
            context.stop();
        }
    }
}
