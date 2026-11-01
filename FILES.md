# Созданные файлы

Инструкции `AGENTS.md` и `prompts/001-initial.md` оставлены без изменений. Коммиты не создавались.

## Сборка и инфраструктура

- `settings.gradle.kts`
- `build.gradle.kts`
- `gradlew`
- `gradlew.bat`
- `gradle/wrapper/gradle-wrapper.jar`
- `gradle/wrapper/gradle-wrapper.properties`
- `.gitignore`
- `.dockerignore`
- `.env.example`
- `Dockerfile`
- `docker-compose.yml`

## Приложение

- `src/main/java/dev/taskflow/Main.java`
- `src/main/java/dev/taskflow/auth/AuthService.java`
- `src/main/java/dev/taskflow/auth/AuthStore.java`
- `src/main/java/dev/taskflow/auth/PasswordHasher.java`
- `src/main/java/dev/taskflow/http/ApiException.java`
- `src/main/java/dev/taskflow/http/HttpRequestLogging.java`
- `src/main/java/dev/taskflow/http/Routes.java`
- `src/main/java/dev/taskflow/http/SseNotifications.java`
- `src/main/java/dev/taskflow/persistence/Database.java`
- `src/main/java/dev/taskflow/persistence/PgAuthStore.java`
- `src/main/java/dev/taskflow/persistence/PgTaskStore.java`
- `src/main/java/dev/taskflow/task/Task.java`
- `src/main/java/dev/taskflow/task/TaskStore.java`
- `src/main/java/dev/taskflow/task/TaskService.java`
- `src/main/java/dev/taskflow/task/TaskExecutor.java`
- `src/main/java/dev/taskflow/task/TaskHandler.java`
- `src/main/java/dev/taskflow/task/TaskHandlerRegistry.java`
- `src/main/java/dev/taskflow/task/TimedTaskHandler.java`

## Ресурсы и интерфейс

- `src/main/resources/db/schema.sql`
- `src/main/resources/logback.xml`
- `src/main/resources/webroot/index.html`
- `src/main/resources/webroot/style.css`
- `src/main/resources/webroot/app.js`

## Тесты

- `src/test/java/dev/taskflow/MemoryStores.java`
- `src/test/java/dev/taskflow/PasswordHasherTest.java`
- `src/test/java/dev/taskflow/TaskExecutorTest.java`
- `src/test/java/dev/taskflow/RoutesTest.java`
- `src/test/java/dev/taskflow/LoggingConfigurationTest.java`
- `src/test/java/dev/taskflow/PostgresTest.java`
- `src/test/java/dev/taskflow/http/SseNotificationsTest.java`

## Документация

- `README.md`
- `FILES.md`

Во время проверки также создаются игнорируемые каталоги `build`, `.gradle`, `.tools` (загруженный Gradle, его кэш,
временный кластер PostgreSQL и журналы проверок). Эти файлы не входят в исходный код проекта.

## Изменения по prompts/002-logging.md

- `build.gradle.kts` — SLF4J и Logback.
- `src/main/resources/logback.xml` — новый файл конфигурации.
- `src/main/java/dev/taskflow/Main.java` — логи запуска/ошибок, сброс очереди в shutdown hook.
- `src/main/java/dev/taskflow/http/HttpRequestLogging.java` — новый обработчик access log.
- `src/main/java/dev/taskflow/http/Routes.java` — подключение access log и безопасные ошибки.
- `src/main/java/dev/taskflow/http/SseNotifications.java` — совместная очистка через RoutingContext.
- `src/main/java/dev/taskflow/task/TaskService.java` — событие создания задачи.
- `src/main/java/dev/taskflow/task/TaskExecutor.java` — начало, завершение, исходная ошибка и ошибка сохранения.
- `src/test/java/dev/taskflow/LoggingConfigurationTest.java` — новый тест конфигурации.
- `src/test/java/dev/taskflow/RoutesTest.java` — проверки HTTP/жизненного цикла/секретов/отключения SSE.
- `src/test/java/dev/taskflow/http/SseNotificationsTest.java` — адаптация к совместным обработчикам завершения.
- `.env.example`, `docker-compose.yml` — передача LOG_LEVEL.
- `README.md`, `FILES.md` — документация и перечень изменений.
