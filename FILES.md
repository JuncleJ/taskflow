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
- `src/test/java/dev/taskflow/task/TimedTaskHandler.java` — тестовый обработчик фиксированных шагов.
- `src/test/java/dev/taskflow/RoutesTest.java`
- `src/test/java/dev/taskflow/LoggingConfigurationTest.java`
- `src/test/java/dev/taskflow/PostgresTest.java`
- `src/test/java/dev/taskflow/http/SseNotificationsTest.java`
- `src/test/java/dev/taskflow/http/SseIntegrationTest.java`

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

## Дополнение: события аутентификации

- `src/main/java/dev/taskflow/auth/AuthService.java` — успех регистрации и входа, отказы и внутренние сбои,
  DEBUG-событие успешной проверки токена; без учётных данных.
- `src/test/java/dev/taskflow/RoutesTest.java` — проверки уровней, причин отказов, успеха и отсутствия секретов.
- `README.md` — описание событий и результатов проверки: 16 тестов прошли без пропусков.
- `FILES.md` — перечень изменений.

## Дополнение: шесть интеграционных сценариев SSE

- `src/test/java/dev/taskflow/http/SseIntegrationTest.java` — новый набор тестов с настоящими HTTP/SSE и PostgreSQL,
  управляемым прогрессом, проверкой ресурсов и русским описанием назначения каждого сценария.
- `README.md` — таблица сценариев, условия и команда запуска, особенности и границы проверок.
- `FILES.md` — перечень добавленных и изменённых файлов.

Код приложения и зависимости не изменены.

## Дополнение: текущий пользователь в шапке

- `src/main/resources/webroot/index.html`, `style.css`, `app.js` — имя рядом с кнопкой выхода,
  загрузка профиля при восстановлении сессии и очистка при выходе.
- `src/main/java/dev/taskflow/auth/AuthStore.java`, `AuthService.java` — публичный профиль текущего пользователя.
- `src/main/java/dev/taskflow/persistence/PgAuthStore.java` — неблокирующая выборка UUID и логина.
- `src/main/java/dev/taskflow/http/Routes.java` — защищённый GET /api/auth/me.
- `src/main/java/dev/taskflow/http/HttpRequestLogging.java` — шаблон нового маршрута в access log.
- `src/test/java/dev/taskflow/MemoryStores.java`, `RoutesTest.java`, `PostgresTest.java` — тестовое хранилище,
  проверка доступа, изоляции профилей и полей ответа, реальные запросы PostgreSQL.
- `README.md`, `FILES.md` — описание API, поведения интерфейса и перечень изменений.

Проверено: `gradlew.bat build test` — успешно, все 23 теста прошли без пропусков, включая PostgreSQL.

## Изменения по prompts/003-task-lifecycle.md

Созданы:

- `src/main/java/dev/taskflow/task/CancellationToken.java` — кооперативный сигнал отмены.
- `src/main/resources/db/002-cancelled.sql` — совместимое обновление CHECK существующей таблицы.
- `src/test/java/dev/taskflow/TaskLifecycleTest.java` — управляемые проверки жизненного цикла и гонок.
- `docs/task-lifecycle.md` — API, переходы, технические решения, проверки и ограничения.

Изменены:

- `src/main/java/dev/taskflow/task/Task.java` — терминальный статус CANCELLED.
- `src/main/java/dev/taskflow/task/TaskHandler.java`, `TimedTaskHandler.java` — контракт и обработка отмены.
- `src/main/java/dev/taskflow/task/TaskExecutor.java` — отмена очереди/исполнения, финализация и освобождение слота.
- `src/main/java/dev/taskflow/task/TaskService.java`, `TaskStore.java` — операции отмены/удаления и проверки доступа.
- `src/main/java/dev/taskflow/persistence/PgTaskStore.java`, `Database.java` — условные SQL-переходы, физическое
  удаление и миграция.
- `src/main/java/dev/taskflow/http/Routes.java`, `HttpRequestLogging.java` — endpoints и шаблон маршрута отмены.
- `src/main/java/dev/taskflow/http/SseNotifications.java` — cancelled/deleted и очистка подписок при удалении.
- `src/main/java/dev/taskflow/Main.java` — подключение уведомлений об удалении.
- `src/main/resources/webroot/app.js` — кнопки, статус, обработка 204/ошибок и удаление карточек.
- `src/test/java/dev/taskflow/MemoryStores.java`, `TaskExecutorTest.java`, `RoutesTest.java` — актуальный контракт
  тестовых обработчиков и хранилища.
- `src/test/java/dev/taskflow/PostgresTest.java`, `http/SseIntegrationTest.java` — SQL/HTTP/SSE-проверки отмены и
  удаления.
- `FILES.md` — перечень изменений.

README не изменялся. Новых зависимостей и Git-коммитов нет.

Проверки жизненного цикла: чистая сборка `gradlew.bat clean build test --rerun-tasks` успешна,
все 42 теста прошли без ошибок и пропусков, включая PostgreSQL и HTTP/SSE. Синтаксис app.js проверен через node --check.

## Изменения по prompts/004-configurable-tasks.md

Созданы:

- `src/main/java/dev/taskflow/task/TaskParameters.java` — строгая проверка числовых параметров.
- `src/main/java/dev/taskflow/task/DemoTaskHandler.java` — длительность DEMO на неблокирующих таймерах.
- `src/main/java/dev/taskflow/task/CpuTaskHandler.java` — подсчёт простых чисел в ограниченном CPU-пуле.
- `src/main/resources/db/003-task-parameters.sql` — JSONB-параметры для существующих баз.
- `src/test/java/dev/taskflow/task/CpuTaskHandlerTest.java`, `DemoTaskHandlerTest.java` — алгоритм, потоки, отмена,
  длительность и ограничения.
- `src/test/java/dev/taskflow/ConfigurableTaskServiceTest.java` — параметры и резервирование ограниченной общей очереди.
- `docs/configurable-tasks.md` — API, потоки, настройки, технические решения, проверки и ограничения.

Изменены:

- `src/main/java/dev/taskflow/task/Task.java` — защищённые копированием параметры в снимке задачи.
- `src/main/java/dev/taskflow/task/TaskHandler.java`, `TimedTaskHandler.java` — передача сохранённой задачи в
  обработчик.
- `src/main/java/dev/taskflow/task/CancellationToken.java` — неблокирующее уведомление для отмены ожидающей CPU-работы.
- `src/main/java/dev/taskflow/task/TaskService.java`, `TaskExecutor.java` — нормализация, резервирование очереди и
  передача параметров.
- `src/main/java/dev/taskflow/task/TaskStore.java`, `persistence/PgTaskStore.java`, `persistence/Database.java` —
  сохранение JSONB и инициализация схемы.
- `src/main/java/dev/taskflow/http/Routes.java` — параметры из тела POST /api/tasks.
- `src/main/java/dev/taskflow/Main.java` — регистрация обработчиков, настройки и остановка CPU-пула.
- `.env.example`, `docker-compose.yml` — CPU_WORKERS, CPU_QUEUE_CAPACITY, TASK_QUEUE_CAPACITY.
- `src/main/resources/webroot/index.html`, `style.css`, `app.js` — поля DEMO/CPU_TASK, отправка и отображение
  параметров.
- `src/test/java/dev/taskflow/MemoryStores.java`, `TaskExecutorTest.java`, `TaskLifecycleTest.java`, `RoutesTest.java` —
  новый контракт обработчиков и хранилища.
- `src/test/java/dev/taskflow/PostgresTest.java`, `http/SseIntegrationTest.java` — реальные параметры, HTTP/SSE, отмена
  и доступность HTTP при активных CPU_TASK.
- `FILES.md` — перечень изменений.

Новых зависимостей и Git-коммитов нет. README не изменялся.

Результат проверки настраиваемых задач: `gradlew.bat clean build test --rerun-tasks` — успешно,
все 63 теста прошли без ошибок и пропусков, включая PostgreSQL, HTTP/SSE, CPU-потоки и завершение пула.
Синтаксис app.js проверен через node --check.

## Удаление REPORT и отступы списка задач

- `src/main/java/dev/taskflow/Main.java` — в реестре приложения оставлены DEMO и CPU_TASK.
- `src/main/java/dev/taskflow/task/TimedTaskHandler.java` перенесён в
  `src/test/java/dev/taskflow/task/TimedTaskHandler.java`: фиксированные шаги нужны только тестовым сценариям.
- `src/main/resources/webroot/index.html` — убрана кнопка REPORT.
- `src/main/resources/webroot/style.css` — разделитель и отступ панели запуска, интервалы карточек,
  отдельный отступ кнопок внутри карточки.
- `src/main/resources/webroot/app.js` — класс для расположения кнопок карточки.
- `src/test/java/dev/taskflow/RoutesTest.java`, `PostgresTest.java`, `http/SseIntegrationTest.java` — убран запуск REPORT,
  проверяется отказ HTTP 400 при попытке его создания.
- `README.md`, `docs/configurable-tasks.md`, `FILES.md` — актуальные типы и перечень изменений.

Ранее сохранённые задачи REPORT не удаляются автоматически. Чистая сборка успешна, все 63 теста прошли
без ошибок и пропусков с PostgreSQL. Синтаксис app.js проверен. Визуальная проверка браузером не выполнялась.
