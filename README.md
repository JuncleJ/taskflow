# TaskFlow

Приложение для асинхронного выполнения фоновых задач. Статусы и прогресс сохраняются в PostgreSQL, интерфейс обновляется в реальном времени через Server-Sent Events (SSE).

Возможности: регистрация и вход, запуск задач с параметрами, просмотр прогресса, отмена активных задач и удаление завершённых. Каждый пользователь видит только свои задачи.

Стек: Java 21, Vert.x 5.2.2, Reactive PostgreSQL Client, PostgreSQL 17 в Docker Compose, Gradle Kotlin DSL, JUnit 5. Интерфейс — HTML/CSS/JavaScript без фреймворков.

## Запуск через Docker

Нужны Docker Engine и Docker Compose v2. Команды выполняются из корня проекта.

1. Скопируйте `.env.example` в `.env`:
   ```powershell
   Copy-Item .env.example .env
   ```
   В Linux/macOS: `cp .env.example .env`.
2. Задайте непустой `DB_PASSWORD` в `.env`.
3. Соберите и запустите приложение с БД:
   ```sh
   docker compose up --build -d
   ```
4. Откройте <http://localhost:8080> (или порт из `HTTP_PORT`).

Логи: `docker compose logs -f app`. Остановка: `docker compose down`.
Данные сохраняются в volume `postgres-data`. Таблицы создаются автоматически при запуске приложения.

## Локальный запуск

Нужны JDK 21 и PostgreSQL. Gradle Wrapper включён в проект; отдельная установка Gradle не требуется.

Создайте БД и роль PostgreSQL заранее. Либо подготовьте `.env`, как выше, и запустите только БД:

```sh
docker compose up -d db
```

Передайте настройки подключения через окружение. Пример для PowerShell:

```powershell
# JAVA_HOME должен указывать на JDK 21.
$env:DB_HOST = 'localhost'
$env:DB_PORT = '5432'
$env:DB_NAME = 'taskflow'
$env:DB_USER = 'taskflow'
$env:DB_PASSWORD = Read-Host 'Пароль PostgreSQL'
.\gradlew.bat run
```

При использовании БД из Compose значения должны совпадать с `.env`.
**Локальное приложение не читает `.env` автоматически.**

В Linux/macOS:

```sh
export DB_HOST=localhost DB_PORT=5432 DB_NAME=taskflow DB_USER=taskflow
read -r -s -p 'Пароль PostgreSQL: ' DB_PASSWORD; echo
export DB_PASSWORD
bash ./gradlew run
```

После запуска откройте <http://localhost:8080>.

## Использование

1. Введите логин и пароль, нажмите «Зарегистрироваться». В дальнейшем используйте «Войти».
2. Укажите параметр задачи и нажмите кнопку запуска:

   | Тип | Что делает | Параметр |
   |---|---|---|
   | `DEMO` | Имитирует длительную операцию неблокирующим таймером | `durationSeconds`: 1–300 секунд, по умолчанию 5 |
   | `CPU_TASK` | Считает простые числа от 2 до указанной границы включительно | `limit`: обязательное целое число от 2 до 10 000 000 |

3. Следите за статусом и прогрессом: они обновляются автоматически. «Обновить» повторно загружает список.
4. Для `PENDING` и `RUNNING` доступна «Отменить», для `COMPLETED`, `FAILED`, `CANCELLED` — «Удалить».

Логин: 3–64 латинских буквы, цифры или `_`. Пароль: 8–128 символов.
Результат подсчёта простых чисел (`primeCount`) записывается в лог, в интерфейсе отображаются статус и прогресс.

## Настройки

Для Compose настройки задаются в `.env`, для локального запуска — в переменных окружения.

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DB_HOST` | `localhost` | Адрес БД; в Compose фиксирован как `db` |
| `DB_PORT` | `5432` | Порт БД; в Compose фиксирован как `5432` |
| `DB_NAME` | `taskflow` | Имя БД |
| `DB_USER` | `taskflow` | Пользователь БД |
| `DB_PASSWORD` | — | Обязательный пароль БД |
| `HTTP_PORT` | `8080` | HTTP-порт; в Compose — внешний порт приложения |
| `TASK_PARALLELISM` | `3` | Максимум активных задач всех типов, положительное целое |
| `TASK_QUEUE_CAPACITY` | `100` | Вместимость общей очереди ожидания: 1–10 000 |
| `CPU_WORKERS` | `2` | Число потоков CPU-пула: 1–32 |
| `CPU_QUEUE_CAPACITY` | `16` | Вместимость очереди ожидания CPU-потока: 1–256 |
| `LOG_LEVEL` | `INFO` | Уровень логов: TRACE, DEBUG, INFO, WARN, ERROR, OFF |

CPU-задача, ожидающая поток, уже занимает место в `TASK_PARALLELISM`. Одновременно вычисления выполняют максимум `min(CPU_WORKERS, TASK_PARALLELISM)` задач. При заполнении общей очереди создание отклоняется с HTTP 429; при переполнении CPU-пула принятая задача завершается со статусом `FAILED`.

## REST API

Тела запросов и ответов — JSON, кроме SSE и пустого ответа удаления.
Для всех маршрутов, кроме регистрации и входа, нужен заголовок `Authorization: Bearer <token>`. Срок действия токена — 24 часа.

| Метод | Путь | Результат |
|---|---|---|
| POST | `/api/auth/register` | 201, `{token, expiresIn}` |
| POST | `/api/auth/login` | 200, `{token, expiresIn}` |
| GET | `/api/auth/me` | 200, `{id, login}` |
| POST | `/api/tasks` | 202, созданная задача; заголовок `Location` |
| GET | `/api/tasks` | 200, до 200 последних собственных задач |
| GET | `/api/tasks/:id` | 200, задача |
| POST | `/api/tasks/:id/cancel` | 200, отменённая задача; повторная отмена `CANCELLED` допустима |
| DELETE | `/api/tasks/:id` | 204, физическое удаление завершённой задачи |
| GET | `/api/tasks/:id/events` | 200, поток `text/event-stream` |

Регистрация и вход принимают `{"login":"alice","password":"your-password"}`.
Создание принимает `{"type":"DEMO","durationSeconds":20}` или `{"type":"CPU_TASK","limit":1000000}`.

Пример PowerShell:

```powershell
$base = 'http://localhost:8080'
$credentials = @{login = 'alice'; password = Read-Host 'Пароль TaskFlow'} | ConvertTo-Json
$session = Invoke-RestMethod "$base/api/auth/register" -Method Post -ContentType 'application/json' -Body $credentials
# Для существующего пользователя замените register на login.
$headers = @{Authorization = "Bearer $($session.token)"}
$task = Invoke-RestMethod "$base/api/tasks" -Method Post -Headers $headers -ContentType 'application/json' -Body '{"type":"DEMO","durationSeconds":20}'
Invoke-RestMethod "$base/api/tasks/$($task.id)" -Headers $headers
Invoke-RestMethod "$base/api/tasks/$($task.id)/cancel" -Method Post -Headers $headers
Invoke-RestMethod "$base/api/tasks/$($task.id)" -Method Delete -Headers $headers
```

SSE передаёт снимок задачи при подключении и события `progress`, `completed`, `failed`, `cancelled`; при удалении — `deleted` для открытых подписок. После завершения поток закрывается, при разрыве интерфейс переподключается.

Ошибки имеют вид `{"error":"описание"}`: 400 — неверные параметры, 401 — ошибка авторизации, 404 — задача не найдена или принадлежит другому пользователю, 409 — занятый логин или недопустимая операция со статусом задачи, 429 — очередь заполнена, 500 — внутренняя ошибка. Удаление активной задачи и отмена `COMPLETED` / `FAILED` возвращают 409.

## Устройство и ограничения

- `Routes` — HTTP и проверка авторизации; `AuthService` и `TaskService` — бизнес-логика; `PgAuthStore` и `PgTaskStore` — асинхронные запросы к БД.
- `TaskExecutor` управляет общей очередью и параллелизмом, `TaskHandlerRegistry` выбирает обработчик. CPU-вычисления идут в отдельном ограниченном пуле, вне Event Loop.
- Статусы: `PENDING` → `RUNNING` → `COMPLETED` / `FAILED` / `CANCELLED`. Ожидающую задачу можно отменить сразу; выполняющуюся — после фактической остановки обработчика. Изменения сохраняются перед отправкой SSE.
- При запуске оставшиеся в БД `PENDING` и `RUNNING` переходят в `FAILED` с ошибкой «Приложение перезапущено». Автоматического повтора нет.
- Поддерживается один экземпляр приложения: очередь и SSE-подписки находятся в памяти. SSE передаёт актуальное состояние без воспроизведения пропущенных событий.
- Пароли хешируются PBKDF2, в БД хранится хеш токена. Выход удаляет токен из браузера; серверная сессия действует до истечения срока.

Подробности параметров задач: [docs/configurable-tasks.md](docs/configurable-tasks.md).

## Сборка и тесты

```powershell
.\gradlew.bat clean build
```

Сборка включает тесты. Только тесты: `.\gradlew.bat test --rerun-tasks`.
В Linux/macOS используйте `bash ./gradlew` вместо `.\gradlew.bat`.
HTML-отчёт: `build/reports/tests/test/index.html`, дистрибутивы: `build/distributions`.

Без PostgreSQL запускаются тесты с хранилищами в памяти. Для включения интеграционных проверок SQL и SSE задайте подключение к тестовой БД:

```powershell
$env:TEST_DB_HOST = 'localhost'
$env:TEST_DB_PORT = '5432'
$env:TEST_DB_NAME = 'taskflow'
$env:TEST_DB_USER = 'taskflow'
$env:TEST_DB_PASSWORD = Read-Host 'Пароль тестовой БД'
.\gradlew.bat test --rerun-tasks
```

Интеграционные проверки создают и удаляют отдельные случайные схемы; роли нужно право `CREATE` в выбранной БД. Без `TEST_DB_PORT` эти проверки пропускаются.
