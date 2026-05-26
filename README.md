        # kafka — Kafka Connect: source и sink connectors

        Homework-шаблон для урока **l4_connect_intro** (Kafka Connect: source и sink connectors) на платформе Vibe Learn.

        ## Что делать

        Реализуй минимальный source connector в Kafka Connect standalone mode на базе
FileStreamSource: читает CSV-файл и публикует строки в Kafka topic.

Задание:
1. Создай `worker.properties` для standalone mode (bootstrap.servers, key/value
   converters, offset.storage.file.filename).
2. Создай `csv-source.properties` для FileStreamSource (file path, topic name).
3. Запусти Connect standalone: `connect-standalone.sh worker.properties csv-source.properties`.
4. Через Go HTTP client сделай GET-запрос к REST API Connect:
   `GET http://localhost:8083/connectors/csv-source/status`
   и распечатай ответ. Assert: поле `connector.state` == "RUNNING".
5. CI-тест: produce-записи проверяют, что количество сообщений в топике == количеству
   строк в CSV-файле (исключая header).

Требования к Go-коду:
- Использовать `net/http` для REST-запроса к Connect API.
- Корректно обрабатывать non-200 ответы (log + exit 1).
- Логировать: имя коннектора, статус, количество задач.

## Контекст (из transfer-задачи урока)

Команда data engineering в e-commerce компании хочет построить два пайплайна:

**Пайплайн 1.** PostgreSQL таблица `orders` (CDC через Debezium) → Kafka топик
`orders_events`. Нагрузка: 10 000 INSERT/UPDATE в минуту в часы пик.

## Recap из урока

- **Source connector** читает из внешней системы и пишет в Kafka; **sink connector** читает из Kafka и пишет во внешнюю систему. Направление — главное различие.
- **Standalone mode** — один процесс для dev/PoC; **distributed mode** — кластер воркеров с failover, REST API и хранением конфигов в Kafka. Production всегда distributed.
- Connect снимает с команды retry-логику, offset management (`connect-offsets`), мониторинг и distributed coordination. Кастомный consumer оправдан только при сложной бизнес-логике внутри обработки.
- Source connector сохраняет позицию во внешней системе в топик **`connect-offsets`** — при рестарте продолжает с последнего сохранённого места, не теряя данных.
- **Schema Registry** — мощный, но необязательный компонент Connect. Рекомендуется для production-пайплайнов с долгим жизненным циклом и эволюцией схем.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
