# Распределенная система видеоаналитики

Этот сервис предоставляет REST API для запуска и управления сценариями обработки видео, а также получения результатов инференса. Сервис разворачивается с помощью Docker Compose.

## Запуск

```bash
docker compose up --build
```

После запуска, сервис будет доступен на `http://localhost:PORT`, где `PORT` — порт, указанный в `docker-compose.yml`, а также в `/userapi/.env`, по умолчанию `8080`

## REST API

### 1. `POST /scenario`

**Описание:** Запуск или остановка сценария по видео.

**Тело запроса (JSON):**

```json
{
  "id": "cat",
  "action": "start"
}
```

`id` - название видео из `/producer/files` без расширения
`action` - start/stop - начать/завершить обработку сценария

**Пример запроса с curl:**

```bash
curl -X POST http://localhost:8080/scenario \
     -H "Content-Type: application/json" \
     -d '{"id": "cat", "action": "start"}'
```

---

### 2. `GET /scenario/:id`

**Описание:** Получение текущего статуса сценария.

**Пример запроса:**

```bash
curl http://localhost:8080/scenario/cat
```

**Ожидаемый ответ (пример):**

```json
{
  "status": "active"
}
```

---

### 3. `GET /prediction/:id`

**Описание:** Получение всех результатов инференса по сценарию.

**Пример запроса:**

```bash
curl http://localhost:8080/prediction/cat
```

**Ожидаемый ответ (пример):**

```json
{
  "status": "ok",
  "predictions": [
    {
      "scenario_id": "cat",
      "class": "tabby cat",
      "confidence": 0.98,
      "created_at": "2025-06-30T18:45:00Z"
    }
  ]
}
```

---

## Логи и мониторинг

Все логи доступны в консоли после запуска через `docker compose up`.
