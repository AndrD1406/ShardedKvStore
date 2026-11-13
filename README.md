# ShardedKvStore

Розподілена Key-Value база даних із шардингом (спрощений аналог DynamoDB) на .NET 10 / C# 14.

* **Coordinator** приймає запити клієнтів, зберігає метадані таблиць, реєстр шардів і кільце consistent hashing (150 vnodes на шард, MurmurHash3, бінарний пошук O(log V)).
* **Shard** — вузол зберігання (`ConcurrentDictionary` партицій → `SortedDictionary` за sortKey) з опціональним JSON-снапшотом. При старті сам реєструється в координаторі (Polly retry, експоненційна затримка) і далі шле heartbeat.
* Шард обирається **тільки за `tableName:partitionKey`**, тому вся партиція лежить на одному шарді, відсортована за `sortKey`.
* **Single-Node режим**: поки шардів немає, координатор зберігає дані сам. Коли з'являється перший шард (або новий шард), фоновий `RebalanceService` переносить партиції їхнім новим власникам.

## Структура

```
ShardedKvStore/
├── ShardedKvStore.sln
├── Directory.Build.props
├── docker-compose.yml
├── .dockerignore / .gitignore
├── .github/workflows/deploy.yml        # build → benchmark → GHCR → EC2 (SSH або SSM)
├── scripts/export-openapi.{ps1,sh}     # збереження openapi.json у ./docs
├── src/
│   ├── Shared/                         # DTO, ConsistentHashRing<T>, MurmurHash3, PartitionedItemStore, JSON-персистенс
│   ├── Coordinator/                    # Minimal API: tables, shards, items, cluster; маршрутизація; міграція ключів
│   └── Shard/                          # Minimal API /internal/*; самореєстрація + heartbeat
└── tests/
    └── ShardingBenchmark/              # симуляція міграції 2→3 шарди + live-перевірка кластера
```

## Запуск через `dotnet run` (5 терміналів)

```bash
dotnet run --project src/Coordinator --launch-profile coordinator          # http://localhost:5000/scalar/v1
dotnet run --project src/Shard --launch-profile shard-1                    # http://localhost:5001
dotnet run --project src/Shard --launch-profile shard-2                    # http://localhost:5002
dotnet run --project tests/ShardingBenchmark -- --cluster http://localhost:5000 --items 3000
dotnet run --project src/Shard --launch-profile shard-3                    # додаємо 3-й шард → автоматична міграція
dotnet run --project tests/ShardingBenchmark -- --cluster http://localhost:5000 --items 3000 --verify-only
```

Офлайн-бенчмарк консистентного хешування (10 000 ключів, 2 → 3 шарди, порівняння з `hash % N`):

```bash
dotnet run --project tests/ShardingBenchmark
```

## Запуск через Docker

```bash
docker compose up --build
```

Coordinator: http://localhost:8080/scalar/v1, шарди: http://localhost:8081..8083/scalar/v1.
Щоб побачити міграцію в Docker: `docker compose up -d --build coordinator shard-1 shard-2`, запустити бенчмарк з `--cluster http://localhost:8080`, потім `docker compose up -d shard-3` і повторити з `--verify-only`.

## OpenAPI

* UI: `/scalar/v1`, документ: `/openapi/v1.json`.
* Експорт: `./scripts/export-openapi.ps1` (або `.sh`) → `docs/coordinator.openapi.json`, `docs/shard.openapi.json`.

## Приклади

```bash
curl -X POST localhost:5000/api/tables -H "content-type: application/json" -d '{"tableName":"orders","partitionKey":"userId","sortKey":"orderId"}'
curl -X POST localhost:5000/api/tables/orders/items -H "content-type: application/json" -d '{"userId":"u1","orderId":"o-001","total":42.5}'
curl "localhost:5000/api/tables/orders/items/u1?sortKey=o-001"
curl "localhost:5000/api/tables/orders/items/u1?from=o-000&to=o-100&limit=10"
curl -I "localhost:5000/api/tables/orders/items/u1?sortKey=o-001"
curl "localhost:5000/api/tables/orders/items/u1/exists?sortKey=o-001"
curl -X DELETE "localhost:5000/api/tables/orders/items/u1?sortKey=o-001"
curl "localhost:5000/api/cluster/route?tableName=orders&partitionKey=u1"
curl localhost:5000/api/cluster/stats
```

Відповіді на запис/читання містять заголовок `X-Stored-On` — вузол, де лежить запис.

## Змінні середовища

| Сервіс | Змінна | Типово | Опис |
|---|---|---|---|
| Coordinator | `DATA_DIR` | `data` | `cluster-state.json` + локальне сховище single-node; `none` — лише пам'ять |
| Coordinator | `VIRTUAL_NODES` | `150` | vnodes на шард |
| Coordinator | `SHARD_TIMEOUT_SECONDS` | `10` | таймаут запиту до шарда |
| Shard | `SHARD_ID` | `shard-local` | ідентифікатор |
| Shard | `SELF_URL` | перша адреса Kestrel | адреса, за якою координатор досягає шард |
| Shard | `COORDINATOR_URL` | — | без неї шард працює автономно |
| Shard | `PERSISTENCE_PATH` | `data/{SHARD_ID}.json` | JSON-снапшот; `none` — лише пам'ять |
| Shard | `HEARTBEAT_SECONDS` | `15` | інтервал повторної реєстрації |

## Деплой (GitHub Actions → EC2)

`.github/workflows/deploy.yml` на push у `main`: білд, бенчмарк, публікація образів у GHCR, деплой на EC2.
* SSH (типово): секрети `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY`.
* SSM: змінна `DEPLOY_METHOD=ssm`, секрети `AWS_ROLE_ARN` (OIDC), `EC2_INSTANCE_ID`.
На EC2 потрібен Docker з compose-плагіном.

## Обмеження (свідомо спрощено)

* Немає реплікації: падіння шарда → `503` для його партицій.
* Міграція без tombstones: запис, видалений на новому власнику під час перенесення партиції, може «воскреснути»; під час перенесення читання партиції може коротко повернути 404.
* Шард не видаляється з кільця (немає `DELETE /api/shards`).
