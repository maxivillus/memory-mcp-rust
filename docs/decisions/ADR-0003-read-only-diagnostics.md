# ADR-0003: read-only diagnostics for native memory-mcp

## Статус

Принято для `NTSI-893`. Контракт реализуется аддитивно в native Rust MCP
server; существующие compatibility tools и их schemas не меняются.

## Решение

Добавить в `tools/list` четыре bounded read-only tools:
`capabilities_doctor`, `search_diagnose`, `audit_coverage` и
`measurement_status`. Для клиентов, которым нужны человеко-читаемые имена,
оставить неadvertised aliases `capabilities/doctor`, `search diagnose`,
`search/diagnose`, `audit coverage`, `audit/coverage`, `measurement status` и
`measurement/status`.

Диагностика выполняется через текущий `Store` и тот же MCP dispatcher:

- `capabilities_doctor` читает allowlisted provider state, schema/migration/FTS5
  readiness, safe backend counters, explicit workspace hash и aggregate
  telemetry readiness;
- `search_diagnose` классифицирует bounded lexical/semantic/hybrid read и
  возвращает только `query_hash`, scope hash, counters, fallback, safe status и
  один `next_action`;
- `audit_coverage` читает только aggregate telemetry из
  `lifecycle_events.metadata`, фильтрует exact workspace/issue/run/site и
  возвращает aggregate counts, bounded latency и `telemetry_gap`;
- `measurement_status` показывает baseline/memory pair coverage и всегда
  оставляет `status=not_claimed`, `efficacy=not_claimed` и
  `independent_check=not_run`.

`capture_event` с `event_kind=memory-access` принимает только bounded opaque
references, allowlisted outcome, fallback, result count, latency и optional
SHA-256 query hash. Неизвестные поля, raw prompts, comments, queries и payloads
отклоняются до persistence. Диагностика не получает authority, не выполняет
provider calls и не меняет durable state.

## Последствия

Потребитель получает причину пустого или недоступного read без доступа к raw
query или payload. Ошибка SQLite/FTS5 становится `unavailable`, а не
`no_match`; отсутствие, unmapped или truncated telemetry становится
`telemetry_gap`. Поверхность `tools/list` увеличивается с 80 upstream tools до
84 advertised tools, поэтому `docs/current-contract.md`,
`docs/diagnostic-tools.json`, пример ответа и repository skill обновляются
одновременно.

## Проверка

Приёмка должна подтвердить public MCP compatibility, coordinator route,
read-only boundary, bounded output, workspace isolation, secret/raw-data
redaction, negative status matrix и сохранение `not_claimed` без независимой
QA-проверки. Efficacy не заявляется этим ADR.
