# Идемпотентный append (design-заготовка, НЕ готов к реализации)

> Статус: **DESIGN / нужны продуктовые решения.** Вынесено из плана
> `20260627-transient-404-retry.md`, который закрывает 404 только для идемпотентных
> режимов (merge/overwrite/ensure_sheet). Этот документ — не готовый план, а
> фиксация проблемы, вариантов и решений, которые надо принять ДО написания задач.
> Дополнен по итогам codex-review (2026-07-01): исправлен вариант B (XCom), добавлены
> failure modes и решения.

## Проблема

`GoogleSheetsWriteOperator` в режиме `append` (`_execute_append`) дописывает строки
через `values.append` (`INSERT_ROWS`) в цикле по батчам. Это **не идемпотентно** —
повтор дописывает те же строки повторно → **дубли**. Источников повтора **три**,
и дизайн обязан закрыть все:

1. **Airflow task-retry** после частичного сбоя → повтор всего `_execute_append`.
2. **Транзиентный 404** в середине цикла (мотивация исходного плана).
3. **Ambiguous success существующего hook-retry.** `append_values` уже декорирован
   retry на `429/500/503` (`hooks/google_sheets.py:190`) и вызывается в цикле
   (`write.py:543`). Если `append` **применился на сервере, но ответ потерян** (или
   пришёл 503), внутренний hook-retry допишет тот же батч повторно — **ещё до**
   любого operator-level dedup. То есть дубли возможны даже без task-retry и без 404.

Простой operation-level retry (как для merge/overwrite) здесь **не подходит** —
повтор всей операции = повтор append = дубли.

## Почему best-effort in-memory guard отвергнут

Рассматривался batch-guard: якорь на `updates.updatedRange`, presence-реконсиляция
по позиции. Отвергнут:

1. **Не переживает Airflow task-retry** — якорь в памяти теряется при падении задачи.
2. **Риск тихой потери данных** — ложное «батч записан» → пропуск батча.
3. **Держится на непроверяемых допущениях** (атомарность append; нет конкурентной записи).
4. **Не закрывает источник №3** (ambiguous success hook-retry) — он срабатывает
   внутри одного `append_values`, ниже уровня guard.

Вывод: правильная защита требует **durable state**, переживающего task-retry, и
меняет контракт append — отдельный дизайн, а не обёртка на 404.

## Кандидаты-подходы (нужно выбрать; список неполный — дополнять)

### A. Dedup / idempotency-ключ (marker state)

Каждой строке/батчу — детерминированный токен операции. Перед записью читаем
существующие токены, пропускаем уже записанные. Токен должен кодировать **identity
операции**: `dag_id/task_id/run_id/map_index` + порядковый номер строки/батча.

- ✅ переживает task-retry и ambiguous-success (токен персистентен в листе).
- ✅ при identity-токене **намеренные дубли значений сохраняются** (одинаковые
  строки в одном append имеют разные ordinal).
- ❌ **где хранить marker** — открытый вопрос (не обязательно служебная колонка):
  - служебная колонка (меняет раскладку листа, конфликт со схемой/формулами);
  - скрытый технический лист / ledger-вкладка;
  - Google Sheets `developerMetadata` (без видимых изменений данных — оценить лимиты);
- ❌ чтение существующих токенов на больших листах (40k+) каждый прогон.
- ⚠️ **не использовать слепой hook-retry вокруг non-idempotent append** — retry
  должен быть на уровне стратегии, с повторным чтением durable markers.

### B. Позиционная запись с durable baseline

На первой попытке фиксируем `baseline_end` (конец таблицы до append) в **durable
store**; на retry читаем его → пишем `update_values` в фиксированный диапазон
`[baseline_end+1 ..]` (идемпотентно), а не `append`.

- ✅ лишней колонки нет, намеренные дубли сохраняются.
- ❌ **XCom НЕ подходит:** для `apache-airflow>=2.7,<3.0` XCom failed-задачи
  **чистится перед retry** → `baseline_end` исчезнет ровно тогда, когда нужен.
  Durable store должен переживать retry: скрытый лист/ledger, внешняя БД/objects,
  Airflow `Variable` (namespace/TTL), либо **отдельный upstream baseline-task**, чей
  *успешный* XCom читает write-task (успешные XCom не чистятся).
- ❌ **несовместим с `sort_keys`:** если первый прогон записал+отсортировал, потом
  упал, retry `update_values` в `[baseline_end+1..]` перезапишет уже отсортированные
  старые строки → потеря/коррупция. Нужно запретить `sort_keys` для B или
  marker-lookup перед update.
- ❌ **конкурентная запись:** retry может перезаписать строки другого writer'а,
  появившиеся после baseline. Нужен mutual exclusion или conflict detection (читать
  target перед update, fail-fast если не пусто / не совпадает с own payload).
- ❌ **`append + cell_range`:** `update_values` обязан воспроизвести server-side
  table detection append'а (cell_range/table_start/gaps/headers/формулы) — отдельный
  design risk; либо ограничить B без `cell_range`.

## Решения, которые надо принять ДО написания плана

1. **Shape API:** новый `write_mode="idempotent_append"`, opt-in параметр, или смена
   default с migration note? (менять существующий `append` опасно — он легитимно
   допускает повторную запись).
2. **Same-operation-identity:** manual re-run/clear той же task instance в том же
   `run_id` — это новый append или retry? (определяет структуру токена/baseline).
3. Допустима ли служебная колонка? Приемлем ли скрытый лист / `developerMetadata`? (A)
4. Какой durable store для baseline и его жизненный цикл (не XCom)? (B)
5. Возможна ли конкурентная запись в один лист? Нужен mutual exclusion / conflict
   detection? (ломает и A, и B по-разному)
6. Поддерживаем ли `sort_keys` и `cell_range` в идемпотентном append, или ограничиваем?
7. Какие боевые DAG-и реально используют `append` и насколько критичны? (нужен ли
   дизайн вообще, или append второстепенен).

## Контекст в коде (на момент выноса)

- `_execute_append` — `operators/write.py` (setup с ветками sort/legacy; цикл
  `append_values`; sort-хвост `_execute_sort`).
- `append_values` декорирован hook-retry `429/500/503` (`hooks/google_sheets.py:190`),
  вызывается в цикле (`write.py:543`) — источник ambiguous-success дублей.
- `append + cell_range` разрешён без `sort_keys` (запрет только при sort, write.py:201).
- `_execute_sort` (sortRange) идемпотентен сам по себе, но ломает positional invariant B.
- Существующие тесты режима: `TestAppendSortKeys` в `tests/test_operators/test_write.py`.
- Airflow support range: `apache-airflow>=2.7,<3.0` (`pyproject.toml`) — определяет
  поведение XCom-clearing на retry.

## Failure modes для будущего тест-плана

- XCom-clearing / замена на durable store;
- ambiguous success hook-retry (`append` применился, ответ потерян);
- `sort_keys` после частичного успеха;
- `cell_range` / `table_start` / gaps / headers table detection;
- конкурентная запись (conflict detection);
- manual re-run/clear в том же `run_id`.

## Next step

Проработать через brainstorm: ответить на 7 решений выше, выбрать подход (A / B /
гибрид / marker-вариант) с учётом отвергнутого XCom и несовместимости B+sort,
затем написать полноценный implementation-план. codex-рекомендация: контракт,
вероятно, должен быть **opt-in** и с явно ограниченными режимами (не A+B+concurrency+
sort сразу).
