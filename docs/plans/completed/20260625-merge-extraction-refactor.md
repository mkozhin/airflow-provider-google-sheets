# Merge Logic Extraction Refactor

> **Scope note**: это PR2 — чисто **внутренний рефакторинг без изменения
> функционального поведения**. Делается **только после** того как PR1
> (`docs/plans/20260624-merge-key-date-normalization.md`, после реализации —
> `docs/plans/completed/20260624-merge-key-date-normalization.md` — bugfix
> дублей) залит и зелёный. Никаких изменений в публичном поведении оператора;
> CHANGELOG не трогаем (не функциональное поведение) — **кроме** одного
> исключения: logger namespace для merge-key-related warning/info логов
> меняется с `airflow_provider_google_sheets.utils.schema` на
> `airflow_provider_google_sheets.utils.merge_key` (т.к. `normalize_merge_key`
> переезжает в новый модуль и использует `getLogger(__name__)`, как и все
> остальные модули в проекте). Это технически наблюдаемо для тех, кто
> фильтрует логи по имени логгера — задокументировать явно (Task 1), не
> прятать как «не наблюдаемое». Все существующие тесты должны проходить до и
> после, плюс добавляются прямые unit-тесты новых модулей.

## Overview

После PR1 merge-логика размазана по двум местам с низкой тестируемостью:

- `utils/schema.py` смешивает общую конвертацию типов (read+write) с
  merge-специфичной нормализацией ключей (`normalize_merge_key`,
  `infer_date_key_schema`, `_SHEETS_EPOCH`).
- `_execute_merge` (`write.py`) — 155-строчный метод, смешивающий I/O (чтение
  ключей, удаление/добавление строк через API) с чистой логикой (построение
  индекса существующих строк, планирование delete/append операций). Priority-
  логика выбора `key_schema` и построение индекса инлайнятся прямо в нём;
  протестировать их можно только через полный `op.execute()` с mock_hook.

Цель: вынести чистую merge-логику в два модуля со своим прямым, тестируемым без
mock_hook Interface (термины — из `CONTEXT.md`):

- `utils/merge_key.py` — всё про нормализацию **Merge Key** и **Inferred Schema**.
- `utils/merge_planner.py` — построение индекса существующих строк и **Merge Plan**.

`_execute_merge` становится тонким оркестратором: I/O + вызовы чистых функций.

## Context (from discovery)

- `utils/schema.py`
  - `normalize_merge_key`, `_SHEETS_EPOCH` — переезжают в `merge_key.py`.
  - `apply_schema_to_value`, `format_value_for_write`, `validate_schema`,
    `apply_schema_to_row` — **остаются** (общая конвертация типов; `read.py`
    импортирует `apply_schema_to_row`, `validate_schema`).
  - `infer_date_key_schema` — добавлена PR1; переезжает в `merge_key.py`.
- `operators/write.py` (состояние **после** PR1, на котором базируется этот план)
  - `_execute_merge` — инлайн priority-логики `key_schema` (резолвится до чтения
    key_range, см. PR1), условный выбор `date_time_render_option`
    (`SERIAL_NUMBER` только если `normalize_merge_key_format=True` и
    `key_schema.get("type") == "date"`, иначе `FORMATTED_STRING` — это
    поведение **обязательно сохранить**, не возвращаться к безусловному
    `SERIAL_NUMBER` из первой версии PR1), построение `existing_index`,
    вычисление `delete_ops`/`append_rows` + сортировка.
  - `_group_contiguous` (`@staticmethod`, ~строка 571) — используется только
    внутри delete_ops-вычисления. Тестируется в
    `TestNonContiguousDeletion` (`tests/test_operators/test_write.py:898-948`):
    **4 теста** напрямую вызывают `_group_contiguous` (чистые, без mock_hook) —
    они переезжают; **5-й тест** в этом же классе,
    `test_non_contiguous_rows_deleted_separately` (строки 915-948), — полноценный
    `op.execute()`-тест с mock_hook, проверяющий поведение на уровне оператора
    (что несмежные строки одного ключа удаляются отдельными операциями, не
    затрагивая чужие строки между ними). ⚠️ Этот тест **должен остаться** в
    `test_write.py` — он не дублируется чистыми тестами `_group_contiguous` в
    `merge_planner.py` (та проверяет только сегментацию номеров строк, а не
    итоговые API-вызовы и итоговый `result["deleted"]`/`result["appended"]`).
  - `_normalize_sheet_key` — уже удалён в PR1.
  - импорт `normalize_merge_key` из `utils.schema` (строка 16) — после PR1
    также `infer_date_key_schema`; обновляется на `utils.merge_key`.
- `tests/test_utils/test_schema.py`
  - `TestNormalizeMergeKey` (строки 329-439) — переезжает в `test_merge_key.py`.
  - ⚠️ два теста в нём (`test_unparseable_key_returns_raw_with_warning:423`,
    `test_extended_false_unparseable_returns_raw_with_warning:431`) фильтруют
    caplog по `logger="airflow_provider_google_sheets.utils.schema"` — после
    переезда `normalize_merge_key` логгер станет `...utils.merge_key`, имя в
    caplog **обязательно** обновить, иначе тесты упадут (пустой `caplog.records`).
  - импорты `normalize_merge_key` (строка 14) и `import logging` (строка 3) —
    после выноса класса становятся неиспользуемыми в `test_schema.py`, убрать.
  - тесты `infer_date_key_schema`, добавленные PR1 в `test_schema.py`, тоже
    переезжают в `test_merge_key.py`.
- Зависимости направлены строго в одну сторону: `merge_key.py` → `schema.py`;
  `merge_planner.py` → `merge_key.py`; `write.py` → оба. `schema.py` ничего не
  знает про merge (нет обратных/циклических импортов).
- `CONTEXT.md` — глоссарий, включая **Merge Plan**. Имена новых функций/модулей
  должны совпадать с тем, что там описано.

## Development Approach

- **Testing approach**: Regular (перенос/код → прогон тестов в рамках задачи).
- **Поведение не меняется** — это pure refactor. Реализации переносимых функций
  (`normalize_merge_key`, `_group_contiguous`, тело построения индекса и
  планирования) копируются как есть, без правок логики.
- Новые функции — чистые (без `self`, без side-effects). Логирование факта
  срабатывания Inferred Schema остаётся в `write.py`, не уезжает в `merge_key.py`.
- После каждой задачи — полный прогон затронутых тестов; не двигаться дальше с
  красным.

## Testing Strategy

- `utils/merge_key.py` → `tests/test_utils/test_merge_key.py` (без mock_hook).
- `utils/merge_planner.py` → `tests/test_utils/test_merge_planner.py` (без
  mock_hook).
- Существующие операторные/поведенческие тесты в `test_write.py` остаются и
  должны проходить без изменений, включая `test_non_contiguous_rows_deleted_separately`
  (кроме 4 чистых `_group_contiguous`-тестов, которые переезжают).
- E2E/UI тестов в проекте нет — не применимо.

## Solution Overview

**`utils/merge_key.py`** (переезд + новая priority-функция):

- `_SHEETS_EPOCH`, `normalize_merge_key` — перенос из `schema.py` без изменений.
- `_ISO_DATE_RE`, `infer_date_key_schema` — перенос из `schema.py` (добавлены
  в PR1) без изменений.
- `resolve_merge_key_schema(explicit_key_schema: dict | None, incoming_values:
  list[str], *, infer: bool) -> dict | None` — **новая** чистая priority-функция,
  инкапсулирует логику, инлайненную в `_execute_merge` в PR1: явная Schema
  (даже формально не про даты) всегда побеждает; при её отсутствии и `infer=True`
  — `infer_date_key_schema`; иначе `None`. Без логирования (решает `write.py`).

**`utils/merge_planner.py`** (вынос чистой логики из `_execute_merge`):

- `MergePlan` (`NamedTuple`: `delete_ops: list[dict]`, `append_rows: list[list]`)
  — домен-концепт из `CONTEXT.md`.
- `build_existing_key_index(existing_keys_raw, *, has_headers, table_start_row,
  key_schema, extended) -> dict[str, list[int]]` — логика идентична текущему
  циклу построения `existing_index`; импортирует `normalize_merge_key`.
- `plan_merge_operations(existing_index, incoming_groups) -> MergePlan` —
  логика идентична текущему вычислению `delete_ops`/`append_rows`, плюс
  сортировка `delete_ops` по убыванию `row_num` **внутри** функции (раньше
  была на вызывающей стороне; перенос безопасен — `total_deleted` и
  `insert_start` от порядка не зависят, проверено). Инвариант "удалять снизу
  вверх" становится частью контракта функции.
- `_group_contiguous` — перенос из `write.py` как приватный helper.

**`_execute_merge`** — тонкий оркестратор: вызывает `resolve_merge_key_schema`
+ логирует при авто-детекте, **затем** (порядок важен, см. PR1) вычисляет
`use_serial_number` и читает key_range с условным render option (I/O), затем
`build_existing_key_index`, `plan_merge_operations`, затем выполняет
`plan.delete_ops`/`plan.append_rows` против Sheets API (I/O). Выбор render
option (`SERIAL_NUMBER` только для `type="date"` + флаг включён, иначе
`FORMATTED_STRING`) остаётся в `write.py` — это деталь конкретного Sheets API
вызова, а не часть merge-key normalization Module; `resolve_merge_key_schema`
отдаёт только `key_schema`, не знает про `dateTimeRenderOption`.
`incoming_groups`, вычисление `total_deleted`/`insert_start`, append с
очисткой форматирования — остаются в `write.py` (это про API/позиционирование,
не чистая логика планирования).

## Technical Details

### `utils/merge_key.py`

> ⚠️ Сниппет ниже иллюстративный, не копировать буквально. `normalize_merge_key`
> в реальности использует `datetime`, `timedelta` (decode serial-числа,
> `isinstance` проверки) и перехватывает `GoogleSheetsDataError` — перенести
> нужно полный набор импортов из текущего `schema.py`
> (`from datetime import date, datetime, timedelta`,
> `from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError`),
> не только `date`. Также не забыть `import logging` для `logger =
> logging.getLogger(__name__)`.

```python
import logging
import re
from datetime import date, datetime, timedelta

from airflow_provider_google_sheets.exceptions import GoogleSheetsDataError
from airflow_provider_google_sheets.utils.schema import (
    apply_schema_to_value, format_value_for_write,
)

logger = logging.getLogger(__name__)
_SHEETS_EPOCH = date(1899, 12, 30)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def infer_date_key_schema(values: list[str]) -> dict | None: ...   # из schema.py
def resolve_merge_key_schema(explicit_key_schema, incoming_values, *, infer) -> dict | None:
    if explicit_key_schema is not None:
        return explicit_key_schema
    if not infer:
        return None
    return infer_date_key_schema(incoming_values)
def normalize_merge_key(raw, key_schema, extended=True) -> str: ...  # из schema.py, со всеми импортами выше
```

### `utils/merge_planner.py`

```python
from collections import defaultdict
from typing import Any, NamedTuple
from airflow_provider_google_sheets.utils.merge_key import normalize_merge_key

class MergePlan(NamedTuple):
    delete_ops: list[dict]
    append_rows: list[list[Any]]

def build_existing_key_index(existing_keys_raw, *, has_headers, table_start_row,
                             key_schema, extended) -> dict[str, list[int]]: ...
def plan_merge_operations(existing_index, incoming_groups) -> MergePlan: ...
def _group_contiguous(rows: list[int]) -> list[tuple[int, int]]: ...  # из write.py
```

### `_execute_merge` после рефакторинга (фрагмент)

Порядок (резолв `key_schema` → выбор render option → чтение) сохраняется в
точности как закреплено в PR1 — это не пересматривается в этом плане, только
заменяется на вызовы новых модулей:

```python
explicit_key_schema = (self.schema or {}).get(self.merge_key)
incoming_key_strs = [
    str(r[key_col_idx]) for r in rows
    if key_col_idx < len(r) and r[key_col_idx] not in (None, "")
]
key_schema = resolve_merge_key_schema(
    explicit_key_schema, incoming_key_strs, infer=self.normalize_merge_key_format
)
if explicit_key_schema is None and key_schema is not None:
    logger.info("merge_key '%s' looks like a date column (no schema provided) — "
                "applying automatic date normalization", self.merge_key)

# Выбор render option остаётся в write.py (Sheets API деталь, не merge-key
# normalization) — та же логика, что и в PR1, без изменений.
use_serial_number = (
    self.normalize_merge_key_format
    and key_schema is not None
    and key_schema.get("type") == "date"
)
existing_keys_raw = hook.get_values(
    self.spreadsheet_id, key_range,
    date_time_render_option="SERIAL_NUMBER" if use_serial_number else "FORMATTED_STRING",
)
# ... запись headers при пустом листе — без изменений

existing_index = build_existing_key_index(
    existing_keys_raw, has_headers=self.has_headers,
    table_start_row=table_start_row, key_schema=key_schema,
    extended=self.normalize_merge_key_format,
)
incoming_groups = ...  # без изменений, остаётся в write.py
sheet_id = self._get_sheet_id(hook)
plan = plan_merge_operations(existing_index, incoming_groups)
# дальше выполнение plan.delete_ops / plan.append_rows — как и раньше
```

## Implementation Steps

### Task 1: `utils/merge_key.py` + перенос тестов

**Files:**
- Create: `airflow_provider_google_sheets/utils/merge_key.py`
- Create: `tests/test_utils/test_merge_key.py`
- Modify: `airflow_provider_google_sheets/utils/schema.py`
- Modify: `tests/test_utils/test_schema.py`
- Modify: `CHANGELOG.md` (исключение из "без CHANGELOG" — logger namespace)

- [x] создать `merge_key.py`: перенести `_SHEETS_EPOCH`, `normalize_merge_key`,
  `_ISO_DATE_RE`, `infer_date_key_schema` из `schema.py` без изменений
  реализации; импортировать `apply_schema_to_value`, `format_value_for_write`
  из `utils.schema`
- [x] добавить `resolve_merge_key_schema(explicit_key_schema, incoming_values,
  *, infer)` по Technical Details (без логирования)
- [x] убрать перенесённое из `schema.py`; проверить, что не осталось мёртвых
  импортов (`timedelta` и т.п. — если использовались только в
  `normalize_merge_key`)
- [x] перенести `TestNormalizeMergeKey` (строки 329-439) и тесты
  `infer_date_key_schema` из `test_schema.py` в `test_merge_key.py`; импорт →
  `from airflow_provider_google_sheets.utils.merge_key import ...`
- [x] ⚠️ в двух перенесённых caplog-тестах заменить
  `logger="airflow_provider_google_sheets.utils.schema"` →
  `"airflow_provider_google_sheets.utils.merge_key"`
- [x] ⚠️ задокументировать смену logger namespace как явное, осознанное
  исключение из "без изменения наблюдаемого поведения": добавить строку в
  `CHANGELOG.md` (например, под `## v0.12.0` или как примечание к записи
  PR1-фикса, в зависимости от того, в каком релизе фактически выйдет PR2) —
  "internal: merge-key warning/info logs now use logger
  `airflow_provider_google_sheets.utils.merge_key` instead of `...utils.schema`"
- [x] убрать из `test_schema.py` неиспользуемые `normalize_merge_key` (строка
  14) и `import logging` (строка 3), если они больше нигде не нужны
- [x] написать тесты `resolve_merge_key_schema`: явная schema (включая
  "пустую" `{"type": "str"}`) всегда побеждает и не вызывает инференс;
  `infer=False` → инференс не вызывается; `infer=True` + нет явной schema +
  ISO-даты → Inferred Schema; `infer=True` + нет явной schema + не-ISO → `None`
- [x] обновить импорт в `write.py`: `normalize_merge_key`,
  `resolve_merge_key_schema` из `utils.merge_key` (вместо `utils.schema`)
- [x] запустить: `python -m pytest tests/test_utils/test_merge_key.py
  tests/test_utils/test_schema.py tests/test_operators/test_write.py -x -q`

### Task 2: `utils/merge_planner.py` + перенос тестов

**Files:**
- Create: `airflow_provider_google_sheets/utils/merge_planner.py`
- Create: `tests/test_utils/test_merge_planner.py`
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [x] создать `merge_planner.py`: `MergePlan` (`NamedTuple`),
  `build_existing_key_index`, `plan_merge_operations`, `_group_contiguous`
  (перенос из `write.py` без изменений реализации); импорт
  `normalize_merge_key` из `utils.merge_key`
- [x] в `plan_merge_operations` — сортировка `delete_ops` по убыванию
  `row_num` внутри функции
- [x] удалить `_group_contiguous` из `write.py`
- [x] расщепить `TestNonContiguousDeletion` (`test_write.py:898-948`):
  переносить в `test_merge_planner.py` **только** 4 метода, вызывающие
  `_group_contiguous` напрямую (`test_group_contiguous_basic`,
  `_all_sequential`, `_single`, `_empty`); заменить
  `GoogleSheetsWriteOperator._group_contiguous` на прямой импорт
  `merge_planner._group_contiguous`
- [x] ⚠️ `test_non_contiguous_rows_deleted_separately` (строки 915-948) —
  **оставить в `test_write.py`**, не переносить. Это `op.execute()`-тест на
  уровне оператора (проверяет реальные API-вызовы `batch_update` и
  `result["deleted"]`/`["appended"]`), он не дублируется чистыми тестами
  `_group_contiguous`. После переноса 4 методов выше класс
  `TestNonContiguousDeletion` в `test_write.py` останется с этим одним тестом —
  можно оставить как есть или переименовать класс, не обязательно
- [x] написать тесты `build_existing_key_index`: `has_headers=True/False`;
  `key_schema=None` (raw as-is); `key_schema` для дат (нормализация serial-чисел)
- [x] написать тесты `plan_merge_operations`: ключ в обоих → delete+append;
  только incoming → только append; только existing → не трогаем; несмежные
  существующие строки одного ключа → корректные сегменты; `delete_ops`
  отсортированы по убыванию `row_num`
- [x] запустить: `python -m pytest tests/test_utils/test_merge_planner.py -x -q`

### Task 3: Рефакторинг `_execute_merge` в оркестратор

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] импортировать `MergePlan`, `build_existing_key_index`,
  `plan_merge_operations` из `utils.merge_planner`
- [x] заменить инлайн-вычисление `key_schema` + цикл построения
  `existing_index` на `resolve_merge_key_schema` + `build_existing_key_index`
  (с `logger.info` при `explicit_key_schema is None and key_schema is not None`)
- [x] заменить инлайн-вычисление `delete_ops`/`append_rows` (+ сортировку) на
  один вызов `plan_merge_operations`; использовать `plan.delete_ops`/
  `plan.append_rows` дальше
- [x] убедиться, что `total_deleted`/`insert_start`/append-часть остались без
  изменений в поведении
- [x] существующие тесты `test_write.py` должны проходить без правок (кроме 4
  методов `_group_contiguous`, уже перенесённых в Task 2 —
  `test_non_contiguous_rows_deleted_separately` остаётся и должен пройти как
  есть, без изменений)
- [x] запустить: `python -m pytest tests/test_operators/test_write.py -x -q`

### Task 4: Verify + финализация

- [x] grep: нет импортов `normalize_merge_key`/`infer_date_key_schema` из
  `utils.schema` нигде (всё через `utils.merge_key`)
- [x] grep: нет циклических импортов (`schema.py` не импортит из `merge_key.py`/
  `merge_planner.py`)
- [x] сверить имена функций/модулей с `CONTEXT.md` (Merge Key, Inferred Schema,
  Merge Plan)
- [x] полный прогон: `python -m pytest tests/ -x -q`
- [ ] переместить этот файл в `docs/plans/completed/`

## Post-Completion

**Manual verification**: не требуется отдельно — поведение не меняется,
покрывается прогоном полного набора тестов (то же зелёное состояние до и после).
