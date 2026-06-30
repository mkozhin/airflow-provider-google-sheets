# Read Operator: filter_column / filter_value + UniqueValuesOperator

## Overview

Добавить в `GoogleSheetsReadOperator` возможность включающей фильтрации строк по значению
конкретного столбца, а также новый `GoogleSheetsUniqueValuesOperator`, который возвращает
`list[str]` уникальных значений колонки и предназначен для динамического маппинга тасков.

**Проблема:** сейчас оператор чтения читает все данные, либо пропускает/останавливает строки
(row_skip / row_stop). Нет способа явно указать «включить только строки, где column = value».

**Решение:**
- `filter_column` + `filter_value` в ReadOperator — include-фильтр по значению колонки
- `GoogleSheetsUniqueValuesOperator` — читает лист напрямую и извлекает уникальные значения
  для fan-out маппинга (в отличие от `ExtractPartitionsOperator`, который работает с уже
  загруженными данными в памяти и не делает API-вызовов)

**Ключевой инвариант:** `filter_column` и `column` (UniqueValuesOperator) всегда задаются по
**обработанному** имени заголовка — после transliteration / sanitize / lowercase / column_mapping.
Оба оператора применяют одинаковую обработку заголовков, поэтому имена колонок между ними
всегда согласованы, если параметры обработки заголовков совпадают.

**Интеграция с Airflow:**
```python
# "Город" в таблице + column_mapping={"Город": "city"}
# → column/filter_column указывается как "city"
cities = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities", column="city",
    column_mapping={"Город": "city"}, ...
)
read = GoogleSheetsReadOperator.partial(
    task_id="read_by_city", filter_column="city",
    column_mapping={"Город": "city"}, ...
).expand(filter_value=cities.output)
```

## Context (from discovery)

- `airflow_provider_google_sheets/operators/read.py` — GoogleSheetsReadOperator (template_fields, _read_chunks, execute + 4 output methods)
- `airflow_provider_google_sheets/operators/manage.py` — управляющие операторы; авто-обнаруживается через `get_provider_info()` — отдельный __init__.py не нужен
- `airflow_provider_google_sheets/utils/row_filter.py` — normalize_conditions, matches_any
- `airflow_provider_google_sheets/hooks/google_sheets.py` — GoogleSheetsHook (get_values)
- `airflow_provider_google_sheets/utils/headers.py` — process_headers
- `tests/test_operators/test_read.py` — тесты ReadOperator
- `tests/test_operators/test_manage.py` — тесты управляющих операторов

## Development Approach

- **testing approach**: Regular (код → тесты)
- завершить каждую задачу полностью перед переходом к следующей
- **каждая задача должна включать тесты**
- все тесты должны проходить перед стартом следующей задачи

## Testing Strategy

- **unit tests**: для каждого таска
- покрыть: успешные сценарии, граничные случаи, ошибки валидации

## Progress Tracking

- `[x]` — выполнено
- `➕` — добавлено в процессе
- `⚠️` — блокер

## What Goes Where

- **Implementation Steps**: изменения кода и тесты
- **Post-Completion**: ручная проверка, обновление CHANGELOG

---

## Implementation Steps

### Task 1: filter_column + filter_value в GoogleSheetsReadOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/read.py`

- [x] добавить `filter_column: str | None = None` и `filter_value: str | list[str] | None = None` в `__init__`
- [x] добавить оба параметра в `template_fields`
- [x] в `execute()` нормализовать `filter_value` в `set[str]` (если задан); валидация: если задан один параметр без другого → `ValueError`
- [x] добавить `filter_values_set: set[str] | None = None` в сигнатуру `_read_chunks`; обновить все 4 вызова (_stream_to_csv, _stream_to_json, _stream_to_jsonl, _read_to_xcom), передавая `filter_values_set`
- [x] в `_read_chunks` применить include-фильтр **после** `row_stop` и **после** `row_skip` (порядок: stop → skip → include_filter); если `filter_column` отсутствует в заголовках — выдать `logger.warning` и вернуть пустой список строк
- [x] `filter_column` должен совпадать с именем колонки **после** обработки заголовков (transliterate / sanitize / lowercase / column_mapping применяются в `execute()` до `_read_chunks`); это гарантирует согласованность с данными в `rows_to_dicts` и совместимость с `column_mapping`

### Task 2: тесты для filter_column / filter_value

**Files:**
- Modify: `tests/test_operators/test_read.py`

- [x] тест: `filter_column` + одно значение строкой — строки с другим значением исключаются
- [x] тест: `filter_column` + список значений (OR-логика) — включаются строки с любым из значений
- [x] тест: `filter_column` задан без `filter_value` → `ValueError`
- [x] тест: `filter_value` задан без `filter_column` → `ValueError`
- [x] тест: `filter_column` не существует в заголовках (в т.ч. если пользователь указал сырое имя вместо обработанного) — результат пустой, выдаётся warning
- [x] тест: `filter_column` работает корректно при `column_mapping` — имя колонки берётся после маппинга
- [x] тест: `filter_column` + `filter_value` совместно с `row_skip` — оба фильтра применяются (порядок: stop → skip → filter)
- [x] тест: `filter_column` + `row_stop` — строка-маркер остановки обрабатывается до include-фильтра (stop срабатывает даже если маркер-строка не прошла бы фильтр)
- [x] тест: `output_type="csv"` с include-фильтром — фильтр работает при файловом выводе
- [x] запустить тесты — все должны проходить

### Task 3: GoogleSheetsUniqueValuesOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`

- [x] добавить класс `GoogleSheetsUniqueValuesOperator(BaseOperator)` в конец файла
- [x] параметры: `column`, `exclude_values`, `spreadsheet_id`, `sheet_name`, `cell_range`, `chunk_size`, `gcp_conn_id`, `has_headers`, `transliterate_headers`, `sanitize_headers`, `lowercase_headers`, `column_mapping`
- [x] `template_fields`: `spreadsheet_id`, `sheet_name`, `cell_range`, `column`
- [x] в `execute`: читать заголовки через hook, обработать через `process_headers` (с теми же параметрами что у ReadOperator), найти индекс `column` в обработанных заголовках; `column` задаётся пользователем по имени после обработки — это должно быть явно в docstring; если `column` не найден → `ValueError`
- [x] читать лист чанками через `hook.get_values`, собирать уникальные значения в `dict` (сохранение порядка), применять `exclude_values`
- [x] вернуть `list[str]`; логировать количество уникальных значений

### Task 4: тесты для GoogleSheetsUniqueValuesOperator

**Files:**
- Modify: `tests/test_operators/test_manage.py`

- [x] тест: базовый сценарий — возвращает уникальные значения в порядке первого вхождения
- [x] тест: `exclude_values` — указанные значения отсутствуют в результате
- [x] тест: пустая строка `""` в `exclude_values` — пустые ячейки исключаются
- [x] тест: колонка не найдена (в т.ч. если `column` задан по сырому имени вместо обработанного) → `ValueError`
- [x] тест: `column` совпадает с именем после `column_mapping` — значения извлекаются корректно
- [x] тест: несколько чанков — дубликаты между чанками не попадают в результат
- [x] запустить тесты — все должны проходить

### Task 5: Проверка критериев приёмки

- [x] убедиться что `filter_column` / `filter_value` работают с Jinja-шаблонами (в `template_fields`)
- [x] убедиться что паттерн `.partial(...).expand(filter_value=op.output)` корректен концептуально
- [x] запустить полный набор тестов: `pytest tests/`
- [x] проверить что существующие тесты не сломаны

### Task 6: Документация — docstrings + README + CHANGELOG

**Files:**
- Modify: `airflow_provider_google_sheets/operators/read.py`
- Modify: `airflow_provider_google_sheets/operators/manage.py`
- Modify: `readme.md`
- Modify: `readme_ru.md`
- Modify: `CHANGELOG.md`

- [x] обновить docstring `GoogleSheetsReadOperator` — описать `filter_column`, `filter_value`; явно указать что `filter_column` задаётся по **обработанному** имени заголовка (после transliterate/sanitize/lowercase/column_mapping)
- [x] написать docstring для `GoogleSheetsUniqueValuesOperator` — описать все параметры; явно указать что `column` задаётся по обработанному имени
- [x] в `readme.md` добавить раздел с примерами: фильтрация по колонке (статичное значение, список, XCom), динамический маппинг через `UniqueValuesOperator` + `expand(filter_value=...)`
- [x] в `readme_ru.md` добавить тот же раздел на русском
- [x] в `CHANGELOG.md` добавить блок `## v0.9.0` в начало с описанием новых возможностей

### Task 7: Релиз — тег + push

- [x] убедиться что все тесты проходят: `pytest tests/`
- [x] сделать коммит всех изменений
- [x] создать git-тег: `git tag v0.9.0`
- [x] отправить коммит и тег на сервер: `git push && git push --tags`

### Task 8: [Final] Завершение плана

- [x] переместить план в `docs/plans/completed/`

---

## Technical Details

**Нормализация filter_values_set в execute():**
```python
filter_values_set: set[str] | None = None
if self.filter_column is not None and self.filter_value is not None:
    vals = self.filter_value if isinstance(self.filter_value, list) else [self.filter_value]
    filter_values_set = {str(v) for v in vals}
elif (self.filter_column is None) != (self.filter_value is None):
    raise ValueError("filter_column and filter_value must both be set or both be None")
```

**Порядок применения фильтров в _read_chunks (stop → skip → include_filter):**
```python
# 1. row_stop (существующий код)
# 2. row_skip (существующий код)
# 3. include filter — применяется последним
if filter_values_set is not None and headers:
    if self.filter_column not in headers:
        logger.warning("filter_column '%s' not found in headers %s", self.filter_column, headers)
        rows = []
    else:
        col_idx = headers.index(self.filter_column)
        rows = [r for r in rows if (str(r[col_idx]) if col_idx < len(r) else "") in filter_values_set]
```

**UniqueValuesOperator — сбор уникальных значений:**
```python
exclude_set = set(self.exclude_values or [])
seen: dict[str, None] = {}  # dict как ordered set (Python 3.7+)
for chunk in chunks:
    for row in chunk:
        val = str(row[col_idx]) if col_idx < len(row) else ""
        if val not in seen and val not in exclude_set:
            seen[val] = None
return list(seen.keys())
```

## Post-Completion

**Ручная проверка:**
- протестировать паттерн динамического маппинга в реальном DAG с настоящей таблицей
- убедиться что `expand(filter_value=...)` работает с Airflow 2.9+

**CHANGELOG:**
- добавить запись о новых параметрах и операторе в CHANGELOG / README
