# Partitioned Write: автоматическое разделение данных по листам

## Overview

Реализация механизма автоматического fan-out данных по листам Google Sheets на основе значений в
указанной колонке (партиции). Пользователь передаёт полный датасет, указывает колонку-партицию и
шаблон имени листа — оператор создаёт недостающие листы и записывает данные каждой партиции на
соответствующий лист.

**Проблема:** сейчас `GoogleSheetsWriteOperator` пишет только в один конкретный лист.
Для распределения данных по листам пользователю нужно вручную разбивать данные и создавать DAG с
явным перечислением задач.

**Решение:** два дополнения к провайдеру:
1. Новый оператор `GoogleSheetsExtractPartitionsOperator` — извлекает уникальные значения
   партиции и формирует список `[{"sheet_name": "...", "partition_value": "..."}]` для
   Airflow dynamic task mapping (`expand_kwargs`).
2. Расширение `GoogleSheetsWriteOperator` параметрами `partition_by`, `partition_value`
   и `create_sheet_if_missing`.

**Паттерн использования в DAG:**
```python
# fetch_data должен возвращать данные в формате совместимом с normalize_input_data
# (list[dict], list[list], путь к файлу и т.п.)

partitions = GoogleSheetsExtractPartitionsOperator(
    task_id="get_partitions",
    data_xcom_task_id="fetch_data",
    partition_column="period",
    sheet_name_template="Отчёт {value}",   # опционально, default = "{value}"
)

write = GoogleSheetsWriteOperator.partial(
    task_id="write_to_sheet",
    spreadsheet_id="...",
    data_xcom_task_id="fetch_data",
    partition_by="period",
    create_sheet_if_missing=True,
    write_mode="overwrite",
).expand_kwargs(partitions.output)
```

## Context (from discovery)

- **Основные файлы:**
  - `airflow_provider_google_sheets/operators/write.py` — расширяем
  - `airflow_provider_google_sheets/operators/manage.py` — добавляем новый оператор
  - `airflow_provider_google_sheets/hooks/google_sheets.py` — хук, уже умеет `create_sheet`;
    `get_sheet_properties` бросает `GoogleSheetsAPIError` если лист не найден;
    `get_spreadsheet_metadata` возвращает сырой список листов без исключений
  - `airflow_provider_google_sheets/utils/data_formats.py` — `normalize_input_data`
- **Тесты:**
  - `tests/test_operators/test_write.py` — расширяем
  - `tests/test_operators/test_manage.py` — расширяем
- **`operators/__init__.py`** — пустой, операторы импортируются напрямую из модулей

## Development Approach

- **Тестирование:** Regular (сначала код, потом тесты)
- Завершать каждую задачу полностью перед переходом к следующей
- Каждая задача включает тесты как обязательную часть
- Все тесты должны проходить перед началом следующей задачи

## Progress Tracking

- Выполненные пункты отмечать `[x]` сразу по завершении
- Новые задачи добавлять с префиксом ➕
- Блокеры отмечать префиксом ⚠️

## Implementation Steps

---

### Task 1: `GoogleSheetsExtractPartitionsOperator` — новый оператор

Оператор принимает данные (inline или из XCom), извлекает уникальные значения указанной колонки,
применяет шаблон имени листа и возвращает `list[dict]` для `expand_kwargs`.
Не требует соединения с Google Sheets API — работает только с данными в памяти.

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`
- Modify: `tests/test_operators/test_manage.py`

- [x] добавить импорт `normalize_input_data` из `utils.data_formats` в `manage.py`
- [x] добавить класс `GoogleSheetsExtractPartitionsOperator(BaseOperator)` в `manage.py`
- [x] параметры: `partition_column: str`, `sheet_name_template: str = "{value}"`,
  `data: Any = None`, `data_xcom_task_id: str | None = None`,
  `data_xcom_key: str = "return_value"`, `has_headers: bool = True`
- [x] `template_fields` = `("partition_column", "sheet_name_template", "data_xcom_task_id", "data_xcom_key")`
- [x] в `execute`: загрузить данные через `normalize_input_data`; если `has_headers=False` —
  поднять `ValueError` с понятным сообщением (поиск колонки по имени требует заголовков)
- [x] найти индекс `partition_column` в headers (если не найдена — `ValueError`);
  собрать уникальные значения сохраняя порядок первого появления;
  каждое значение привести к `str()` для консистентности при числовых данных;
  применить шаблон: `sheet_name_template.format(value=v)`;
  вернуть `[{"sheet_name": "...", "partition_value": "..."}, ...]`
- [x] написать тесты:
  - inline `list[dict]` с тремя партициями, шаблон по умолчанию → имена листов = значения
  - кастомный шаблон `"Отчёт {value}"` → имена содержат префикс
  - числовые значения партиции (int, float) → `partition_value` всегда строка
  - XCom-данные (mock `ti.xcom_pull`)
  - пустые данные → пустой список
  - `partition_column` не найдена в headers → `ValueError`
  - `has_headers=False` → `ValueError`
  - дублирующиеся значения → дедупликация с сохранением порядка
- [x] запустить тесты — должны проходить

---

### Task 2: `create_sheet_if_missing` в `GoogleSheetsWriteOperator`

Если лист с указанным именем не существует — создать его перед записью.
При `False` (default) — поведение не меняется (ошибка от API пробрасывается как есть).

**Важно для реализации `_ensure_sheet_exists`:** нельзя использовать `hook.get_sheet_properties()`
— он бросает `GoogleSheetsAPIError` при отсутствии листа, что не позволит отличить "нет листа"
от других ошибок. Использовать `hook.get_spreadsheet_metadata()` и итерировать по списку листов.

**Важно для параллельного выполнения:** несколько mapped tasks могут одновременно проверить
отсутствие листа и попытаться его создать. Второй вызов `create_sheet()` вернёт HTTP 400
"already exists". Это нужно перехватывать как no-op.

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [x] добавить параметр `create_sheet_if_missing: bool = False` в `__init__`
- [x] реализовать `_ensure_sheet_exists(hook, spreadsheet_id, sheet_name)`:
  - вызвать `hook.get_spreadsheet_metadata(spreadsheet_id)`
  - проверить наличие листа среди `meta["sheets"]` по `properties["title"]`
  - если лист отсутствует → вызвать `hook.create_sheet(spreadsheet_id, sheet_name)`
  - перехватить `HttpError` с кодом 400 (лист уже существует — race condition) → no-op
- [x] в `execute`: вызывать `_ensure_sheet_exists` если `self.sheet_name` задан
  и `self.create_sheet_if_missing=True`, до вызова write-метода
- [x] написать тесты:
  - лист уже существует → `create_sheet` не вызывается
  - лист отсутствует → `create_sheet` вызывается один раз
  - race condition: `create_sheet` бросает `HttpError(400)` → поглощается, не падает
  - `create_sheet_if_missing=False` (default) → `_ensure_sheet_exists` не вызывается
  - `sheet_name=None` → `_ensure_sheet_exists` не вызывается
- [x] запустить тесты — должны проходить

---

### Task 3: `partition_by` и `partition_value` в `GoogleSheetsWriteOperator`

Фильтрация данных внутри оператора: оставить только строки, где значение в колонке
`partition_by` (строковое) совпадает с `partition_value`.

**Взаимодействие с merge mode:** `partition_by` несовместим с `write_mode="merge"` —
данные фильтруются до начала merge, что меняет ключевые предположения (merge сравнивает
с данными на листе по merge_key, а не по partition). Валидировать явно.

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [x] добавить параметры `partition_by: str | None = None` и
  `partition_value: str | None = None` в `__init__`; добавить оба в `template_fields`
- [x] в начале `execute`: если `partition_by` задан и `write_mode in ("merge", "smart_merge")`
  → поднимать `ValueError("partition_by is not supported with merge mode")`
- [x] если `partition_by` задан, но `partition_value` не задан → `ValueError`
- [x] реализовать `_apply_partition(headers, rows)`:
  - если `partition_by` не задан → возвращать `rows` без изменений
  - найти индекс `partition_by` в headers → если нет → `ValueError`
  - отфильтровать: `[r for r in rows if str(r[idx]) == self.partition_value]`
- [x] вызывать `_apply_partition` в `execute` после `_format_rows`, перед write-методом
- [x] написать тесты:
  - фильтрация: из 3 партиций остаётся только 1 нужная
  - числовые значения в колонке (int) корректно матчатся с `partition_value` строкой
  - нет совпадений → пустой `rows` (передаётся в write-метод, поведение зависит от режима)
  - `partition_by` не найдена в headers → `ValueError`
  - `partition_by=None` → данные не меняются
  - `partition_by` задан, `partition_value=None` → `ValueError`
  - `partition_by` + `write_mode="merge"` → `ValueError`
- [x] запустить тесты — должны проходить

---

### Task 4: Проверка приёмочных критериев

**Files:** нет изменений кода

- [x] проверить что `GoogleSheetsExtractPartitionsOperator` возвращает тип `list[dict]`
  с ключами `"sheet_name"` и `"partition_value"` (строки), в порядке первого появления
- [x] проверить что `create_sheet_if_missing=True` не падает при параллельном запуске
  (HTTP 400 поглощается)
- [x] проверить что фильтрация + `create_sheet_if_missing` работают совместно
- [x] проверить backward compatibility: существующие тесты не сломаны
- [x] запустить полный тест-сьют: `pytest tests/` — 379 passed

---

### Task 5: Обновить README примером partitioned write

**Files:**
- Modify: `README.md` и `readme_ru.md`
- Modify: `CHANGELOG.md` добавить версию 0.8.0 с кратким описанием
- Move: `docs/plans/20260325-partitioned-write.md` → `docs/plans/completed/`

- [ ] добавить раздел в `README.md` и `readme_ru.md` с примером DAG для partitioned write
- [ ] добавить в `CHANGELOG.md` пункты с новым функционалом и изменениями, что были сделаны
- [ ] перенести план в `docs/plans/completed/`

## Technical Details

**`GoogleSheetsExtractPartitionsOperator` — возвращаемый формат:**
```python
[
    {"sheet_name": "Отчёт 2026-01", "partition_value": "2026-01"},
    {"sheet_name": "Отчёт 2026-02", "partition_value": "2026-02"},
]
```
Значения `partition_value` всегда строки (`str()`), даже если в данных числа.

**`_ensure_sheet_exists` — правильная реализация:**
```python
def _ensure_sheet_exists(self, hook, spreadsheet_id, sheet_name):
    meta = hook.get_spreadsheet_metadata(spreadsheet_id)
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if sheet_name not in existing:
        try:
            hook.create_sheet(spreadsheet_id, sheet_name)
        except HttpError as e:
            if e.resp.status == 400:
                pass  # race condition: другой task уже создал лист
            else:
                raise
```

**Порядок операций в `execute` после изменений:**
```
_resolve_data → _format_rows → _apply_partition → _ensure_sheet_exists → write-method
```

**Совместимость:** все три новых параметра имеют значения по умолчанию,
не ломающие существующий код.

## Post-Completion

**Ручная проверка:**
- Запустить тестовый DAG с реальным spreadsheet, данными за 3 периода
- Убедиться что листы создаются с правильными именами
- Проверить что данные разбиты корректно (каждый лист содержит только свой период)
- Проверить повторный запуск (idempotency) в overwrite-режиме
- Проверить параллельное выполнение mapped tasks (race condition не вызывает ошибку)
