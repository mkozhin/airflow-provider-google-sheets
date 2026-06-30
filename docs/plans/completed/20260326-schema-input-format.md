# Добавить `input_format` в schema для раздельного парсинга и форматирования дат

## Overview

В schema колонки одно поле `format` используется и для **парсинга** входных данных, и для **записи** в лист.
Это создаёт проблему: если данные приходят в ISO-формате (`"2026-03-01"`), а лист хранит даты в
`"01.03.2026"` — `merge` не может совместить форматы. Строки не удаляются → новые добавляются → **дубликаты**.

**Решение:** добавить опциональное поле `input_format` в словарь schema для колонки.

- Если задан `input_format` — `apply_schema_to_value` использует его для парсинга входных строк.
- `format` продолжает отвечать только за запись (вывод) в лист.
- В `_format_rows` WriteOperator: колонки с `input_format` проходят полный пайплайн parse → format.
- В `_execute_merge`: нормализовать существующие ключи через схему (парсинг по `format` → вывод по `format`).

**Затрагивает только** типы `date` и `datetime`. Для `str`, `int`, `float`, `bool` — поведение не меняется.

**Acceptance criteria:**
- Merge с `input_format` в schema не создаёт дублей при разных форматах дат
- Данные записываются в лист в формате `format`, а не `input_format`
- Все существующие тесты проходят без изменений

## Context (from discovery)

- `airflow_provider_google_sheets/utils/schema.py`:
  - `apply_schema_to_value` (~line 58): `fmt = column_schema.get("format")` для `date`/`datetime`
  - `format_value_for_write` (~line 182): `fmt = column_schema.get("format")` — меняться не должна
  - `validate_schema` (~line 37): валидирует тип, не формат
- `airflow_provider_google_sheets/operators/write.py`:
  - `_format_rows` (~line 142): вызывает только `format_row_for_write` — НЕ парсит строки в Python-объекты
  - `_execute_merge` (~line 398): строит `existing_index` через `str(row[0])` — без нормализации ключей
  - Импорты на уровне модуля (~line 16): `from ...utils.schema import format_row_for_write`
- Тесты: `tests/test_utils/test_schema.py`, `tests/test_operators/test_write.py`

## Development Approach

- Подход: сначала код, затем тесты
- Изменения минимальны и локальны: два файла
- Обратная совместимость: `input_format` опциональный, без него поведение не меняется
- `input_format` — явный opt-in сигнал что строковые значения нужно парсить+форматировать

## Testing Strategy

- `tests/test_utils/test_schema.py` — unit-тесты `apply_schema_to_value` с `input_format`
- `tests/test_operators/test_write.py` — тест merge без дублей + тест форматирования при записи

## Progress Tracking

- Отмечать выполненные пункты `[x]` сразу по завершению
- Добавлять новые задачи с префиксом ➕
- Документировать блокеры с префиксом ⚠️

## Implementation Steps

### Task 1: Поддержка `input_format` в `apply_schema_to_value`

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [x] В `apply_schema_to_value`, строки `fmt = column_schema.get("format")` (только в этой функции),
  заменить на: `fmt = column_schema.get("input_format") or column_schema.get("format")`
  (`format_value_for_write` — не трогать, там `fmt = column_schema.get("format")` остаётся)
- [x] Добавить тест `test_date_input_format_differs_from_format`:
  ```python
  result = apply_schema_to_value(
      "2026-03-01",
      {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}
  )
  assert result == date(2026, 3, 1)
  ```
- [x] Добавить тест `test_datetime_input_format`:
  парсинг `"2026-03-01 12:00"` с `input_format="%Y-%m-%d %H:%M"` → `datetime(2026, 3, 1, 12, 0)`
- [x] Добавить тест `test_input_format_not_affects_format_value_for_write`:
  `format_value_for_write(date(2026, 3, 1), {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"})`
  → `"01.03.2026"` (использует только `format`)
- [x] Убедиться что существующий `test_date_with_format` продолжает проходить (без `input_format` — поведение прежнее)
- [x] Запустить: `pytest tests/test_utils/test_schema.py -v` — все зелёные

### Task 2: Parse+format пайплайн в `_format_rows` WriteOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

**Проблема:** `format_value_for_write` применяет `strftime` только к `date`/`datetime` объектам,
но не к строкам. Строка `"2026-03-01"` проходит через него как есть — `str(value)`.
Поэтому данные из JSONL не конвертируются в нужный формат при записи.

**Исправление:** в `_format_rows`, для колонок у которых в schema задан `input_format`,
применять полный пайплайн: `apply_schema_to_value` (парсит строку) → `format_value_for_write`
(форматирует Python-объект). Без `input_format` — поведение прежнее (только format).

- [x] Добавить `apply_schema_to_value` в module-level импорт на ~line 16:
  `from airflow_provider_google_sheets.utils.schema import format_row_for_write, apply_schema_to_value`
- [x] Изменить `_format_rows` — pre-process только колонки с `input_format`, затем делегировать
  в существующий `format_row_for_write` (не дублировать его логику):
  ```python
  def _format_rows(self, headers, rows):
      if not self.schema or not headers:
          return rows
      result = []
      for row in rows:
          # Pre-process columns with input_format: parse string → Python object
          preprocessed = list(row)
          for i, value in enumerate(row):
              if i < len(headers):
                  col_schema = (self.schema or {}).get(headers[i], {})
                  if col_schema.get("input_format"):
                      preprocessed[i] = apply_schema_to_value(value, col_schema)
          result.append(format_row_for_write(preprocessed, headers, self.schema))
      return result
  ```
  Это избегает дублирования логики `format_row_for_write` — только добавляет шаг pre-parse.
- [x] Добавить тест `test_format_rows_with_input_format` (прямой вызов `_format_rows`):
  - Operator с `schema={"date": {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}}`
  - Входящая строка: `[["2026-03-01", "val"]]` (строка ISO)
  - Ожидание: `[["01.03.2026", "val"]]` (сконвертирована в нужный формат)
- [x] Убедиться что `test_format_rows_without_input_format` тоже работает (регрессия):
  - Без `input_format` — поведение прежнее
- [x] Запустить существующие тесты: `pytest tests/test_operators/test_write.py -v` — все зелёные

### Task 3: Нормализация ключей merge через схему

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] Добавить private-метод класса `_normalize_sheet_key` (не nested function — следуем стилю
  существующих хелперов `_sheet_prefix`, `_get_sheet_id` и др.):
  ```python
  def _normalize_sheet_key(self, raw: str) -> str:
      """Normalize a key value read from the sheet to canonical write format."""
      key_schema = (self.schema or {}).get(self.merge_key)
      if not key_schema:
          return raw
      # Sheet keys were written using "format" — parse with "format", not "input_format"
      parse_schema = {k: v for k, v in key_schema.items() if k != "input_format"}
      try:
          parsed = apply_schema_to_value(raw, parse_schema)
          return format_value_for_write(parsed, key_schema)
      except (ValueError, TypeError, GoogleSheetsDataError):
          logger.warning("Could not normalize existing key %r via schema, using raw value", raw)
          return raw
  ```
  Применить при построении `existing_index` (~line 408):
  ```python
  key_val = self._normalize_sheet_key(str(row[0]))
  ```
  Добавить `GoogleSheetsDataError` в импорты `write.py` если его нет.
- [x] Добавить тест `test_merge_no_duplicates_with_input_format`:
  - Лист: строки с ключом `"01.03.2026"` и значением `"old"`
  - Входящие данные: `[{"date": "2026-03-01", "value": "new"}]` (строки, без Python-объектов)
  - `merge_key="date"`, `schema={"date": {"type": "date", "input_format": "%Y-%m-%d", "format": "%d.%m.%Y"}}`
  - Ожидания:
    - Старые строки удалены (`hook.batch_update` вызван)
    - Новые добавлены с `"01.03.2026"` в колонке date (не `"2026-03-01"`)
- [x] Запустить: `pytest tests/test_operators/test_write.py -k "merge" -v` — все зелёные

### Task 4: Финальная проверка

**Files:** —

- [x] Запустить полный тест-сьют: `pytest tests/ -v`
- [x] Убедиться что все тесты зелёные

### Task 5: Обновить документацию и переместить план

**Files:**
- Modify: `CHANGELOG.md`

- [ ] Добавить запись в `CHANGELOG.md`:
  `feat: add input_format to schema for separate parse/write date formats`
- [ ] Переместить план в `docs/plans/completed/`

## Technical Details

**Схема с `input_format` — пример использования:**
```python
schema = {
    "date": {
        "type": "date",
        "input_format": "%Y-%m-%d",   # как парсить входные данные (ISO из JSONL)
        "format": "%d.%m.%Y",          # как записывать в лист (русский формат)
    }
}
```

**Полный пайплайн для WriteOperator merge:**
```
Входящий ключ "2026-03-01" (строка из JSONL)
  ↓ _format_rows: apply_schema_to_value("2026-03-01", schema["date"])
    input_format="%Y-%m-%d" → date(2026, 3, 1)
  ↓ format_value_for_write(date(2026, 3, 1), schema["date"])
    format="%d.%m.%Y" → "01.03.2026"
  Итоговый ключ в incoming_groups: "01.03.2026"

Существующий ключ "01.03.2026" (из листа)
  ↓ _normalize_sheet_key: parse_schema без input_format
    apply_schema_to_value("01.03.2026", {"type": "date", "format": "%d.%m.%Y"})
    → date(2026, 3, 1)
  ↓ format_value_for_write → "01.03.2026"
  Итоговый ключ в existing_index: "01.03.2026"

Сравнение: "01.03.2026" == "01.03.2026" → СОВПАДЕНИЕ → дублей нет ✓
Запись в лист: "01.03.2026" (а не "2026-03-01") ✓
```

**Почему для нормализации ключей из листа НЕ используем `input_format`:**
`input_format` — формат ВХОДЯЩИХ данных. Ключи в листе были записаны с `format` → парсим через `format`.
При наличии `input_format` в parse_schema, `apply_schema_to_value` попытается распарсить `"01.03.2026"`
по `"%Y-%m-%d"` → ошибка → fallback на raw. Поэтому явно убираем `input_format` перед парсингом.

**Backwards compatibility:**
- Без `input_format` в схеме: пайплайн в `_format_rows` не меняется, `_normalize_sheet_key` не меняет поведение
- Без `schema` вообще: всё как сейчас

## Post-Completion

*Внешние действия*

- Обновить в обе версии README примером использования `input_format`
- Проверить что существующие пайплайны с датами в merge не затронуты (нет `input_format` → нет изменений)
