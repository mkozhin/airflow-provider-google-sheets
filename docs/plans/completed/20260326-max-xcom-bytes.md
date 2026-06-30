# Добавить параметр `max_xcom_bytes` в `GoogleSheetsReadOperator`

## Overview

`max_xcom_rows=50000` защищает от слишком большого XCom по числу строк, но не учитывает реальный
объём данных в байтах. Широкие данные (много столбцов, длинные строки) при 50k строк могут занимать
50 MB+, что перегружает метадату Airflow или вызывает ошибки БД.

**Решение:**
- Добавить `max_xcom_bytes: int | None = None` — опциональный лимит в байтах (по умолчанию `None`,
  backwards compatible).
- Добавить `logger.warning` при объёме > 5 MB независимо от лимита — видимость без жёсткого ограничения.
- Сохранить `max_xcom_rows` как быстрый «первый» guard по числу строк.

## Context (from discovery)

- Основной файл: `airflow_provider_google_sheets/operators/read.py`, метод `_read_to_xcom` (~line 355)
- Тесты: `tests/test_operators/test_read.py`, класс с тестами `test_xcom_max_rows_exceeded` / `test_xcom_within_limit_works` (~line 696)
- Существующий паттерн тестов: `mock_hook.get_values.side_effect`, `pytest.raises(GoogleSheetsDataError)`
- `max_xcom_bytes` **не добавлять** в `template_fields` — числовой лимит, не DAG-контекст
- `json` уже импортирован в `read.py` — новый импорт не нужен

## Development Approach

- Подход: сначала код, затем тесты (изменение небольшое и хорошо изолировано)
- Обратная совместимость: дефолт `None` — пользователи без `max_xcom_bytes` не замечают изменений
- Изменения в одном методе `_read_to_xcom` + сигнатура `__init__`

## Testing Strategy

- Четыре новых теста рядом с существующими `test_xcom_max_rows_exceeded`:
  - `test_xcom_max_bytes_exceeded` — превышение лимита → `GoogleSheetsDataError` с упоминанием байт
  - `test_xcom_max_bytes_ok_under_limit` — данные в лимите → возвращает результат нормально
  - `test_xcom_max_bytes_none_no_check` — `None` дефолт → без ошибки (предупреждение не проверяем)
  - `test_xcom_large_payload_warning` — данные > 5 MB при `max_xcom_bytes=None` → `caplog` содержит warning

## Progress Tracking

- Отмечать выполненные пункты `[x]` сразу по завершению
- Добавлять новые задачи с префиксом ➕
- Документировать блокеры с префиксом ⚠️

## Implementation Steps

### Task 1: Добавить параметр и проверку в `GoogleSheetsReadOperator`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/read.py`

- [ ] Добавить `max_xcom_bytes: int | None = None` в сигнатуру `__init__` рядом с `max_xcom_rows`
- [ ] Добавить `self.max_xcom_bytes = max_xcom_bytes` в тело `__init__`
- [ ] Обновить docstring параметра (`max_xcom_bytes: Maximum XCom payload size in bytes...`)
- [ ] В `_read_to_xcom` преобразовать данные через `rows_to_dicts` ДО проверки байтов —
  это обеспечит измерение реального XCom-payload (dict-форма крупнее list-формы):
  ```python
  result = rows_to_dicts(all_rows, headers) if headers else all_rows
  estimated_bytes = len(json.dumps(result))
  if estimated_bytes > 5 * 1024 * 1024:
      logger.warning(
          "XCom payload is large: ~%d bytes (%d rows). "
          "Consider using output_type='csv'/'json' for large datasets.",
          estimated_bytes, len(all_rows),
      )
  if self.max_xcom_bytes is not None and estimated_bytes > self.max_xcom_bytes:
      raise GoogleSheetsDataError(
          f"Estimated XCom size ({estimated_bytes:,} bytes) exceeds "
          f"max_xcom_bytes ({self.max_xcom_bytes:,}). "
          f"Use output_type='csv' or 'json' for large datasets."
      )
  return result
  ```
- [ ] Удалить прежний `if headers: return rows_to_dicts(...) / return all_rows` — теперь return через `result`
- [ ] Убедиться что существующие тесты не сломались: `pytest tests/test_operators/test_read.py -k "xcom" -v`

### Task 2: Написать тесты

**Files:**
- Modify: `tests/test_operators/test_read.py`

- [ ] Добавить `test_xcom_max_bytes_exceeded`:
  мокаем данные с длинными значениями (`"x" * 100` × несколько строк),
  задаём `max_xcom_bytes=10`, ожидаем `GoogleSheetsDataError` с `match="max_xcom_bytes"`
- [ ] Добавить `test_xcom_max_bytes_ok_under_limit`:
  те же данные, `max_xcom_bytes=10_000_000` → результат возвращается без ошибки
- [ ] Добавить `test_xcom_max_bytes_none_no_check`:
  `max_xcom_bytes=None` (дефолт), данные чуть крупнее 10 байт → нет `GoogleSheetsDataError`
- [ ] Добавить `test_xcom_large_payload_warning` с `caplog`:
  мокаем данные > 5 MB (например `[["x" * 200] for _ in range(30_000)]`),
  `max_xcom_bytes=None`, `caplog.set_level(logging.WARNING)` →
  `assert "XCom payload is large" in caplog.text`
- [ ] Запустить тесты: `pytest tests/test_operators/test_read.py -k "xcom" -v`

### Task 3: Финальная проверка

**Files:** —

- [ ] Запустить полный тест-сьют: `pytest tests/ -v`
- [ ] Убедиться что все тесты зелёные

### Task 4: Обновить CHANGELOG и переместить план

- [ ] Добавить запись в `CHANGELOG.md` (feat: add `max_xcom_bytes` to `GoogleSheetsReadOperator`)
- [ ] Добавить упоминание параметра в `readme.md` и `readme_ru.md`
- [ ] Переместить этот план в `docs/plans/completed/`

## Technical Details

**Финальная форма `_read_to_xcom`:**
```python
def _read_to_xcom(self, hook, headers, data_start_row, row_skip=None, row_stop=None):
    all_rows: list[list[Any]] = []
    for chunk in self._read_chunks(hook, data_start_row, headers, row_skip, row_stop):
        all_rows.extend(chunk)
        if len(all_rows) > self.max_xcom_rows:
            raise GoogleSheetsDataError(...)  # быстрый guard без сериализации

    logger.info("Finished reading. Total rows: %d", len(all_rows))

    # Конвертируем сразу — нужна dict-форма для точного размера
    result = rows_to_dicts(all_rows, headers) if headers else all_rows

    estimated_bytes = len(json.dumps(result))  # без .encode() — json.dumps даёт ASCII
    if estimated_bytes > 5 * 1024 * 1024:
        logger.warning("XCom payload is large: ~%d bytes (%d rows)...", estimated_bytes, len(all_rows))
    if self.max_xcom_bytes is not None and estimated_bytes > self.max_xcom_bytes:
        raise GoogleSheetsDataError(...)

    return result
```

**Почему измеряем dict-форму, а не list-форму:**
Airflow сериализует XCom именно то, что возвращает `execute()`. При наличии заголовков возвращается
`list[dict]`, а не `list[list]`. Разница существенная: `[["a","b"]]` ≈ 12 байт,
`[{"col1":"a","col2":"b"}]` ≈ 28 байт — множитель растёт с числом колонок.

**Почему `len(json.dumps(result))` без `.encode("utf-8")`:**
`json.dumps` по умолчанию экранирует non-ASCII символы (`\uXXXX`), поэтому вывод — чистый ASCII.
`len(str)` и `len(str.encode("utf-8"))` для ASCII совпадают. `.encode()` — лишняя операция.

**Порядок guards:**
- `max_xcom_rows` — инкрементально в цикле чанков, без сериализации, быстро
- `max_xcom_bytes` — после цикла, требует `json.dumps`, зато точно

## Post-Completion

*Внешние действия, не входящие в этот план*

- Рассмотреть рекомендуемое значение `max_xcom_bytes` в документации (например, 10 MB)
