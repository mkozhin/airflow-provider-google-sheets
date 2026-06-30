# Normalize Merge Key Format

## Overview

При merge-операции `GoogleSheetsWriteOperator` читает ключевую колонку из Google Sheets и строит индекс существующих строк. Метод `_normalize_sheet_key` нормализует значения ключей, но парсит только по `schema[merge_key]["format"]`.

**Три реальных сценария, порождающих дубли:**
1. **Региональный формат таблицы**: хук пишет через `USER_ENTERED` → Sheets авто-распознаёт `"2024-01-01"` как дату и сохраняет как serial number → при чтении (`UNFORMATTED_VALUE + FORMATTED_STRING`) возвращается строка в региональном формате (`"01.01.2024"` для российской локали). Несовпадение с `format="%Y-%m-%d"` → дубль.
2. **Ручное изменение**: пользователь вручную поменял формат ячеек в таблице.
3. **Serial date number**: ячейка хранит число `46023` без date-формата (формат сброшен или не применён) → API возвращает `"46023"` вместо строки-даты.

Во всех случаях `_normalize_sheet_key` пытается распарсить через `format` → FAILS → возвращает raw → ключ не совпадает с входящим значением → строка не удаляется → дубль.

Добавляем параметр `normalize_merge_key_format: bool = True`. При `True` метод нормализации пробует расширенный набор fallback-стратегий для парсинга ключей из таблицы. По умолчанию `True` — breaking change безопасен, т.к. `format`/`input_format` задаётся только для дат; для строковых и числовых ключей без схемы нормализация возвращает raw без изменений.

## Context

- **`airflow_provider_google_sheets/utils/schema.py`** — новая функция `normalize_merge_key`
- **`airflow_provider_google_sheets/operators/write.py`**
  - `__init__`: новый параметр (boolean, НЕ в `template_fields`)
  - `_normalize_sheet_key`: тонкая обёртка над `normalize_merge_key`
- **`tests/test_utils/test_schema.py`** — unit-тесты `TestNormalizeMergeKey` (прямые, без mock_hook)
- **`tests/test_operators/test_write.py`** — минимальный smoke-тест (оператор корректно использует нормализацию)
- **Импорты**: `date`, `datetime`, `timedelta` уже есть в `schema.py`; в `write.py` добавлять не нужно

## Development Approach

- **Testing approach**: Regular (code → tests)
- Сохранить обратную совместимость: `normalize_merge_key_format=False` → поведение не меняется
- Только in-memory изменения, никаких дополнительных API-вызовов
- Serial-date fallback применяется ТОЛЬКО когда `type` колонки — `date` или `datetime`

## Solution Overview

**Новая функция** `normalize_merge_key(raw: str, key_schema: dict | None, extended: bool = True) -> str` в `utils/schema.py`.

При `extended=True` порядок попыток парсинга:
1. `format` из схемы (текущее поведение — уже в целевом формате)
2. `input_format` из схемы (если задан)
3. Google Sheets serial date number (только если `type in ("date", "datetime")`): `int(float(raw))` → `date(1899, 12, 30) + timedelta(days=serial)` → форматировать через `format`
4. Не распарсилось → вернуть raw + warning в лог

При `extended=False` — только шаг 1.
При `key_schema=None` — вернуть raw немедленно (no schema = no normalization).

**Оператор** `_normalize_sheet_key(self, raw: str) -> str` становится тонкой обёрткой:
```python
def _normalize_sheet_key(self, raw: str) -> str:
    key_schema = (self.schema or {}).get(self.merge_key)
    return normalize_merge_key(raw, key_schema, extended=self.normalize_merge_key_format)
```

Заголовок таблицы защищён: `data_rows = existing_keys_raw[1:]` при `has_headers=True` — `_normalize_sheet_key` никогда не вызывается на заголовке.

## Technical Details

**Serial date conversion** (только когда `key_schema["type"] in ("date", "datetime")`):
```python
serial = int(float(raw))
sheets_epoch = date(1899, 12, 30)
parsed_date = sheets_epoch + timedelta(days=serial)
```
Для `type: "datetime"` — `datetime.combine(parsed_date, datetime.min.time())`.

**Edge cases**:
- `input_format` не задан в схеме → шаг 2 пропускается
- `type` не `date`/`datetime` → шаг 3 пропускается (исключает ложные срабатывания для числовых ID-ключей)
- `normalize_merge_key_format=False` → поведение как сейчас (только `format`, без input_format и serial date fallbacks)
- Нецелые числа (`"46023.5"`) → `int(float(raw))` обрезает дробную часть; для `datetime` это может потерять время суток — задокументировать в docstring как known limitation

## Implementation Steps

### Task 1: Новая функция `normalize_merge_key` в `utils/schema.py`

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`

- [x] Добавить функцию `normalize_merge_key(raw: str, key_schema: dict | None, extended: bool = True) -> str` в конец файла
- [x] `key_schema=None` → вернуть `raw` немедленно
- [x] При `extended=True`: fallback на `input_format` (шаг 2), затем serial date только если `type in ("date", "datetime")` (шаг 3), последний fallback — raw + `logger.warning`
- [x] При `extended=False` — только шаг 1 (parse with `format`)
- [x] Добавить приватную `_sheets_serial_to_date(serial: int) -> date` — конвертирует Google Sheets serial number в `date`. Эпоха: 1899-12-30 (quirk, унаследованный от Lotus 1-2-3)
- [x] В `normalize_merge_key` для serial date шага: `_sheets_serial_to_date(int(float(raw)))` → `format_value_for_write`

### Task 2: Обновить оператор

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] Добавить `normalize_merge_key_format: bool = True` в `__init__` и docstring, присвоить `self.normalize_merge_key_format`
- [x] **НЕ** добавлять в `template_fields` — boolean-флаги не шаблонизируются
- [x] Добавить импорт `from airflow_provider_google_sheets.utils.schema import ..., normalize_merge_key`
- [x] Заменить реализацию `_normalize_sheet_key` тонкой обёрткой:
  ```python
  def _normalize_sheet_key(self, raw: str) -> str:
      key_schema = (self.schema or {}).get(self.merge_key)
      return normalize_merge_key(raw, key_schema, extended=self.normalize_merge_key_format)
  ```
- [x] Запустить тесты (должны пройти существующие): `cd /Users/mkozhin/PycharmProjects/airflow-provider-google-sheets && python -m pytest tests/test_operators/test_write.py -x -q`

### Task 3: Unit-тесты `normalize_merge_key` в `test_schema.py`

**Files:**
- Modify: `tests/test_utils/test_schema.py`

- [x] Добавить класс `TestNormalizeMergeKey` — прямые тесты функции без mock_hook и оператора
- [x] Тест: `key_schema=None` → возвращает raw
- [x] Тест: ключ уже в `format` → нормализуется без изменений
- [x] Тест: `extended=True`, ключ в `input_format` → приводится к `format`
- [x] Тест: `_sheets_serial_to_date(46023)` → конкретная дата (верификация эпохи)
- [x] Тест: `extended=True`, serial date `"46023"` при `type="date"` → приводится к `format`
- [x] Тест: `extended=True`, числовое значение при `type="str"` → serial-date fallback НЕ срабатывает, возвращается raw
- [x] Тест: `extended=True`, ключ не парсится никак → raw + warning (проверить через `pytest.warns` или `caplog`)
- [x] Тест: `extended=False`, ключ в `input_format` → НЕ нормализуется, остаётся raw (старое поведение)
- [x] Запустить: `python -m pytest tests/test_utils/test_schema.py -x -q`

### Task 4: Smoke-тест в `test_write.py`

**Files:**
- Modify: `tests/test_operators/test_write.py`

- [x] Добавить класс `TestNormalizeMergeKeyFormat` после `TestMergeInputFormat`
- [x] Тест: `normalize_merge_key_format=False` (явно отключён) через `op.execute()` — дубли не устраняются (ключ `"01.01.2024"` в таблице ≠ входящий `"2024-01-01"`)
- [x] Тест: `normalize_merge_key_format=True` (дефолт) через `op.execute()` — ключ в `input_format` распознаётся, старая строка удаляется
- [x] Запустить: `python -m pytest tests/test_operators/test_write.py -x -q`

### Task 3: CHANGELOG и финализация

**Files:**
- Modify: `CHANGELOG.md`

- [x] Добавить запись в новую версию (следующую после v0.9.1):
  - `**Fixed:** merge больше не создаёт дубли когда ключ в таблице хранится в формате отличном от \`format\` в схеме (региональный формат Sheets, ручное изменение, serial date number)`
  - `**Added:** \`normalize_merge_key_format\` параметр \`GoogleSheetsWriteOperator\` (bool, default \`True\`) — расширенная нормализация ключей при merge: парсит через \`format\`, затем \`input_format\`, затем как Google Sheets serial date number`
- [x] Запустить полный набор тестов: `python -m pytest tests/ -x -q` — 457 passed
- [x] Проверить, что `TestMergeInputFormat` по-прежнему проходит
- [x] Переместить план в `docs/plans/completed/`

## Post-Completion

**Manual verification**:
- Проверить на реальной таблице: загрузить данные с `input_format`, затем снова с `normalize_merge_key_format=True` — убедиться, что дублей нет
