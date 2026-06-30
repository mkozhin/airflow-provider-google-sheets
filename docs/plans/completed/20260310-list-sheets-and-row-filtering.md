# List Sheets Operator + Row Filtering for Read Operator

## Overview

Two new features:

1. **`GoogleSheetsListSheetsOperator`** — возвращает список вкладок (листов) spreadsheet через XCom с опциональной фильтрацией по имени или позиции. Совместим с dynamic task mapping Airflow (`expand(sheet_name=...)`).

2. **Row filtering в `GoogleSheetsReadOperator`** — два новых параметра `row_skip` и `row_stop` для клиентской фильтрации строк при чтении: пропуск строк по условию и остановка чтения при достижении условия.

## Context (from discovery)

- Files involved:
  - `airflow_provider_google_sheets/operators/manage.py` — добавить новый оператор
  - `airflow_provider_google_sheets/operators/read.py` — добавить параметры и логику фильтрации
  - `airflow_provider_google_sheets/hooks/google_sheets.py` — `get_spreadsheet_metadata()` уже есть, ничего не меняем
  - `tests/test_operators/test_manage.py` — добавить тесты нового оператора
  - `tests/test_operators/test_read.py` — добавить тесты фильтрации
  - `readme.md` — документация

- New file to create:
  - `airflow_provider_google_sheets/utils/row_filter.py` — утилита для вычисления условий фильтрации
  - `tests/test_utils/test_row_filter.py` — тесты утилиты

- Related patterns:
  - Hook мокируется на уровне оператора: `patch("airflow_provider_google_sheets.operators.X.GoogleSheetsHook")`
  - Тесты используют `side_effect=[...]` для последовательных вызовов `get_values`
  - Классовая организация тестов (`class TestXxx`)

## Development Approach

- **Testing approach**: Regular (код → тесты)
- Завершить каждую задачу полностью перед переходом к следующей
- Все тесты должны проходить перед переходом к следующей задаче
- Запускать тесты: `pytest`

## Technical Details

### GoogleSheetsListSheetsOperator

Параметры:
- `gcp_conn_id: str = "google_cloud_default"`
- `spreadsheet_id: str` — обязательный
- `name_pattern: str | None = None` — regex для включения листов по имени
- `exclude_pattern: str | None = None` — regex для исключения листов по имени
- `index_range: tuple[int, int] | None = None` — срез по позиции (0-based, start включительно, end исключительно)

Возвращает: `list[str]` имён листов.

Логика фильтрации:
1. Получить все листы через `get_spreadsheet_metadata()`
2. Применить `index_range` (срез списка)
3. Применить `name_pattern` (re.search)
4. Применить `exclude_pattern` (re.search, исключить совпавшие)

### Row filtering: `row_skip` и `row_stop`

Формат условия:
```python
{"column": "status", "value": "ИТОГО", "op": "equals"}
# op по умолчанию = "equals"
# для empty/not_empty поле value не нужно
```

Поддерживаемые `op`: `equals`, `not_equals`, `contains`, `not_contains`, `starts_with`, `ends_with`, `empty`, `not_empty`

Параметры оператора принимают `dict` или `list[dict]` (одиночный dict нормализуется в список).

`column` — имя после всей обработки заголовков (transliteration/mapping/etc.).

Утилита `row_filter.py`:
- `normalize_conditions(raw) -> list[dict]` — принимает dict или list[dict]
- `validate_conditions(conditions)` — проверяет структуру, поднимает ValueError
- `matches_any(row_dict, conditions) -> bool` — возвращает True если строка совпадает хотя бы с одним условием

Применение в `_read_chunks()`:
- После применения схемы к чанку
- `row_stop`: при совпадении — вернуть строки до совпавшей (не включая), выставить флаг остановки
- `row_skip`: отфильтровать строки из чанка
- Если `row_stop` сработал — генератор завершается, дальнейшие API-вызовы не делаются

Строки для фильтрации передаются как `dict` (после `rows_to_dicts`), но внутри `_read_chunks` строки ещё в виде `list` — нужно конвертировать в dict для проверки условия, используя `headers`.

## Implementation Steps

### Task 1: Утилита row_filter.py

**Files:**
- Create: `airflow_provider_google_sheets/utils/row_filter.py`
- Create: `tests/test_utils/test_row_filter.py`

- [ ] создать `utils/row_filter.py` с функцией `normalize_conditions(raw) -> list[dict]`
- [ ] добавить `validate_conditions(conditions: list[dict]) -> None` с проверкой обязательных полей и допустимых значений `op`
- [ ] добавить `matches_any(row_dict: dict, conditions: list[dict]) -> bool` с реализацией всех 8 операторов
- [ ] написать тесты для `normalize_conditions` (dict → list, list → list, None edge case)
- [ ] написать тесты для `validate_conditions` (корректные условия, неверный op, отсутствует column, value обязателен для неempty-ops)
- [ ] написать тесты для `matches_any` — по одному тесту на каждый из 8 операторов + тест множественных условий (OR-логика)
- [ ] запустить тесты — все должны пройти

### Task 2: GoogleSheetsListSheetsOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`
- Modify: `tests/test_operators/test_manage.py`

- [ ] добавить `GoogleSheetsListSheetsOperator` в `manage.py` после существующих операторов
- [ ] реализовать фильтрацию: `index_range` → `name_pattern` → `exclude_pattern`
- [ ] добавить оператор в `template_fields`: `spreadsheet_id`, `name_pattern`, `exclude_pattern`
- [ ] написать тесты: возврат всех листов без фильтров
- [ ] написать тесты: фильтрация по `name_pattern` (совпадение / нет совпадений)
- [ ] написать тесты: фильтрация по `exclude_pattern`
- [ ] написать тесты: фильтрация по `index_range`
- [ ] написать тест: комбинация всех трёх фильтров
- [ ] запустить тесты — все должны пройти

### Task 3: row_skip и row_stop в GoogleSheetsReadOperator

**Files:**
- Modify: `airflow_provider_google_sheets/operators/read.py`
- Modify: `tests/test_operators/test_read.py`

- [ ] добавить параметры `row_skip` и `row_stop` в `__init__` (тип `dict | list[dict] | None`, default `None`)
- [ ] добавить оба параметра в `template_fields`
- [ ] в `execute()` нормализовать и валидировать условия через утилиту до начала чтения
- [ ] изменить `_read_chunks()`: добавить аргументы `row_skip` и `row_stop`, применять фильтрацию после схемы
- [ ] для `row_stop` внутри чанка: найти первую совпавшую строку, вернуть только строки до неё, выставить флаг `stop=True`
- [ ] написать тест `row_skip`: пропуск строк с конкретным значением
- [ ] написать тест `row_skip`: пропуск нескольких условий (OR-логика)
- [ ] написать тест `row_stop`: остановка до строки с ИТОГО
- [ ] написать тест `row_stop`: срабатывание в середине чанка — нет лишних API-вызовов
- [ ] написать тест `row_stop` + `row_skip` одновременно
- [ ] написать тест: невалидное условие поднимает ValueError до начала чтения
- [ ] запустить тесты — все должны пройти

### Task 4: Проверка приёмочных критериев

- [ ] все операторы присутствуют и корректно работают по сценариям из Overview
- [ ] `GoogleSheetsListSheetsOperator` совместим с `expand(sheet_name=op.output)` (проверить что возвращает `list[str]`)
- [ ] `row_stop` не делает лишних API-вызовов после срабатывания (проверить mock call count)
- [ ] запустить полный тест-сьют: `pytest`
- [ ] нет регрессий в существующих тестах

### Task 5: [Final] Документация и CHANGELOG

**Files:**
- Modify: `readme.md`
- Modify: `CHANGELOG.md`

- [ ] добавить `GoogleSheetsListSheetsOperator` в раздел Operators readme с примером и таблицей параметров
- [ ] добавить `row_skip` и `row_stop` в таблицу параметров `GoogleSheetsReadOperator` readme с примерами
- [ ] добавить запись в `CHANGELOG.md` (v0.5.0)
- [ ] переместить этот план в `docs/plans/completed/`

## Post-Completion

**Ручная проверка:**
- Проверить совместимость с Airflow dynamic task mapping на реальном DAG
- Убедиться что `row_stop` корректно работает при стриминге в CSV/JSONL (не только XCom)
