# Fix: Уточнить перехват HTTP 400 в `_ensure_sheet_exists`

## Overview

Метод `_ensure_sheet_exists` в `GoogleSheetsWriteOperator` перехватывает **любую** ошибку HTTP 400
от Google Sheets API и молча считает её рейс-кондишном (параллельное создание листа другой задачей).

На самом деле Google API возвращает 400 и в других случаях:
- недопустимое имя листа (слишком длинное, спецсимволы)
- превышение лимита листов в таблице
- невалидный `spreadsheet_id`

Все эти ошибки сейчас проглатываются без сообщения пользователю.

**Решение:** при 400 дополнительно проверять тело ошибки — рейс-кондишн содержит `"already exists"`.

## Context (from discovery)

- Файл с багом: `airflow_provider_google_sheets/operators/write.py`, метод `_ensure_sheet_exists` (~line 195)
- Тесты: `tests/test_operators/test_write.py`, класс `TestCreateSheetIfMissing` (~line 1417)
- Существующий тест `test_race_condition_http_400_swallowed` уже использует правильное тело ошибки,
  но кода для проверки тела нет — тест проходит "случайно"
- Отсутствует тест: "400 с посторонним сообщением должен пробрасываться"

## Development Approach

- Подход: сначала код, затем тест (изменение тривиальное)
- Изменения минимальны: одна строка в `write.py`, один новый тест
- Обратная совместимость: не нарушается

## Testing Strategy

- Обновить существующий `test_race_condition_http_400_swallowed` — убедиться что проходит
- Добавить `test_400_non_race_condition_propagates` — 400 с другим сообщением должен вызывать исключение
- Существующий `test_non_400_http_error_propagates` — должен продолжать проходить

## Progress Tracking

- Отмечать выполненные пункты `[x]` сразу по завершению
- Добавлять новые задачи с префиксом ➕
- Документировать блокеры с префиксом ⚠️

## Implementation Steps

### Task 1: Исправить проверку и добавить тест

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Изменить условие с `if e.resp.status == 400:` на
  `if e.resp.status == 400 and "already exists" in str(e).lower():`
  (`str(HttpError)` включает тело ответа — парсинг JSON не нужен)
- [ ] Обновить комментарий рядом — уточнить что проверяется тело ответа
- [ ] Убедиться, что `test_race_condition_http_400_swallowed` проходит без изменений
  (тело уже содержит `"already exists"`)
- [ ] Добавить тест `test_400_other_error_propagates`: 400 с телом `b"Too many sheets in the spreadsheet"`
  должен вызывать `HttpError`, а не проглатываться
- [ ] Запустить тесты: `pytest tests/test_operators/test_write.py -k "create_sheet_if_missing or ensure_sheet" -v`
  — все зелёные перед переходом к Task 2

### Task 3: Финальная проверка

- [ ] Запустить полный тест-сьют: `pytest tests/ -v`
- [ ] Убедиться, что все тесты зелёные

### Task 3: Обновить CHANGELOG и переместить план

- [ ] Добавить запись в `CHANGELOG.md` (секция unreleased или новая версия patch)
- [ ] Переместить этот план в `docs/plans/completed/`

## Technical Details

**До:**
```python
except HttpError as e:
    if e.resp.status == 400:
        # Race condition: another task already created the sheet
        logger.info(...)
    else:
        raise
```

**После:**
```python
except HttpError as e:
    if e.resp.status == 400 and "already exists" in str(e).lower():
        # Race condition: another task already created the sheet
        logger.info(...)
    else:
        raise
```

Google API при рейс-кондишне возвращает:
`"Invalid requests[0].addSheet: A sheet with the name 'X' already exists. Please enter another name."`

`str(HttpError)` включает тело ответа, поэтому проверка `"already exists" in str(e).lower()` надёжна
и не требует парсинга JSON.
