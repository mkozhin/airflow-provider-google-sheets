# Retry для транзиентных 404 от Google Sheets API

## Overview

Google Sheets API иногда возвращает 404 на реально существующий spreadsheet после тяжёлых операций записи (например, `values.append` на 40k+ строк). Текущий retry в провайдере обрабатывает только `(429, 500, 503)` — 404 не ретраится и задача Airflow падает с `HttpError 404`.

Решение: добавить в `retry_with_backoff()` отдельную конфигурацию для "transient not found" кодов с более длинным backoff и меньшим числом попыток.

## Context (from discovery)

- **Файлы изменений**: `airflow_provider_google_sheets/utils/retry.py`, `airflow_provider_google_sheets/hooks/google_sheets.py`
- **Файл тестов**: `tests/test_utils/test_retry.py`
- **Не меняется**: `operators/write.py`
- **Паттерн**: декоратор `@retry_with_backoff()` применён к большинству методов hook; часть хелперов (`create_sheet`, `get_sheet_properties`, `ensure_rows`, `trim_sheet`, `get_sheet_id`) не декорирована
- **Декорированные write-методы** (нужна 404-retry): `update_values` (~168), `append_values` (~191), `clear_values` (~214), `batch_update_values` (~226), `batch_update` (~252)
- **Декорированные read-методы** (404-retry НЕ нужна, должна падать сразу): `get_values` (114), `batch_get_values` (140) и др.
- **Существующий тест** `test_404_is_not_retryable` в `TestIsRetryableHttpError` — остаётся без изменений, т.к. тестирует `is_retryable_http_error()` (regular retry), а не wrapper-логику

## Development Approach

- **testing approach**: Regular (код + тесты в одном таске)
- Выбран opt-in подход: по умолчанию `not_found_max_retries=0` (выключено), явно включается только на write-методах
- Обратная совместимость обязательна: существующие вызовы `@retry_with_backoff()` без новых параметров ведут себя идентично (404 падает сразу, как раньше)

## Solution Overview

В `retry_with_backoff()` добавляются три новых параметра:
- `not_found_status_codes: Sequence[int] = (404,)` — коды, считающиеся транзиентными not found
- `not_found_max_retries: int = 0` — максимум retry-попыток для этих кодов; `0` = выключено (дефолт, opt-in); write-методы явно передают `= 3`
- `not_found_base_delay: float = 5.0` — начальная задержка в секундах

Внутри `wrapper` — два независимых счётчика (`regular_attempts`, `not_found_attempts`). При 404: задержка 5с → 10с → 20с (exponential, capped at `max_delay`). При исчерпании `not_found_max_retries` — `raise` как обычно.

Логика выбора кода на 404:
- 3 retry-попытки + 1 первоначальный вызов = 4 вызова функции максимум; backoff 5→10→20с → суммарно ~35с ожидания
- Если файл реально удалён — упадёт через ~35с, не бесконечно
- **Допущение об idempotency:** транзиентный 404 означает, что Google API не нашёл spreadsheet и запрос **не был применён** (в отличие от 500, где partial apply вероятнее). На этом допущении основана безопасность ретрая: `not_found_status_codes` перехватывается до ветки `retryable_status_codes` — при пересечении кодов not_found имеет приоритет.

## Technical Details

### Новая сигнатура `retry_with_backoff`

```python
DEFAULT_RETRYABLE_STATUS_CODES = (429, 500, 503)
DEFAULT_NOT_FOUND_STATUS_CODES = (404,)

def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: Sequence[Type[Exception]] | None = None,
    retryable_status_codes: Sequence[int] = DEFAULT_RETRYABLE_STATUS_CODES,
    # новые (opt-in: по умолчанию выключено):
    not_found_status_codes: Sequence[int] = DEFAULT_NOT_FOUND_STATUS_CODES,
    not_found_max_retries: int = 0,   # 0 = не ретраить 404 (поведение как раньше)
    not_found_base_delay: float = 5.0,
) -> Callable:
```

В `hooks/google_sheets.py` пять write-методов получают явный параметр:
```python
@retry_with_backoff(not_found_max_retries=3)
def update_values(...): ...

@retry_with_backoff(not_found_max_retries=3)
def append_values(...): ...

@retry_with_backoff(not_found_max_retries=3)
def clear_values(...): ...

@retry_with_backoff(not_found_max_retries=3)
def batch_update_values(...): ...

@retry_with_backoff(not_found_max_retries=3)
def batch_update(...): ...
```

Read-методы и `create_spreadsheet` остаются с `@retry_with_backoff()` без изменений → 404 падает мгновенно.

### Логика wrapper (псевдокод)

```python
regular_attempts = 0
not_found_attempts = 0

while True:
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if isinstance(e, HttpError) and e.resp.status in not_found_status_codes:
            not_found_attempts += 1
            if not_found_attempts > not_found_max_retries:
                raise
            delay = min(not_found_base_delay * (2 ** (not_found_attempts - 1)), max_delay)
            # + jitter если включён
            log + sleep(delay)
            continue

        # regular retry (429/500/503/custom exceptions)
        should_retry = ...
        regular_attempts += 1
        if not should_retry or regular_attempts > max_retries:
            raise
        delay = min(base_delay * (2 ** (regular_attempts - 1)), max_delay)
        # + jitter если включён
        log + sleep(delay)
```

## Implementation Steps

### Task 1: Обновить `retry.py` — новые параметры и логика wrapper

**Files:**
- Modify: `airflow_provider_google_sheets/utils/retry.py`

- [ ] добавить `DEFAULT_NOT_FOUND_STATUS_CODES = (404,)` константу
- [ ] добавить три новых параметра в `retry_with_backoff()`: `not_found_status_codes`, `not_found_max_retries=0` (opt-in!), `not_found_base_delay=5.0`
- [ ] переписать `wrapper` с `while True` и двумя счётчиками (`regular_attempts`, `not_found_attempts`); убедиться что каждая ветка завершается `return`, `continue` или `raise` — нет fall-through
- [ ] добавить отдельное log-сообщение для 404-ветки с `not_found_attempts` / `not_found_max_retries` (не переиспользовать формат regular retry)
- [ ] убедиться что при `not_found_max_retries=0` (дефолт) 404 падает мгновенно — поведение идентично текущему
- [ ] обновить docstring `retry_with_backoff`: описать новые параметры, задокументировать приоритет not_found над retryable при пересечении кодов

### Task 2: Обновить `hooks/google_sheets.py` — включить 404-retry на write-методах

**Files:**
- Modify: `airflow_provider_google_sheets/hooks/google_sheets.py`

- [ ] добавить `not_found_max_retries=3` в декоратор `update_values` (~строка 168)
- [ ] добавить `not_found_max_retries=3` в декоратор `append_values` (~строка 191)
- [ ] добавить `not_found_max_retries=3` в декоратор `clear_values` (~строка 214)
- [ ] добавить `not_found_max_retries=3` в декоратор `batch_update_values` (~строка 226)
- [ ] добавить `not_found_max_retries=3` в декоратор `batch_update` (~строка 252)
- [ ] read-методы (`get_values`, `batch_get_values`, `get_spreadsheet_metadata` и др.) — не трогать
- [ ] `create_spreadsheet` — не трогать: транзиентный 404 при создании таблицы крайне маловероятен, остаётся с дефолтом `=0`

### Task 3: Добавить тесты для 404 retry поведения

**Files:**
- Modify: `tests/test_utils/test_retry.py`

- [ ] тест: при `not_found_max_retries=0` (дефолт) 404 падает мгновенно, `time.sleep` не вызывается
- [ ] тест: 404 ретраится до `not_found_max_retries`, потом succeed → **ровно 4 вызова функции и 3 sleep'а** при `not_found_max_retries=3`
- [ ] тест: 404 превышает лимит → `HttpError 404` пробрасывается
- [ ] тест: exponential backoff для 404 (delays: 5.0, 10.0, 20.0 при `not_found_base_delay=5.0, jitter=False`)
- [ ] тест: jitter применяется к 404-ветке — delay попадает в диапазон `[base, base * 1.5]` при `jitter=True`
- [ ] тест: счётчики 500 и 404 независимы (функция падает 500 → 404 → success, оба счётчика растут независимо)
- [ ] тест: обратный порядок — 404 → 500 → success (убедиться что 404-retry не мешает regular retry)
- [ ] тест hook-уровня: `append_values` + 404 → ретраится; `get_values` + 404 → падает мгновенно
- [ ] убедиться что существующий `test_404_is_not_retryable` в `TestIsRetryableHttpError` по-прежнему проходит

### Task 4: Проверка и финализация

- [ ] запустить полный тест-сьют: `cd /Users/mkozhin/PycharmProjects/airflow-provider-google-sheets && python -m pytest tests/ -v`
- [ ] все тесты зелёные, включая ранее существующие
- [ ] обновить `CHANGELOG.md` (секция unreleased/fixed: добавить 404-retry для write-методов)
- [ ] переместить план в `docs/plans/completed/`

## Post-Completion

**Деплой в Airflow:**
- Опубликовать новую версию провайдера и обновить её на Airflow-инстансе `realcombi.mgcom.ru`
- Перезапустить упавший run `osnova_dash_ch_to_gs / write_ovz_control_to_sheets` от 2026-06-27 вручную (данные не повреждены, задача упала до завершения batchUpdate)
