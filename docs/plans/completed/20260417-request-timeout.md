# request_timeout для GoogleSheetsHook и всех операторов

## Overview

Добавить параметр `request_timeout` в `GoogleSheetsHook` и все операторы провайдера, чтобы
пользователи могли точечно управлять таймаутом HTTP-запросов к Google Sheets API на уровне
отдельной задачи — без изменения глобальных настроек Airflow.

**Проблема:** `httplib2` внутри `googleapiclient` использует `socket.getdefaulttimeout()`,
которое Airflow выставляет через `AIRFLOW__CORE__SOCKET_TIMEOUT` (дефолт — 240 с = 4 мин).
При `smart_merge` на 120k строк запрос `append_values` занимает дольше 4 минут → падает с
`TimeoutError: The read operation timed out`.

**Решение:** явно создавать `httplib2.Http(timeout=N)` + `AuthorizedHttp` в `_build_service()`,
что переопределяет глобальный socket timeout для всех запросов хука.

**Acceptance Criteria:**
1. Передача `request_timeout=N` устанавливает timeout в `httplib2.Http` и переопределяет Airflow-дефолт
2. Все существующие тесты продолжают проходить

**Результат:**
```python
GoogleSheetsWriteOperator(
    task_id="big_merge",
    write_mode="smart_merge",
    request_timeout=900,  # 15 минут для этого таска
    ...
)
```

## Context

- **Файлы реализации:** `airflow_provider_google_sheets/hooks/google_sheets.py`,
  `operators/write.py`, `operators/read.py`, `operators/manage.py`
- **Файлы тестов:** `tests/test_hooks/test_google_sheets.py`,
  `tests/test_operators/test_write.py`, `tests/test_operators/test_read.py`,
  `tests/test_operators/test_manage.py`
- **Зависимость:** `google-auth-httplib2` — прямая зависимость `google-api-python-client>=2.0`, уже гарантирована, добавлять в `pyproject.toml` не нужно
- **Исключение:** `GoogleSheetsExtractPartitionsOperator` в manage.py API не вызывает — `request_timeout` туда НЕ добавляем

## Development Approach

- **Тестирование:** Code first, затем тесты
- Каждую задачу завершать полностью до перехода к следующей
- Все тесты должны проходить перед переходом к следующей задаче

## Testing Strategy

- Проверять что `httplib2.Http` создаётся с нужным `timeout=`
- Проверять что `build()` вызывается с `http=authorized_http` (не с `credentials=`)
- Покрывать три кейса: конкретное значение, `None`, дефолт 300

## Solution Overview

1. `GoogleSheetsHook` получает `request_timeout: int | None = 300`
2. `_build_service()` при `request_timeout is not None` создаёт `AuthorizedHttp` с
   `httplib2.Http(timeout=self.request_timeout)` и передаёт в `build(http=...)`
3. При `request_timeout=None` — старое поведение: `build(credentials=...)` → inherits socket default
4. Все операторы (кроме `GoogleSheetsExtractPartitionsOperator`) получают `request_timeout: int | None = 300`
   и пробрасывают в хук

## Technical Details

**Изменение в `_build_service()`:**
```python
# Импорты — на уровне модуля вместе с остальными:
import httplib2
from google_auth_httplib2 import AuthorizedHttp

# В _build_service():
if self.request_timeout is not None:
    authorized_http = AuthorizedHttp(credentials, http=httplib2.Http(timeout=self.request_timeout))
    return build("sheets", "v4", http=authorized_http, cache_discovery=False)
else:
    # request_timeout=None: наследует socket.getdefaulttimeout() (текущее поведение)
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)
```

**Семантика `None`:** `request_timeout=None` означает "наследовать глобальный socket timeout"
(т.е. текущее поведение — может быть 240 с от Airflow). Не путать с "ждать бесконечно".

**Взаимодействие с retry:** `request_timeout` — таймаут на один HTTP-вызов. При `TimeoutError`
`retry_with_backoff` срабатывает (статус ошибки — Exception, не HttpError). Суммарное время
может вырасти до `request_timeout × (max_retries + 1)` = 300 × 6 = 30 мин. Планировать
`execution_timeout` в DAG с запасом.

**API-вызывающие операторы в manage.py** (4 класса, `ExtractPartitions` — пропускаем):
- `GoogleSheetsCreateSpreadsheetOperator`
- `GoogleSheetsCreateSheetOperator`
- `GoogleSheetsListSheetsOperator`
- `GoogleSheetsUniqueValuesOperator`

## Implementation Steps

### Task 1: GoogleSheetsHook — добавить request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/hooks/google_sheets.py`

- [x] добавить на уровне модуля (рядом с остальными импортами): `import httplib2` и `from google_auth_httplib2 import AuthorizedHttp`
- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] обновить docstring хука: описать `request_timeout` с пояснением семантики `None`
- [x] в `_build_service()` реализовать условную логику: если `request_timeout is not None` → `AuthorizedHttp + httplib2.Http(timeout=...)`, иначе → старый путь `credentials=`
- [x] запустить: `pytest tests/test_hooks/ -v`

### Task 2: Тесты GoogleSheetsHook

**Files:**
- Modify: `tests/test_hooks/test_google_sheets.py`

- [x] тест: `request_timeout=300` (дефолт) → `httplib2.Http` вызван с `timeout=300`, `build` вызван с `http=authorized_http` (не `credentials=`)
- [x] тест: `request_timeout=600` → `httplib2.Http` вызван с `timeout=600`
- [x] тест: `request_timeout=None` → `build` вызван со старыми аргументами `credentials=`, `httplib2.Http` не создаётся
- [x] mock-ить `httplib2.Http`, `AuthorizedHttp` и `build`, проверять аргументы через `assert_called_once_with`
- [x] запустить: `pytest tests/test_hooks/ -v` — все должны пройти (22/22)

### Task 3: GoogleSheetsWriteOperator — добавить request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] обновить docstring класса: описать `request_timeout`
- [x] в `execute()` передавать `request_timeout` в хук: `GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id, request_timeout=self.request_timeout)`
- [x] запустить: `pytest tests/test_operators/test_write.py -v` (109/109)

### Task 4: Тесты GoogleSheetsWriteOperator

**Files:**
- Modify: `tests/test_operators/test_write.py`

- [x] тест: оператор с `request_timeout=600` → хук создаётся с `request_timeout=600`
- [x] тест: оператор без параметра → хук создаётся с `request_timeout=300`
- [x] тест: оператор с `request_timeout=None` → хук создаётся с `request_timeout=None`
- [x] запустить: `pytest tests/test_operators/test_write.py -v` — все должны пройти (112/112)

### Task 5: GoogleSheetsReadOperator — добавить request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/operators/read.py`

- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] обновить docstring: описать `request_timeout`
- [x] в `execute()` передавать `request_timeout` в хук
- [x] запустить: `pytest tests/test_operators/test_read.py -v` (62/62)

### Task 6: Тесты GoogleSheetsReadOperator

**Files:**
- Modify: `tests/test_operators/test_read.py`

- [x] тест: оператор с `request_timeout=120` → хук создаётся с `request_timeout=120`
- [x] тест: оператор без параметра → хук создаётся с `request_timeout=300`
- [x] тест: оператор с `request_timeout=None` → хук создаётся с `request_timeout=None`
- [x] запустить: `pytest tests/test_operators/test_read.py -v` — все должны пройти (65/65)

### Task 7a: GoogleSheetsCreateSpreadsheetOperator — request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`

- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] в `execute()` передавать `request_timeout` в хук
- [x] запустить: `pytest tests/test_operators/test_manage.py -v`

### Task 7b: GoogleSheetsCreateSheetOperator — request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`

- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] в `execute()` передавать `request_timeout` в хук
- [x] запустить: `pytest tests/test_operators/test_manage.py -v`

### Task 7c: GoogleSheetsListSheetsOperator — request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`

- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] в `execute()` передавать `request_timeout` в хук
- [x] запустить: `pytest tests/test_operators/test_manage.py -v`

### Task 7d: GoogleSheetsUniqueValuesOperator — request_timeout

**Files:**
- Modify: `airflow_provider_google_sheets/operators/manage.py`

- [x] добавить `request_timeout: int | None = 300` в `__init__`, сохранить как `self.request_timeout`
- [x] в `execute()` передавать `request_timeout` в хук
- [x] запустить: `pytest tests/test_operators/test_manage.py -v` (33/33)

### Task 8: Тесты операторов manage.py

**Files:**
- Modify: `tests/test_operators/test_manage.py`

- [x] для каждого из 4 операторов: тест что `request_timeout=600` пробрасывается в хук
- [x] для каждого из 4 операторов: тест дефолтного значения 300
- [x] для каждого из 4 операторов: тест `request_timeout=None`
- [x] запустить: `pytest tests/test_operators/test_manage.py -v` — все должны пройти (45/45)

### Task 9: Финальная проверка и документация

- [x] запустить полный набор тестов: `pytest tests/ -v`
- [x] убедиться что все существующие тесты продолжают проходить (438/438)
- [x] проверить что `request_timeout` задокументирован в docstring у хука и всех 6 операторов
- [x] обновить `CHANGELOG.md`: добавить запись о `request_timeout` для ближайшей patch-версии
- [x] переместить план в `docs/plans/completed/`

## Post-Completion

**Использование в DAG:**
```python
# Для большого merge с 120k+ строк — установить больше чем ожидаемое время операции:
GoogleSheetsWriteOperator(
    task_id="big_merge",
    write_mode="smart_merge",
    request_timeout=900,   # 15 минут per API call
    batch_size=4000,
    ...
)
```

**Важно:** учитывать взаимодействие с retry — при таймауте `retry_with_backoff` повторяет
запрос. Итоговое время = `request_timeout × (max_retries + 1)`. Планировать `execution_timeout`
в DAG с соответствующим запасом.

**Версия:** patch-релиз (например v0.9.1) — изменение обратно совместимо, дефолт 300 лучше
прежнего поведения (240 с от Airflow). Обновить версию в `_version.py` при релизе.
