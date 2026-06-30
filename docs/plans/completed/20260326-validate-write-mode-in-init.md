# Валидация `write_mode` в `__init__`

## Overview

`write_mode` в `GoogleSheetsWriteOperator` принимается без валидации в `__init__` — ошибка
обнаруживается только при запуске задачи (`execute()`), а не при парсинге DAG.

**Решение:** перенести валидацию в `__init__`, в `execute()` заменить standalone `raise` на
`else`-ветку как defensive fallback.

**Не влияет на существующие DAG:** все валидные значения (`overwrite`, `append`, `merge`,
`smart_merge`) продолжают работать без изменений. Невалидные значения теперь будут
обнаруживаться раньше — при парсинге DAG, а не при запуске задачи.

## Context (from discovery)

- Файл: `airflow_provider_google_sheets/operators/write.py`
  - `__init__` ~line 86: `write_mode: str = "overwrite"` без валидации
  - `execute()` ~line 253: standalone `raise ValueError(f"Unknown write_mode: ...")` в конце цепочки `if/if/if`
- Тесты: `tests/test_operators/test_write.py`, класс `TestUnknownWriteMode` (~line 1122)
  - Существующий тест вызывает `op.execute(context)` для получения ошибки — нужно обновить
- Паттерн: другие параметры (`partition_value`, `merge_key`) валидируются в методах, а не в
  `__init__`, но для `write_mode` перенос оправдан — это фундаментальный режим работы оператора

## Development Approach

- Подход: сначала код, затем тест (изменение тривиальное)
- Изменения минимальны: два файла, несколько строк
- Обратная совместимость: не нарушается

## Testing Strategy

- Обновить `TestUnknownWriteMode.test_raises_on_unknown_mode` — ошибка теперь при создании, не при `execute()`
- Обновить `match=` паттерн: `"Unknown write_mode"` → `"Invalid write_mode"`
- Убедиться что все существующие тесты с валидными `write_mode` проходят

## Progress Tracking

- Отмечать выполненные пункты `[x]` сразу по завершению
- Добавлять новые задачи с префиксом ➕
- Документировать блокеры с префиксом ⚠️

## Implementation Steps

### Task 1: Перенести валидацию в `__init__` и обновить тест

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`
- Modify: `tests/test_operators/test_write.py`

- [ ] Добавить константу на уровне модуля (после `logger = ...`):
  ```python
  _VALID_WRITE_MODES = frozenset({"overwrite", "append", "merge", "smart_merge"})
  ```
- [ ] В `__init__` добавить проверку *до* `self.write_mode = write_mode`:
  ```python
  if write_mode not in _VALID_WRITE_MODES:
      raise ValueError(
          f"Invalid write_mode '{write_mode}'. "
          f"Supported: {sorted(_VALID_WRITE_MODES)}"
      )
  self.write_mode = write_mode
  ```
- [ ] В `execute()` преобразовать цепочку `if/if/if/raise` в `if/elif/elif/else`:
  ```python
  if self.write_mode == "overwrite":
      return self._execute_overwrite(hook, headers, rows)
  elif self.write_mode == "append":
      return self._execute_append(hook, headers, rows)
  elif self.write_mode in ("merge", "smart_merge"):
      return self._execute_merge(hook, headers, rows, original_headers=original_headers)
  else:
      raise ValueError(f"Unknown write_mode: '{self.write_mode}'")  # defensive fallback
  ```
- [ ] Обновить `TestUnknownWriteMode.test_raises_on_unknown_mode`:
  - ошибка теперь при `GoogleSheetsWriteOperator(...)`, не при `op.execute(context)`
  - обновить `match="Unknown write_mode"` → `match="Invalid write_mode"`
- [ ] Запустить: `pytest tests/test_operators/test_write.py -v` — все зелёные
- [ ] Запустить полный тест-сьют: `pytest tests/ -v` — все зелёные

### Task 2: Обновить CHANGELOG и переместить план

**Files:**
- Modify: `CHANGELOG.md`

- [ ] Добавить запись в `CHANGELOG.md`
- [ ] Переместить этот план в `docs/plans/completed/`

## Technical Details

**До:**
```python
def __init__(self, ..., write_mode: str = "overwrite", ...):
    self.write_mode = write_mode  # без валидации

def execute(self, context):
    if self.write_mode == "overwrite": ...
    if self.write_mode == "append": ...
    if self.write_mode in ("merge", "smart_merge"): ...
    raise ValueError(f"Unknown write_mode: '{self.write_mode}'")  # поздняя ошибка
```

**После:**
```python
_VALID_WRITE_MODES = frozenset({"overwrite", "append", "merge", "smart_merge"})

def __init__(self, ..., write_mode: str = "overwrite", ...):
    if write_mode not in _VALID_WRITE_MODES:
        raise ValueError(
            f"Invalid write_mode '{write_mode}'. "
            f"Supported: {sorted(_VALID_WRITE_MODES)}"
        )
    self.write_mode = write_mode  # валидировано

def execute(self, context):
    if self.write_mode == "overwrite": ...
    elif self.write_mode == "append": ...
    elif self.write_mode in ("merge", "smart_merge"): ...
    else:
        raise ValueError(...)  # defensive fallback — недостижим при нормальном использовании
```

**Почему `frozenset`:** неизменяемый, O(1) поиск, явно сигнализирует что это фиксированный набор.

**Почему `else` в execute():** защита от подклассов и рефакторингов, которые могут обойти `__init__`.
