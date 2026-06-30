# Smart Merge: Переработка логики

## Overview

Переписать `_execute_smart_merge` с правильной логикой:

**Текущее поведение (неверное):** позиционное сопоставление строк внутри группы с одинаковым ключом. При изменении порядка строк (BigQuery не гарантирует порядок) данные оказываются в неправильных строках.

**Новое поведение:** для каждого уникального значения ключа во входящих данных — удалить ВСЕ существующие строки с этим ключом, добавить все входящие строки в конец. Строки которых нет во входящих данных — не трогать.

**Удаляется:** параметра `merge_strategy` нет — это единственная и правильная реализация.

## Context (from discovery)

- **Основной файл**: `airflow_provider_google_sheets/operators/write.py` — метод `_execute_smart_merge`
- **Удаляемые методы**: `_adjust_row_indices`, `_adjust_post_insert_indices` — больше не нужны (нет позиционных вставок)
- **Тесты**: `tests/test_operators/test_write.py` — класс `TestSmartMerge`, `TestSmartMergeRowIndexAdjustment` — нужно переписать
- **Changelog**: `CHANGELOG.md`

## Technical Details

**Новая логика `_execute_smart_merge`:**

```
1. Читаем ключевую колонку из таблицы
2. Строим индекс: {key_val: [row_numbers]} (1-based)
3. Группируем входящие строки: {key_val: [rows]}
4. Для каждого key_val в incoming_groups:
   - если есть в existing_index → добавляем DELETE op для всех строк
   - добавляем все incoming rows в append_rows
5. Строки которых нет во входящих → не трогаем
6. Выполняем удаления (bottom-up, батч)
7. Делаем append всех новых строк
```

**Удаляются из `write.py`:**
- `_adjust_row_indices` — не нужен (нет вставок в-место)
- `_adjust_post_insert_indices` — не нужен
- `post_insert_updates` — не нужен
- insert structural ops — не нужны

**Остаётся:**
- `_group_contiguous` — нужен для группировки строк при удалении
- `_get_sheet_id` — нужен для deleteDimension
- `_batched_batch_update` — нужен для батч-удалений

## Implementation Steps

### Task 1: Переписать `_execute_smart_merge`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [ ] удалить логику позиционного обновления (overlap/updates)
- [ ] удалить логику insert structural ops
- [ ] новая логика: для каждого key_val из incoming — DELETE existing + добавить в append_rows
- [ ] удалить методы `_adjust_row_indices` и `_adjust_post_insert_indices`
- [ ] убедиться что `_group_contiguous`, `_get_sheet_id`, `_batched_batch_update` остаются

### Task 2: Переписать тесты smart_merge

**Files:**
- Modify: `tests/test_operators/test_write.py`

- [ ] удалить класс `TestSmartMergeRowIndexAdjustment` (тесты adjust_row_indices — метода больше нет)
- [ ] переписать `TestSmartMerge` под новую логику:
  - тест: ключ существует, одна строка → удаляется и добавляется в конец
  - тест: ключ существует, несколько строк → все удаляются, все новые в конец
  - тест: ключ не существует → строки добавляются в конец (без удалений)
  - тест: ключ есть в таблице, нет во входящих → строки не трогаются
  - тест: несколько ключей, часть обновляется, часть новые
  - тест: несколько ключей, удаления идут bottom-up
  - тест: пустые входящие → ничего не делается
  - тест: отсутствующий merge_key → ValueError
  - тест: merge_key не в headers → ValueError
- [ ] запустить тесты — должны пройти

### Task 3: Проверка и финализация

- [ ] запустить полный тест-сьют: `.venv/bin/python -m pytest tests/ -q`
- [ ] убедиться что все остальные тесты (overwrite, append, row_filter) проходят

### Task 4: Обновить документацию и версию

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `readme.md`
- Modify: `readme_ru.md`

- [ ] добавить секцию `v0.6.0` в `CHANGELOG.md` (breaking change → minor version)
- [ ] обновить описание `smart_merge` в `readme.md`
- [ ] обновить описание `smart_merge` в `readme_ru.md`
- [ ] закоммитить, создать тег `v0.6.0`

## Post-Completion

**Использование в DAG пользователя** (без изменений в коде DAG):
```python
write_to_sheets = GoogleSheetsWriteOperator(
    task_id="write_to_sheets",
    write_mode="smart_merge",
    merge_key="date",   # все строки за дату удаляются и заменяются новыми
    ...
)
```
