# Глубокие методы hook: delete_rows / sort_rows / clear_row_formatting (design-заготовка, follow-up)

> Статус: **DEFERRED / follow-up.** Кандидат №3 архитектурного ревью 2026-07-02.
> Осознанно отложен: у каждой формы запроса один продакшн-потребитель («один
> адаптер = гипотетический seam»), выгода в основном тестовая. Естественный
> момент реализации — **следующее касание merge-кода**, вместе с унификацией
> append-ноги merge (см. ниже). Не реализовывать в отрыве от той работы.

## Проблема

`GoogleSheetsHook` уже имеет два образцово глубоких метода — `ensure_rows`
(hooks/google_sheets.py:314, прячет gridProperties + appendDimension) и
`trim_sheet` (:343, прячет deleteDimension + математику keep_rows). Но три
другие структурные операции оператор собирает как сырой Google-JSON и шлёт в
generic `hook.batch_update`:

- `deleteDimension` — merge, удаление строк (write.py:818–830);
- `repeatCell` + 11-полевая строка `style_fields` — merge, сброс визуального
  форматирования с сохранением numberFormat (write.py:861–888);
- `sortRange` — серверная сортировка (write.py:1013–1024).

Симптом протечки seam: тестовый `FakeSheetsHook._apply_request`
(tests/test_operators/test_write.py:3743–3753) вынужден диспетчеризоваться по
ключам dict (`"deleteDimension" in req`, `"sortRange" in req`) — фейк
реверс-инжинирит протокол Google API, который не должен пересекать интерфейс
hook. Реальный риск: фейк интерпретирует dict чуть иначе, чем Google, и тест
«зелёный неправильно».

## Предлагаемый интерфейс (по образцу trim_sheet)

```python
# hooks/google_sheets.py
def delete_rows(self, spreadsheet_id, sheet_id,
                ranges: list[tuple[int, int]]) -> None:
    """0-based [start, end); один batchUpdate; порядок (bottom-up) — забота hook."""

def sort_rows(self, spreadsheet_id, sheet_id, *,
              start_row, end_row, start_col, end_col,
              specs: list[tuple[int, str]]) -> None:

def clear_row_formatting(self, spreadsheet_id, sheet_id, *,
                         start_row, end_row, start_col, end_col) -> None:
    """Сбрасывает визуальные стили, СОХРАНЯЯ numberFormat (style_fields внутри)."""
```

Оператор говорит «удали эти строки / отсортируй блок», формы запросов и
`style_fields` живут за seam'ом. `FakeSheetsHook` реализует три понятных метода
вместо парсера запросов.

## Открытые вопросы (решить при реализации)

1. **Батчинг.** `_batched_batch_update` (write.py:943–949) нарезает requests по
   `batch_size` с паузами. Кто батчит после разноса по методам: hook (тогда ему
   нужны `batch_size`/`pause_between_batches` — утяжеляет интерфейс) или
   оператор (теряется часть глубины)?
2. **Сигнатура sort_rows vs Written Extent.** К моменту реализации в кодовой
   базе будет `WrittenExtent` (см. CONTEXT.md) — вероятно, `sort_rows` должен
   принимать extent, а не 4 голых индекса.
3. **`_ensure_sheet_exists`** (write.py:362–380: metadata-check + гашение
   race-400) — по духу тоже глубокая способность hook, которую мог бы разделить
   `GoogleSheetsCreateSheetOperator` (manage.py:92–103 её не имеет). Рассмотреть
   перенос заодно.

## Связанная работа (делать вместе)

**Унификация append-ноги merge.** `_execute_merge` до сих пор использует
неидемпотентный `hook.append_values` (write.py:854) внутри retried-обёртки —
корректно только за счёт delete-then-append replay-семантики. После реализации
позиционного append (план 20260701-idempotent-append-design.md) эту ногу можно
перевести на ту же позиционную запись и убрать зависимость merge от
`append_values` вовсе. Это же касание merge — правильный момент для
`delete_rows`/`clear_row_formatting`.

## Ожидаемый эффект

- Seam оператор↔hook симметричен с `ensure_rows`/`trim_sheet` (достройка
  существующего стиля, не новый).
- `FakeSheetsHook` сокращается и перестаёт дублировать знание о формах запросов.
- 11-строчная конкатенация `style_fields` уходит из оператора.
- Поведение НЕ меняется — рефактор эквивалентной замены под существующими
  тестами.
