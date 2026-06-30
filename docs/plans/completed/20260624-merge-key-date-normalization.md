# Merge Key Date Normalization Without Schema

> **Scope note**: это PR1 — **только bugfix**, минимальным инлайн-изменением в
> существующих `schema.py`/`write.py`. Архитектурный вынос merge-логики в
> отдельные модули (`utils/merge_key.py`, `utils/merge_planner.py`) вынесен в
> отдельный последующий план
> `docs/plans/20260625-merge-extraction-refactor.md`, который делается **после**
> того как этот фикс залит и зелёный. Цель разделения — не блокировать срочный
> прод-фикс (дубли) за рефакторингом с большей площадью регрессии.

## Overview

`GoogleSheetsWriteOperator` (`write_mode="merge"`) молча создаёт дубликаты строк,
если `merge_key` — date-колонка и кто-то меняет формат отображения этого
столбца в самой таблице (вручную или другим процессом).

Корневая причина: `utils/schema.py:normalize_merge_key()` в первой строке делает
`if key_schema is None: return raw` — то есть если вызывающий код не передал
`schema` для merge_key-колонки оператору, уже реализованная (v0.10.0,
`normalize_merge_key_format`) расширенная нормализация (`format` → `input_format`
→ serial-число) вообще не вызывается. Обнаружено в проде на
`kortros_ch_to_sheets_calls_cpa.py`, где `schema` для merge_key не передавалась.

Дополнительно, даже если `schema` передана, чтение ключевой колонки всегда идёт с
`date_time_render_option="FORMATTED_STRING"` — то есть возвращается **отображаемая**
строка по текущему формату ячейки, а не устойчивое к смене формата serial-число.
Из-за этого даже шаг 3 (serial-fallback) практически никогда не достижим для
настоящих дата-ячеек.

**Цель фикса**: merge по date-ключу должен быть безопасен **даже без явной
`schema`** для стандартного ISO-формата (`YYYY-MM-DD`), и устойчив к смене
формата отображения столбца (включая кейс, когда ячейка из-за сброса формата
показывает serial-число вместо даты, например `46023` вместо `2026-01-01`).

**Сознательно вне scope**: schema-free инференс для `datetime`-ключей. Инцидент
из `todo.md` — про `date`-колонку; `datetime` без `schema` никто не просил, а
serial→datetime decode теряет время суток (известное ограничение), что дало бы
только половинчатую гарантию ("работает только если время 00:00:00") — хуже,
чем явное отсутствие гарантии. `datetime` с явной `schema` продолжает работать
как в v0.10.0, без изменений.

> **Важное уточнение после review** (см. `merge-plans-review.md`): чтение
> key_range через `SERIAL_NUMBER` применяется **не безусловно**, а только когда
> `normalize_merge_key_format=True` И эффективный `key_schema.get("type") ==
> "date"`. Иначе — `FORMATTED_STRING`, как сегодня. Без этого условия:
> (а) `normalize_merge_key_format=False` перестал бы быть строго legacy (raw
> читался бы как serial-число вместо отображаемой строки даже без нормализации
> поверх), (б) явная `schema` с `type="datetime"` и форматом, совпадающим с
> текущим отображением ячейки (уже рабочий сегодня случай), сломалась бы —
> Step 1 перестал бы парсить serial-число как строку формата, и normalize
> съезжал бы на Step 3, который обрезает время суток. Детали — в Technical
> Details и Edge cases.

## Context (from discovery)

- `airflow_provider_google_sheets/operators/write.py`
  - `GoogleSheetsWriteOperator._execute_merge` (~строка 395-549) — читает
    ключевую колонку через `hook.get_values(self.spreadsheet_id, key_range)`
    (строка 425) без явного `date_time_render_option` (используется дефолт хука
    `"FORMATTED_STRING"`). `key_col_idx` (строка 414) и `rows` (параметр
    функции) доступны уже на этом шаге — значит, можно вычислить `key_schema`
    *до* чтения, а не после. Строит `existing_index` через
    `self._normalize_sheet_key(str(row[0]))` (строка 445). Группирует incoming
    по сырым ключам `str(row[key_col_idx])` (строка 451) — incoming-сторона НЕ
    нормализуется.
  - `_normalize_sheet_key(self, raw: str)` (~строка 180) — тонкая обёртка, читает
    `self.schema` заново на каждой строке, вызывает `normalize_merge_key`.
  - `normalize_merge_key_format: bool = True` — существующий флаг-переключатель
    "умной" нормализации (введён в `docs/plans/20260605-normalize-merge-key-format.md`).
- `airflow_provider_google_sheets/utils/schema.py`
  - `normalize_merge_key(raw, key_schema, extended=True)` — существующая функция с
    цепочкой fallback (`format` → `input_format` → serial-число), бейлаутится сразу
    при `key_schema is None`.
  - `_SHEETS_EPOCH = date(1899, 12, 30)` — уже определена константа эпохи.
  - `apply_schema_to_value`, `format_value_for_write` — уже есть, используются
    `normalize_merge_key` без изменений.
- `airflow_provider_google_sheets/hooks/google_sheets.py`
  - `get_values(..., date_time_render_option: str = "FORMATTED_STRING")` — параметр
    уже принимается методом, просто `write.py` его не переопределяет на вызове для
    key_range.
- `_format_rows` (`write.py:165`): если `self.schema is None` → возвращает `rows`
  без изменений; если schema есть, но merge_key в ней нет →
  `format_row_for_write` отдаёт `str(value)` для этой колонки
  (`schema.py:205`, ветка `else`). В обоих случаях значение merge_key, которое
  увидит инференс, идентично тому, что идёт в `incoming_groups`.
- Тесты вызовов `get_values` в `tests/test_operators/test_write.py` проверяют
  только позиционный `range` (`call_args[0][1]`), kwargs дата-рендера не
  проверяются — смена дефолта не ломает существующие тесты автоматически.
- Прошлый аналогичный фикс (`docs/plans/20260605-normalize-merge-key-format.md`,
  коммит `fd78f90`) — тот же стиль: новая функция в `schema.py` + тонкая обёртка в
  `write.py` + смок-тесты в `test_write.py` + unit-тесты в `test_schema.py`.
- `CONTEXT.md` — глоссарий: **Merge Key**, **Schema**, **Inferred Schema**.
  Используется как единообразная терминология.

## Development Approach

- **Testing approach**: Regular (код → тесты в рамках каждой задачи), как и в
  прошлом аналогичном плане по этой же области кода.
- **Минимальное инлайн-изменение** — никакого выноса в новые модули в этом PR.
  `infer_date_key_schema` добавляется рядом с `normalize_merge_key` в `schema.py`;
  priority-логика инлайнится в `_execute_merge`.
- Полная обратная совместимость: при `normalize_merge_key_format=False` —
  поведение строго legacy (raw как есть при отсутствии явной `schema`, только
  `format`-парсинг при наличии `schema`).
- Никаких новых API-вызовов — только смена `date_time_render_option` в уже
  существующем вызове `get_values` для key_range.
- Никакой новой абстракции сверх необходимого — Inferred Schema прогоняется
  через уже существующий `normalize_merge_key()` без изменений сигнатуры этой
  функции.
- Запускать тесты после каждой задачи, не переходить к следующей с падающими
  тестами.

## Testing Strategy

- **Unit-тесты**: для `infer_date_key_schema` в `test_schema.py`.
- **Операторные/регрессионные тесты**: через `op.execute()` в `test_write.py`.
- **Регрессионные тесты**: числовой (не date) merge_key — авто-детект не
  запускается на ID-колонках.
- **Edge cases**: пустой список входящих значений, `normalize_merge_key_format=False`,
  явная "пустая" schema-запись блокирует инференс, datetime-подобные ключи.
- E2E/UI тестов в проекте нет (CLI/библиотека Airflow-провайдера) — не применимо.

## Progress Tracking

- Отмечать `[x]` сразу по завершении пункта.
- Новые обнаруженные задачи — с префиксом ➕.
- Блокеры — с префиксом ⚠️.

## Solution Overview

**Часть 1 — условно устойчивое чтение.** `key_schema` (явная `schema` либо
Inferred Schema) теперь резолвится **до** чтения key_range, а не после. На
основе результата `_execute_merge` выбирает `date_time_render_option`:
`"SERIAL_NUMBER"` только если `normalize_merge_key_format=True` И
`key_schema.get("type") == "date"`; во всех остальных случаях —
`"FORMATTED_STRING"` (как сегодня). Настоящие date-ячейки при этом условии
всегда отдают serial-число независимо от текущего формата отображения
столбца/локали. Текстовые, обычные числовые (не date) и **datetime** ключи не
затрагиваются — для них read остаётся прежним.

Почему именно так, а не безусловно (см. `merge-plans-review.md`):
- Если `normalize_merge_key_format=False`, render option всё равно обязан
  остаться `FORMATTED_STRING` — иначе флаг "выключить умную нормализацию"
  перестаёт означать "поведение не меняется" (raw-значение само по себе
  изменилось бы с отображаемой строки на число, даже не доходя до
  `normalize_merge_key`).
- `type="datetime"` исключён из `SERIAL_NUMBER` всегда (даже при
  `normalize_merge_key_format=True`): сегодня, если явная `schema` с
  `type="datetime"` и `format`, совпадающим с текущим отображением ячейки
  (рабочий happy path), Step 1 в `normalize_merge_key` парсит строку из
  `FORMATTED_STRING` напрямую — без потери времени суток. Под `SERIAL_NUMBER`
  Step 1 для datetime-ячейки гарантированно проваливается (raw — число, не
  строка формата), и normalize съезжает на Step 3, где `int(float(raw))`
  обрезает дробную часть суток — рабочий случай был бы сломан.

**Часть 2 — Inferred Schema без явной schema.** Новая функция
`infer_date_key_schema(values: list[str]) -> dict | None` в `utils/schema.py`
(рядом с `normalize_merge_key`): по форме **входящих** (текущего запуска)
значений merge_key строго определяет, является ли колонка ISO-датой
(`YYYY-MM-DD`). Если да — возвращает Inferred Schema
(`{"type": "date", "format": "%Y-%m-%d"}`), которая прогоняется через уже
существующий `normalize_merge_key()` без изменений в самой функции. Порог
строгий: **все** непустые входящие значения должны совпадать с шаблоном, иначе
— `None` (защита от false positive на ID-колонках). `datetime` не
поддерживается (возвращает `None`).

`key_schema` для merge вычисляется **один раз** в `_execute_merge` (а не на
каждой строке, как раньше) и передаётся в `normalize_merge_key` напрямую, минуя
старую версию `_normalize_sheet_key`, которая читала `self.schema` в цикле.

**Ключевой инвариант сопоставления** (почему это вообще работает): incoming-
сторона merge **не** нормализуется — `incoming_groups` строится по сырым
`str(row[key_col_idx])`. Нормализуется только existing-сторона. Совпадение
достигается потому, что inferred `format` (`%Y-%m-%d`) даёт ровно ту же строку,
что и сырой входящий ISO-ключ: и serial-число `46023.0`, и обычная ISO-строка
`2026-01-01` из ячейки нормализуются в `"2026-01-01"`, что равно входящему
`"2026-01-01"`. Это держится by construction — схему вывели из тех же ISO-строк.

Флаг `normalize_merge_key_format` остаётся единым переключателем всей "умной"
нормализации: при `False` инференс не запускается вообще (как и расширенный
fallback при наличии явной `schema` — поведение не меняется относительно v0.10.0).

## Technical Details

**Функция инференса** (`utils/schema.py`, рядом с `normalize_merge_key`) — с
валидацией реального календарного значения через `date.fromisoformat()`, не
только формы строки (`date` уже импортирован в `schema.py` для
`_SHEETS_EPOCH`):

```python
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def infer_date_key_schema(values: list[str]) -> dict | None:
    samples = [v for v in values if v]
    if not samples:
        return None
    for v in samples:
        if not _ISO_DATE_RE.match(v):
            return None
        try:
            date.fromisoformat(v)
        except ValueError:
            return None
    return {"type": "date", "format": "%Y-%m-%d"}
```

Без этой проверки `_ISO_DATE_RE` пропустил бы `"9999-99-99"` (валидно по форме,
невалидно как дата). Проверил цепочку `normalize_merge_key` для этого случая —
она не ломается даже без проверки (Step 1/3 проваливаются на невалидном
значении, Step 4 возвращает raw, сравнение raw-vs-raw остаётся корректным), но
`date.fromisoformat()` делает функцию точной по смыслу, а не только по форме,
и не даёт `logger.info` врать про "looks like a date column" на мусорных
данных.

**Инлайн в `_execute_merge`** — `key_schema` теперь резолвится **до** чтения
key_range (а не после, как в первой версии этого плана), потому что от него
зависит выбор `date_time_render_option`. Вставляется сразу после вычисления
`key_range` (после текущей строки 423), перед текущим вызовом `get_values`
(строка 425):

```python
explicit_key_schema = (self.schema or {}).get(self.merge_key)
incoming_key_strs = [
    str(r[key_col_idx]) for r in rows
    if key_col_idx < len(r) and r[key_col_idx] not in (None, "")
]
key_schema = explicit_key_schema
if key_schema is None and self.normalize_merge_key_format:
    key_schema = infer_date_key_schema(incoming_key_strs)
    if key_schema is not None:
        logger.info(
            "merge_key '%s' looks like a date column (no schema provided) — "
            "applying automatic date normalization",
            self.merge_key,
        )

# SERIAL_NUMBER только когда есть реальная нужда декодировать serial-число
# как дату (type="date", инференс включён). "datetime" сюда НЕ входит —
# decode через int(float()) обрезает время суток, что сломало бы уже рабочий
# explicit-schema-datetime случай. normalize_merge_key_format=False — всегда
# FORMATTED_STRING (строгий легаси, raw-значение не меняется вообще).
use_serial_number = (
    self.normalize_merge_key_format
    and key_schema is not None
    and key_schema.get("type") == "date"
)
logger.info("Reading key column from %s", key_range)
existing_keys_raw = hook.get_values(
    self.spreadsheet_id, key_range,
    date_time_render_option="SERIAL_NUMBER" if use_serial_number else "FORMATTED_STRING",
)
```

Дальше — без изменений (запись headers при пустом листе), кроме цикла
построения `existing_index`, который заменяет
`self._normalize_sheet_key(str(row[0]))` на прямой вызов с уже резолвленным
`key_schema`:

```python
existing_index: dict[str, list[int]] = defaultdict(list)
if self.has_headers:
    data_rows = existing_keys_raw[1:]
    start_row_num = table_start_row + 1
else:
    data_rows = existing_keys_raw
    start_row_num = table_start_row
for row_num, row in enumerate(data_rows, start=start_row_num):
    if row:
        key_val = normalize_merge_key(
            str(row[0]), key_schema, extended=self.normalize_merge_key_format
        )
        existing_index[key_val].append(row_num)
```

Метод `_normalize_sheet_key` убирается (его логика теперь инлайнится один раз,
перед чтением key_range, а не перед циклом индексации). `incoming_groups` и
остальная часть `_execute_merge` — без изменений.

**Чтение key_range**: при `use_serial_number=True` (только `type="date"` +
флаг включён) для настоящей date-ячейки приходит число (serial), например
`46023.0`. Существующий шаг 3 в `normalize_merge_key` (`int(float(raw))`) уже
корректно обрабатывает как `"46023"`, так и `"46023.0"`. Во всех остальных
случаях (текст, число-не-дата, `datetime`, `normalize_merge_key_format=False`)
— `FORMATTED_STRING`, raw-значение идентично сегодняшнему.

**Edge cases**:

- Пустой список входящих ключей → `infer_date_key_schema` возвращает `None` →
  поведение как сегодня.
- Один "кривой" входящий ключ среди тысяч ISO-дат → строгий порог "все" отключает
  инференс целиком — сознательный трейдофф.
- `normalize_merge_key_format=False` → ветка инференса не выполняется —
  полностью legacy-поведение.
- Явная, но "пустая" с точки зрения дат запись в `schema` (например
  `{"type": "str"}`) для merge_key блокирует Inferred Schema, т.к.
  `.get(self.merge_key)` вернёт не-`None` — приоритет явного выбора пользователя
  (зафиксировано в `CONTEXT.md`).
- `datetime`-подобные входящие значения (`"2026-01-01 12:30:00"`) →
  `infer_date_key_schema` возвращает `None` — сознательно не покрывается.
- Дробная часть времени суток в serial-числе при `type="datetime"` (явная
  `schema`) — известное ограничение `normalize_merge_key`, не меняется. Не
  обостряется этим планом: `datetime` всегда читается через `FORMATTED_STRING`
  (см. выбор `use_serial_number` выше), Step 3 для datetime остаётся таким же
  труднодостижимым, как и сегодня — не лучше, но и не хуже.
- Синтаксически валидный, но календарно невалидный ISO-шаблон
  (`"9999-99-99"`) → `infer_date_key_schema` теперь отклоняет через
  `date.fromisoformat()`, а не только через regex.
- Распознаём только `YYYY-MM-DD` без `schema`; другие форматы и `datetime`
  без явной `schema`/`input_format` — не поддерживаются, документируется в readme.

## Implementation Steps

### Task 1: `infer_date_key_schema` в `utils/schema.py`

**Files:**
- Modify: `airflow_provider_google_sheets/utils/schema.py`
- Modify: `tests/test_utils/test_schema.py`

- [x] добавить `_ISO_DATE_RE` и `infer_date_key_schema(values: list[str]) ->
  dict | None` рядом с `normalize_merge_key`
- [x] написать тесты в `test_schema.py`: ISO-даты (`["2026-01-01",
  "2026-01-02"]`) → `{"type": "date", "format": "%Y-%m-%d"}`
- [x] написать тест: ISO-datetime-подобные строки (`["2026-01-01 12:30:00"]`)
  → `None` (datetime не поддерживается, подтверждаем явно)
- [x] написать тест: смешанные/произвольные значения (`["2026-01-01", "abc"]`)
  → `None`
- [x] написать тест: пустой список и список из одних пустых строк → `None`
- [x] написать тест: числовые ID (`["1001", "1002"]`) → `None`
- [x] написать тест: синтаксически валидный, но календарно невалидный шаблон
  (`["9999-99-99"]`, а также `["2026-02-30"]` — несуществующий день) → `None`
  (проверяет, что `date.fromisoformat()`, а не только regex, отклоняет мусор)
- [x] запустить: `python -m pytest tests/test_utils/test_schema.py -x -q` —
  должны пройти перед Task 2

### Task 2: SERIAL_NUMBER read + инлайн Inferred Schema в `_execute_merge`

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py`

- [x] добавить `infer_date_key_schema` в импорт из `utils.schema`
- [x] в `_execute_merge` переместить резолв `key_schema` (явная `schema` →
  инференс, если её нет и `normalize_merge_key_format=True`) на место **до**
  вызова `hook.get_values(...)` (строка 425), сразу после вычисления
  `key_range` (строка 423) — порядок важен, см. Technical Details
- [x] вычислить `use_serial_number = self.normalize_merge_key_format and
  key_schema is not None and key_schema.get("type") == "date"`; передать
  `date_time_render_option="SERIAL_NUMBER" if use_serial_number else
  "FORMATTED_STRING"` в `hook.get_values(...)`
- [x] добавить `logger.info` при срабатывании авто-детекта (когда
  `explicit_key_schema is None and key_schema is not None`)
- [x] заменить блок построения `existing_index` (строки 435-446): прямой вызов
  `normalize_merge_key` с уже резолвленным `key_schema` вместо
  `self._normalize_sheet_key(str(row[0]))`
- [x] удалить метод `_normalize_sheet_key`
- [x] написать тест: merge без `schema`, входящие ключи `"2026-01-01"`/
  `"2026-01-02"`, `get_values` возвращает существующую строку с числом
  (`46023`) в колонке ключа → строка матчится и обновляется, а не дублируется
  (прямое регрессионное покрытие бага из `todo.md`); дополнительно проверить
  вызов `get_values` с `date_time_render_option="SERIAL_NUMBER"` в этом случае
- [x] написать тест: merge без `schema`, числовой НЕ-датовый merge_key
  (`"1001"`/`"1002"`) → поведение не меняется, никакого date-decode, и
  `get_values` вызван с `date_time_render_option="FORMATTED_STRING"`
- [x] написать тест: явная "пустая" запись `schema={"date": {"type": "str"}}`
  для merge_key с ISO-датами во входящих → Inferred Schema НЕ применяется,
  read остаётся `FORMATTED_STRING` (тип не `"date"`)
- [x] написать тест (regression guard для review finding #1): merge без
  `schema`, `normalize_merge_key_format=False`, входящие ISO-даты →
  `get_values` вызван с `date_time_render_option="FORMATTED_STRING"` (не
  `SERIAL_NUMBER`) — флаг действительно даёт строго legacy-чтение, не только
  legacy-нормализацию
- [x] написать тест (regression guard для review finding #2): явная
  `schema={"date": {"type": "datetime", "format": "..."}}` (как в
  `TestMergeInputFormat`/`TestNormalizeMergeKeyFormat`, но `type="datetime"`),
  существующий ключ в таблице с ненулевым временем суток, формат совпадает с
  отображением ячейки → ключ нормализуется и матчится **без потери времени
  суток** (`get_values` вызван с `FORMATTED_STRING`, не `SERIAL_NUMBER`) —
  доказывает, что уже рабочий explicit-datetime случай не сломан
- [x] написать тест: merge без `schema`, входящие ключи `"2026-01-01 12:30:00"`
  (datetime-подобные) → инференс не срабатывает (`key_schema is None`), читаем
  `FORMATTED_STRING`, поведение как раньше
- [x] запустить: `python -m pytest tests/test_operators/test_write.py -x -q` —
  должны пройти перед Task 3

### Task 3: Verify acceptance criteria

- [x] перепроверить сценарий из `todo.md` (kortros): merge без `schema`,
  колонка `date`, входящие ISO-даты, существующая ячейка читается как
  serial-число → дубликатов не возникает
  (`TestMergeSchemaFreeDateInference::test_iso_date_key_no_schema_serial_number_in_sheet_matches`,
  `test_write.py:2080` — passes; `deleted=1`, `appended=2`,
  `date_time_render_option="SERIAL_NUMBER"`)
- [x] перепроверить, что merge-сценарии с `schema` и
  `normalize_merge_key_format=True/False` из прошлого плана
  (`docs/plans/completed/20260605-normalize-merge-key-format.md`) проходят без
  изменений в поведении (`TestNormalizeMergeKeyFormat`, `test_write.py:1976` —
  3/3 tests pass)
- [x] перепроверить `TestSmartMerge` (`test_write.py:341`) — там merge без
  `schema` с ISO-датами (`"2024-04-01"`); убедиться, что инференс даёт
  identity-преобразование и не меняет `deleted`/`appended` счётчики
  (19/19 tests pass, counters unchanged)
- [x] явно подтвердить оба фикса из `merge-plans-review.md`: (1)
  `normalize_merge_key_format=False` всегда читает `FORMATTED_STRING`,
  независимо от формы входящих ключей; (2) явная `schema` с `type="datetime"`
  и ненулевым временем суток в существующем ключе продолжает матчиться без
  потери времени (`FORMATTED_STRING`, не `SERIAL_NUMBER`)
  (`test_normalize_merge_key_format_false_forces_formatted_string` and
  `test_explicit_datetime_schema_matching_format_keeps_formatted_string`,
  `test_write.py:2140` and `:2156` — both pass)
- [x] запустить полный набор тестов: `python -m pytest tests/ -x -q`
  (470 passed)

### Task 4: [Final] Документация

**Files:**
- Modify: `airflow_provider_google_sheets/operators/write.py` (docstring)
- Modify: `readme.md`
- Modify: `readme_ru.md`
- Modify: `CHANGELOG.md`
- Modify: `todo.md`

- [x] обновить docstring `normalize_merge_key_format` в `write.py` — упомянуть,
  что флаг теперь также включает/выключает schema-free инференс (Inferred
  Schema) по форме входящих ISO-дат, а не только extended-fallback при
  наличии явной `schema`
- [x] обновить `readme.md` (раздел про merge_key/normalize_merge_key_format,
  включая уточнение неточной формулировки в строке 574 про "without any
  additional schema configuration"): описать, что merge по date-ключу теперь
  безопасен без `schema` для ISO-формата (`YYYY-MM-DD`), явно указать границы
  — другие форматы дат и `datetime`-ключи без `schema`/`input_format` не
  поддерживаются
- [x] аналогично обновить `readme_ru.md`
- [x] добавить запись в `CHANGELOG.md` под заголовком `## v0.11.0` (minor bump)
  с описанием фикса и его scope (date-only schema-free инференс; `datetime`
  без `schema` не поддерживается)
- [x] отметить пункт про merge_key/даты в `todo.md` как решённый
- [ ] переместить этот файл в `docs/plans/completed/`

## Post-Completion

**Follow-up refactor** (отдельный план, после залития этого фикса):
- `docs/plans/20260625-merge-extraction-refactor.md` — вынос merge-логики в
  `utils/merge_key.py` и `utils/merge_planner.py` на уже зелёном коде.

**Manual verification**:
- На реальной таблице: записать данные с merge по date-ключу без `schema`,
  вручную сменить формат отображения столбца (Date → Number → обратно),
  прогнать merge повторно — убедиться, что дубликатов не появляется.
- Применить фикс к `kortros_ch_to_sheets_calls_cpa.py` (project-kortros) после
  релиза новой версии провайдера — проверить, что инцидент не повторяется.
