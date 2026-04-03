# airflow-provider-google-sheets

Apache Airflow провайдер для Google Sheets API v4. Чтение, запись и управление таблицами Google Sheets из DAG-ов Airflow.

---

> **Раскрытие информации об ИИ:** Этот провайдер разработан при участии **Claude Code** (Anthropic, модель **Claude Opus 4.6**). Код, тесты и документация написаны совместно разработчиком и LLM. Оценивайте качество кода по его содержанию и принимайте осознанное решение о том, использовать ли его в своих проектах.

---

## Возможности

- **Чтение** данных из Google Sheets с потоковой обработкой чанками, конвертацией типов по схеме и выводом в CSV/JSON/JSONL/XCom
- **Запись** данных в трёх режимах: перезапись, добавление и merge (upsert по ключу)
- **Merge** — обновление, вставка и удаление строк по ключевому столбцу с корректным пересчётом индексов
- **Управление** таблицами — создание новых spreadsheet, листов, получение списка листов с фильтрацией и авто-создание листов при записи
- **Партиционированная запись** — разбивка данных по листам на основе значения колонки с использованием Airflow dynamic task mapping
- **Большие датасеты** — потоковое чтение/запись без накопления данных в памяти
- **Поддержка схем** — автоматическая конвертация типов (date, int, float, bool) при чтении и записи
- **Обработка заголовков** — дедупликация, транслитерация кириллицы (по умолчанию), удаление спецсимволов, приведение к нижнему регистру, нормализация в snake_case

## Установка

```bash
pip install airflow-provider-google-sheets
```

С поддержкой транслитерации кириллических заголовков:

```bash
pip install airflow-provider-google-sheets[transliterate]
```

## Требования

- Python >= 3.10
- Apache Airflow 2.x (>= 2.7, протестировано на 2.9.1; Airflow 3.x не тестировался)
- Сервисный аккаунт Google с доступом к Sheets API

## Настройка подключения

1. Создайте сервисный аккаунт Google Cloud с включённым **Google Sheets API**.
2. Скачайте JSON-файл ключа.
3. В интерфейсе Airflow создайте подключение одним из способов:

### Вариант A: Стандартное подключение Google Cloud (рекомендуется)

Используйте, если у вас уже настроено подключение `google_cloud_platform` в Airflow.

- **Conn Id**: `google_cloud_default`
- **Conn Type**: `Google Cloud`
- **Keyfile Path**: `/path/to/service-account.json`
- **Scopes**: `https://www.googleapis.com/auth/spreadsheets` (добавьте другие при необходимости)

### Вариант B: Inline JSON-ключ

- **Conn Id**: `google_cloud_default`
- **Conn Type**: `Google Cloud` или `google_sheets`
- **Keyfile JSON**: вставьте полный JSON сервисного аккаунта

### Вариант C: JSON в поле Extra

- **Conn Id**: `google_cloud_default`
- **Conn Type**: `google_sheets`
- **Extra**: вставьте полный JSON-ключ или используйте `{"keyfile_dict": <JSON-ключ>}`

Хук проверяет учётные данные в следующем порядке: `key_path` / `keyfile_path` (файл на диске) → `keyfile_dict` (inline JSON) → сырой JSON из Extra.

## Операторы

### GoogleSheetsReadOperator

Чтение данных из таблицы.

```python
from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

# Базовое чтение — возвращает list[dict] через XCom
read = GoogleSheetsReadOperator(
    task_id="read_sheets",
    spreadsheet_id="your-spreadsheet-id",
    sheet_name="Sheet1",
)

# Потоковое чтение большой таблицы в CSV (без накопления в памяти)
read_csv = GoogleSheetsReadOperator(
    task_id="read_to_csv",
    spreadsheet_id="your-spreadsheet-id",
    output_type="csv",
    output_path="/tmp/export.csv",
    chunk_size=10000,
)

# Потоковое чтение в JSONL (один JSON-объект на строку, экономит память)
read_jsonl = GoogleSheetsReadOperator(
    task_id="read_to_jsonl",
    spreadsheet_id="your-spreadsheet-id",
    output_type="jsonl",
    output_path="/tmp/export.json",
    chunk_size=10000,
)

# Чтение в JSON array файл
read_json = GoogleSheetsReadOperator(
    task_id="read_to_json",
    spreadsheet_id="your-spreadsheet-id",
    output_type="json",
    output_path="/tmp/export.json",
)

# Чтение с конвертацией типов
read_typed = GoogleSheetsReadOperator(
    task_id="read_typed",
    spreadsheet_id="your-spreadsheet-id",
    schema={
        "date": {"type": "date", "format": "%Y-%m-%d"},
        "revenue": {"type": "float", "required": True},
        "quantity": {"type": "int"},
    },
)

# По умолчанию: заголовки транслитерируются, очищаются от спецсимволов
# и приводятся к нижнему регистру.
# "Дата отчёта" → "data_otchyota", "Клиент (ФИО)" → "klient_fio"
read_default = GoogleSheetsReadOperator(
    task_id="read_default",
    spreadsheet_id="your-spreadsheet-id",
)

# column_mapping имеет приоритет — вся остальная обработка заголовков
# пропускается, ключи маппинга используют оригинальные имена из таблицы.
read_mapped = GoogleSheetsReadOperator(
    task_id="read_mapped",
    spreadsheet_id="your-spreadsheet-id",
    output_type="jsonl",
    output_path="/tmp/export.json",
    column_mapping={
        "Дата": "date",
        "Клиент": "client",
        "Сумма": "amount",
    },
)

# Отключение всей обработки заголовков — оригинальные имена из таблицы
read_raw = GoogleSheetsReadOperator(
    task_id="read_raw",
    spreadsheet_id="your-spreadsheet-id",
    transliterate_headers=False,
    sanitize_headers=False,
    lowercase_headers=False,
)

# Пропуск строк со статусом "deleted" и остановка чтения на "ИТОГО"
read_filtered = GoogleSheetsReadOperator(
    task_id="read_filtered",
    spreadsheet_id="your-spreadsheet-id",
    row_skip={"column": "status", "value": "deleted"},
    row_stop={"column": "name", "value": "ИТОГО"},
)

# Пропуск по нескольким условиям — строка пропускается если совпадает ХОТЯ БЫ одно (OR-логика)
read_multi_skip = GoogleSheetsReadOperator(
    task_id="read_multi_skip",
    spreadsheet_id="your-spreadsheet-id",
    row_skip=[
        {"column": "status", "value": "deleted"},
        {"column": "status", "value": "archived"},
        {"column": "amount", "op": "empty"},
    ],
)

# row_stop тоже принимает список — остановка при совпадении любого условия
read_stop_multi = GoogleSheetsReadOperator(
    task_id="read_stop_multi",
    spreadsheet_id="your-spreadsheet-id",
    row_stop=[
        {"column": "name", "value": "ИТОГО"},
        {"column": "type", "op": "starts_with", "value": "total_"},
    ],
)

# Фильтрация строк по значению столбца (include-фильтр)
# filter_column указывается по ОБРАБОТАННОМУ имени заголовка
# (после transliterate/sanitize/lowercase/column_mapping)
read_city = GoogleSheetsReadOperator(
    task_id="read_moscow",
    spreadsheet_id="your-spreadsheet-id",
    filter_column="city",
    filter_value="Moscow",
)

# Фильтрация по нескольким значениям (OR-логика)
read_cities = GoogleSheetsReadOperator(
    task_id="read_two_cities",
    spreadsheet_id="your-spreadsheet-id",
    filter_column="city",
    filter_value=["Moscow", "Berlin"],
)

# Динамический fan-out: чтение данных по каждому городу в отдельных mapped tasks
# "Город" в таблице + column_mapping → filter_column="city"
from airflow_provider_google_sheets.operators.manage import GoogleSheetsUniqueValuesOperator

cities = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities",
    spreadsheet_id="your-spreadsheet-id",
    column="city",
    column_mapping={"Город": "city"},
    exclude_values=[""],   # исключить пустые ячейки
)
read_by_city = GoogleSheetsReadOperator.partial(
    task_id="read_by_city",
    spreadsheet_id="your-spreadsheet-id",
    column_mapping={"Город": "city"},
    filter_column="city",
).expand(filter_value=cities.output)
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | ID подключения Airflow |
| `spreadsheet_id` | str | — | ID таблицы |
| `sheet_name` | str | `None` | Имя листа (None = первый лист) |
| `cell_range` | str | `None` | Диапазон в формате A1 (None = весь лист) |
| `has_headers` | bool | `True` | Первая строка содержит заголовки |
| `transliterate_headers` | bool | `True` | Транслитерировать кириллицу в латиницу |
| `sanitize_headers` | bool | `True` | Удалить пробелы и спецсимволы (оставить буквы, цифры, `_`) |
| `lowercase_headers` | bool | `True` | Привести заголовки к нижнему регистру |
| `normalize_headers` | bool | `False` | Нормализовать в snake_case (перекрывает `sanitize` + `lowercase`) |
| `column_mapping` | dict | `None` | Переименование по оригинальным именам: `{"Исходный": "new_name"}`. Пропускает всю обработку |
| `schema` | dict | `None` | Схема типов столбцов |
| `strip_strings` | bool | `False` | Удалять пробелы в начале и конце строковых значений ячеек |
| `row_skip` | dict \| list[dict] | `None` | Пропуск строк по условию. Строка пропускается если совпадает **хотя бы одно** условие (OR). Один dict или список: `{"column": "status", "value": "deleted", "op": "equals"}` |
| `row_stop` | dict \| list[dict] | `None` | Остановка чтения на первой совпадающей строке (совпавшая строка тоже отбрасывается). Принимает один dict или список — останавливается при совпадении **любого** условия. Дальнейшие API-вызовы не делаются |
| `chunk_size` | int | `5000` | Строк за один API-запрос |
| `output_type` | str | `"xcom"` | `"xcom"`, `"csv"`, `"json"` (JSON array) или `"jsonl"` (объект на строку) |
| `output_path` | str | `None` | Путь к файлу для csv/json/jsonl |
| `max_xcom_rows` | int | `50000` | Максимум строк для XCom |
| `max_xcom_bytes` | int | `None` | Максимальный размер XCom-payload в байтах. Вызывает ошибку при превышении. `None` = без ограничения (WARNING всё равно выводится при >5 МБ) |
| `filter_column` | str | `None` | Имя столбца для include-фильтра (обработанное имя заголовка). Указывается вместе с `filter_value` |
| `filter_value` | str \| list[str] | `None` | Значение(я) для включения. OR-логика при списке. Поддерживает Jinja-шаблоны и динамический маппинг через `expand(filter_value=...)` |

**Поддерживаемые значения `op` для `row_skip` / `row_stop`:**

| `op` | Описание | `value` обязателен |
|---|---|---|
| `equals` (по умолчанию) | Точное совпадение строки | да |
| `not_equals` | Не равно | да |
| `contains` | Ячейка содержит подстроку | да |
| `not_contains` | Ячейка не содержит подстроку | да |
| `starts_with` | Ячейка начинается с value | да |
| `ends_with` | Ячейка заканчивается на value | да |
| `empty` | Ячейка пустая или None | нет |
| `not_empty` | Ячейка не пустая | нет |

Все сравнения выполняются над строковым представлением значения ячейки. Если указанный `column` отсутствует в строке, условие молча игнорируется (строка не пропускается/не останавливается по этому условию).

### GoogleSheetsWriteOperator

Запись данных в таблицу.

```python
from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

# Перезапись с list[dict]
write = GoogleSheetsWriteOperator(
    task_id="write_sheets",
    spreadsheet_id="your-spreadsheet-id",
    sheet_name="Output",
    write_mode="overwrite",
    data=[{"date": "2024-01-01", "value": 100}],
)

# Добавление строк
append = GoogleSheetsWriteOperator(
    task_id="append_sheets",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="append",
    data=[{"event": "login", "user": "alice"}],
)

# Merge по ключу
merge = GoogleSheetsWriteOperator(
    task_id="merge",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="merge",  # "smart_merge" принимается как псевдоним
    merge_key="date",
    data=[
        {"date": "2024-01-01", "value": 110},  # обновление существующей
        {"date": "2024-01-03", "value": 200},  # добавление новой
    ],
)

# Таблица начинается не с A1 (например, с C3)
# При первом запуске заголовок записывается в C3; ключевая колонка отсчитывается от C
merge_offset = GoogleSheetsWriteOperator(
    task_id="merge_offset",
    spreadsheet_id="your-spreadsheet-id",
    sheet_name="Отчёт",
    write_mode="merge",
    merge_key="date",
    table_start="C3",   # заголовок таблицы находится в ячейке C3
    data=[{"date": "2024-01-01", "revenue": 110}],
)
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | ID подключения Airflow |
| `spreadsheet_id` | str | — | ID таблицы |
| `sheet_name` | str | `None` | Имя листа |
| `cell_range` | str | `None` | Целевой диапазон A1 (режим перезаписи) |
| `write_mode` | str | `"overwrite"` | `"overwrite"`, `"append"`, `"merge"` (псевдоним: `"smart_merge"`) |
| `clear_mode` | str | `"sheet"` | Стратегия очистки при перезаписи: `"sheet"` очищает весь лист и удаляет лишние строки; `"range"` очищает только столбцы данных |
| `data` | Any | `None` | Данные: list[list], list[dict] или путь к файлу |
| `data_xcom_task_id` | str | `None` | Получить данные из XCom этого таска |
| `data_xcom_key` | str | `"return_value"` | Ключ XCom |
| `has_headers` | bool | `True` | Данные содержат заголовки |
| `write_headers` | bool | `True` | Записывать строку заголовков. В режимах `append`/`merge` заголовки записываются автоматически при пустом листе |
| `schema` | dict | `None` | Схема для форматирования значений |
| `batch_size` | int | `1000` | Строк за один API-запрос |
| `pause_between_batches` | float | `1.0` | Пауза между батчами (секунды) |
| `merge_key` | str | `None` | Ключевой столбец для режима merge |
| `table_start` | str | `"A1"` | Верхний левый угол таблицы (например, `"C3"`). Используется в `append` и `merge` для определения строки заголовка и абсолютных позиций колонок. В режиме `overwrite` игнорируется — используется `cell_range` |
| `create_sheet_if_missing` | bool | `False` | При `True` автоматически создаёт лист если он не существует. Безопасно для параллельного выполнения — конкурентные попытки создания обрабатываются корректно |
| `partition_by` | str | `None` | Имя колонки для фильтрации данных перед записью. Записываются только строки, где значение колонки совпадает с `partition_value` |
| `partition_value` | str | `None` | Значение для сопоставления с колонкой `partition_by`. Обязателен если задан `partition_by` |
| `column_mapping` | dict | `None` | Переименование заголовков перед записью: `{"исходная_колонка": "Заголовок в таблице"}`. Применяется после всей фильтрации — `merge_key`, `partition_by` и `schema` всегда используют **оригинальные** имена колонок из входных данных |

**Форматы входных данных:**
- `list[dict]` — заголовки определяются автоматически из ключей
- `list[list]` — сырые строки (установите `has_headers=True`, если первая строка — заголовок)
- `str` — путь к файлу (`.csv` читается как CSV; все остальные расширения — как JSONL по умолчанию)
- XCom — установите `data_xcom_task_id`

Формат файла определяется по расширению: `.csv` → CSV, всё остальное → JSONL.
Для чтения JSON array файла используйте `source_type="json"` в `normalize_input_data()` или записывайте данные в формате JSONL.

### Алгоритм Merge

Merge считывает ключевой столбец из таблицы, сравнивает с входными данными и генерирует минимальный набор операций:

1. **Чтение** ключевого столбца для построения индекса `{значение_ключа: [номера_строк]}`
2. **Удаление** всех существующих строк для каждого ключа из входных данных (снизу вверх, чтобы избежать сдвигов индексов)
3. **Добавление** всех входящих строк через `values.append`
4. **Очистка** унаследованного форматирования на новых строках через `repeatCell`

### GoogleSheetsCreateSpreadsheetOperator

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsCreateSpreadsheetOperator

create = GoogleSheetsCreateSpreadsheetOperator(
    task_id="create_spreadsheet",
    title="Ежемесячный отчёт",
    sheet_titles=["Сводка", "Детали"],
)
# Возвращает spreadsheet_id через XCom
```

### GoogleSheetsCreateSheetOperator

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsCreateSheetOperator

add_sheet = GoogleSheetsCreateSheetOperator(
    task_id="add_sheet",
    spreadsheet_id="your-spreadsheet-id",
    sheet_title="НовыйЛист",
)
```

### GoogleSheetsListSheetsOperator

Получение списка листов (вкладок) таблицы с опциональной фильтрацией. Возвращает `list[str]`, совместим с dynamic task mapping Airflow.

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsListSheetsOperator

# Получить все листы
list_sheets = GoogleSheetsListSheetsOperator(
    task_id="list_sheets",
    spreadsheet_id="your-spreadsheet-id",
)

# Фильтрация по regex и использование с dynamic task mapping
list_data_sheets = GoogleSheetsListSheetsOperator(
    task_id="list_data_sheets",
    spreadsheet_id="your-spreadsheet-id",
    name_pattern=r"^Data",          # включить только листы, начинающиеся с "Data"
    exclude_pattern=r"_archive$",   # исключить листы, заканчивающиеся на "_archive"
    index_range=(0, 10),            # только первые 10 листов
)

# Dynamic task mapping — чтение каждого листа параллельно
read_each = GoogleSheetsReadOperator.partial(
    task_id="read_each",
    spreadsheet_id="your-spreadsheet-id",
).expand(sheet_name=list_data_sheets.output)
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | ID подключения Airflow |
| `spreadsheet_id` | str | — | ID таблицы |
| `name_pattern` | str | `None` | Regex для включения листов по имени (`re.search`) |
| `exclude_pattern` | str | `None` | Regex для исключения листов по имени (`re.search`) |
| `index_range` | tuple[int, int] | `None` | Срез по позиции `(start, end)`, 0-based, start включительно, end исключительно |

### GoogleSheetsExtractPartitionsOperator

Извлекает уникальные значения партиции из данных и возвращает список `{"sheet_name", "partition_value"}` для Airflow `expand_kwargs`. **Не обращается к Google Sheets API** — работает только с данными в памяти.

Основной сценарий — запись данных на разные листы, где каждое уникальное значение колонки соответствует своему листу.

```python
from airflow_provider_google_sheets.operators.manage import (
    GoogleSheetsExtractPartitionsOperator,
)
from airflow_provider_google_sheets.operators.write import GoogleSheetsWriteOperator

# Возвращает [{"sheet_name": "Отчёт 2026-01", "partition_value": "2026-01"}, ...]
partitions = GoogleSheetsExtractPartitionsOperator(
    task_id="get_partitions",
    data_xcom_task_id="fetch_data",   # или data="/path/to/file.jsonl"
    partition_column="period",
    sheet_name_template="Отчёт {value}",   # опционально, по умолчанию "{value}"
)

# Запись каждой партиции на свой лист — один Airflow task на партицию
write = GoogleSheetsWriteOperator.partial(
    task_id="write_to_sheet",
    spreadsheet_id="your-spreadsheet-id",
    data_xcom_task_id="fetch_data",
    partition_by="period",          # фильтрация данных внутри каждого task
    create_sheet_if_missing=True,   # создать лист если не существует
    write_mode="overwrite",
).expand_kwargs(partitions.output)
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `partition_column` | str | — | Имя колонки, уникальные значения которой определяют партиции |
| `sheet_name_template` | str | `"{value}"` | Шаблон имени листа. Используйте `{value}` как placeholder |
| `data` | Any | `None` | Данные: `list[dict]`, `list[list]` или путь к файлу (`.jsonl`, `.csv`) |
| `data_xcom_task_id` | str | `None` | Получить данные из XCom этого task |
| `data_xcom_key` | str | `"return_value"` | Ключ XCom |
| `has_headers` | bool | `True` | Должен быть `True` — поиск колонки по имени требует заголовков |

**Возвращает:** `list[dict]` — по одной записи на уникальное значение партиции, в порядке первого появления:
```python
[
    {"sheet_name": "Отчёт 2026-01", "partition_value": "2026-01"},
    {"sheet_name": "Отчёт 2026-02", "partition_value": "2026-02"},
]
```

### GoogleSheetsUniqueValuesOperator

Читает уникальные значения одного столбца из Google Sheets и возвращает `list[str]` в порядке первого вхождения. Предназначен для динамического маппинга тасков в Airflow — в отличие от `ExtractPartitionsOperator`, обращается к API напрямую (не работает с данными в памяти).

Параметр `column` указывается по **обработанному** имени заголовка (после transliterate / sanitize / lowercase / column_mapping — такая же обработка, как в `GoogleSheetsReadOperator`).

```python
from airflow_provider_google_sheets.operators.manage import GoogleSheetsUniqueValuesOperator
from airflow_provider_google_sheets.operators.read import GoogleSheetsReadOperator

# Получить уникальные значения столбца "city" из таблицы
cities = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities",
    spreadsheet_id="your-spreadsheet-id",
    column="city",           # обработанное имя заголовка
    exclude_values=[""],     # исключить пустые ячейки
)

# Fan-out: чтение данных по каждому городу в параллельных mapped tasks
read_by_city = GoogleSheetsReadOperator.partial(
    task_id="read_by_city",
    spreadsheet_id="your-spreadsheet-id",
    filter_column="city",
).expand(filter_value=cities.output)

# С column_mapping — "Город" в таблице → "city" после маппинга
cities_mapped = GoogleSheetsUniqueValuesOperator(
    task_id="get_cities",
    spreadsheet_id="your-spreadsheet-id",
    column="city",                          # имя ПОСЛЕ маппинга
    column_mapping={"Город": "city"},
    exclude_values=[""],
)
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | ID подключения Airflow |
| `spreadsheet_id` | str | — | ID таблицы |
| `sheet_name` | str | `None` | Имя листа (None = первый лист) |
| `cell_range` | str | `None` | Диапазон в формате A1 (None = весь лист) |
| `column` | str | — | Обработанное имя заголовка столбца для извлечения уникальных значений |
| `exclude_values` | list[str] | `None` | Значения для исключения. Передайте `[""]` чтобы исключить пустые ячейки |
| `chunk_size` | int | `5000` | Строк за один API-запрос |
| `has_headers` | bool | `True` | Первая строка содержит заголовки |
| `transliterate_headers` | bool | `True` | Транслитерировать кириллицу в латиницу |
| `sanitize_headers` | bool | `True` | Удалить пробелы и спецсимволы |
| `lowercase_headers` | bool | `True` | Привести заголовки к нижнему регистру |
| `column_mapping` | dict | `None` | Переименование по оригинальным именам. Пропускает всю остальную обработку |

**Возвращает:** `list[str]` — уникальные значения в порядке первого вхождения.

## Схема

Определение типов столбцов для автоматической конвертации при чтении/записи:

```python
schema = {
    "date": {"type": "date", "format": "%Y-%m-%d", "required": True},
    "revenue": {"type": "float", "required": True},
    "quantity": {"type": "int"},
    "comment": {"type": "str"},
    "is_active": {"type": "bool"},
}
```

**Поддерживаемые типы:** `str`, `int`, `float`, `date`, `datetime`, `bool`

### Устойчивый парсинг числовых значений

Для числовых столбцов (`int`, `float`) добавьте `"default"` для мягкого парсинга.
Нечисловые значения заменяются на значение по умолчанию вместо ошибки:

```python
schema = {
    "revenue": {"type": "float", "default": None},   # "n/a", "-", "" → None
    "quantity": {"type": "int",   "default": 0},       # "n/a", "-", "" → 0
}
```

Мягкий режим также обрабатывает:
- Запятая как десятичный разделитель: `"1,2"` → `1.2`
- Удаление префиксов/суффиксов: `"1000.4 р."` → `1000.4`, `"10.2%"` → `10.2`

Без `"default"` сохраняется строгое поведение (ошибка при невалидных значениях).

### Раздельные форматы парсинга и записи для дат (`input_format`)

По умолчанию поле `format` используется и для **парсинга** входящих строк, и для **записи** в лист.
Если входные данные используют другой формат дат, чем лист (например, ISO `"2026-03-01"` из JSONL против `"01.03.2026"` в листе), используйте `input_format` чтобы задать формат парсинга отдельно:

```python
schema = {
    "date": {
        "type": "date",
        "input_format": "%Y-%m-%d",   # как парсить входящие строки (например, из JSONL)
        "format": "%d.%m.%Y",          # как записывать в лист
    }
}
```

Это особенно важно в режиме `merge`: без `input_format` входящий ключ `"2026-03-01"` и существующий ключ в листе `"01.03.2026"` не совпадут, что приведёт к дублированию строк при каждом запуске.

`input_format` влияет только на столбцы типа `date` и `datetime`. Для остальных типов (`str`, `int` и т.д.) не имеет эффекта.

## Примеры

Готовые примеры DAG-ов в директории `examples/`:

- `example_read.py` — чтение с различными конфигурациями
- `example_write.py` — режимы перезаписи и добавления
- `example_smart_merge.py` — сценарии merge
- `example_manage.py` — создание таблиц и листов
- `example_sheets_to_bigquery.py` — Google Sheets → BigQuery (перезапись, добавление, обновление по диапазону дат)
- `example_bigquery_to_sheets.py` — BigQuery → Google Sheets (перезапись, merge по дате)

## Лицензия

MIT License
