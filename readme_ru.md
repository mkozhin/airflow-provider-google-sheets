# airflow-provider-google-sheets

Apache Airflow провайдер для Google Sheets API v4. Чтение, запись и управление таблицами Google Sheets из DAG-ов Airflow.

---

> **Раскрытие информации об ИИ:** Этот провайдер разработан при участии **Claude Code** (Anthropic, модель **Claude Opus 4.6**). Код, тесты и документация написаны совместно разработчиком и LLM. Оценивайте качество кода по его содержанию и принимайте осознанное решение о том, использовать ли его в своих проектах.

---

## Возможности

- **Чтение** данных из Google Sheets с потоковой обработкой чанками, конвертацией типов по схеме и выводом в CSV/JSON/XCom
- **Запись** данных в трёх режимах: перезапись, добавление и smart merge (upsert по ключу)
- **Smart merge** — обновление, вставка и удаление строк по ключевому столбцу с корректным пересчётом индексов
- **Управление** таблицами — создание новых spreadsheet и листов
- **Большие датасеты** — потоковое чтение/запись без накопления данных в памяти
- **Поддержка схем** — автоматическая конвертация типов (date, int, float, bool) при чтении и записи
- **Обработка заголовков** — дедупликация, транслитерация кириллицы, нормализация в snake_case

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
3. В интерфейсе Airflow создайте подключение:
   - **Conn Id**: `google_cloud_default`
   - **Conn Type**: `google_sheets`
   - **Extra**: вставьте полный JSON-ключ или используйте `{"keyfile_dict": <JSON-ключ>}`

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
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | ID подключения Airflow |
| `spreadsheet_id` | str | — | ID таблицы |
| `sheet_name` | str | `None` | Имя листа (None = первый лист) |
| `cell_range` | str | `None` | Диапазон в формате A1 (None = весь лист) |
| `has_headers` | bool | `True` | Первая строка содержит заголовки |
| `transliterate_headers` | bool | `False` | Транслитерировать кириллицу в латиницу |
| `normalize_headers` | bool | `False` | Нормализовать в snake_case |
| `schema` | dict | `None` | Схема типов столбцов |
| `chunk_size` | int | `5000` | Строк за один API-запрос |
| `output_type` | str | `"xcom"` | `"xcom"`, `"csv"` или `"json"` |
| `output_path` | str | `None` | Путь к файлу для csv/json |
| `max_xcom_rows` | int | `50000` | Максимум строк для XCom |

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

# Smart merge по ключу
merge = GoogleSheetsWriteOperator(
    task_id="smart_merge",
    spreadsheet_id="your-spreadsheet-id",
    write_mode="smart_merge",
    merge_key="date",
    data=[
        {"date": "2024-01-01", "value": 110},  # обновление существующей
        {"date": "2024-01-03", "value": 200},  # добавление новой
    ],
)
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `gcp_conn_id` | str | `"google_cloud_default"` | ID подключения Airflow |
| `spreadsheet_id` | str | — | ID таблицы |
| `sheet_name` | str | `None` | Имя листа |
| `cell_range` | str | `None` | Целевой диапазон A1 (режим перезаписи) |
| `write_mode` | str | `"overwrite"` | `"overwrite"`, `"append"`, `"smart_merge"` |
| `data` | Any | `None` | Данные: list[list], list[dict] или путь к файлу |
| `data_xcom_task_id` | str | `None` | Получить данные из XCom этого таска |
| `data_xcom_key` | str | `"return_value"` | Ключ XCom |
| `has_headers` | bool | `True` | Данные содержат заголовки |
| `write_headers` | bool | `True` | Записывать строку заголовков (режим перезаписи) |
| `schema` | dict | `None` | Схема для форматирования значений |
| `batch_size` | int | `1000` | Строк за один API-запрос |
| `pause_between_batches` | float | `1.0` | Пауза между батчами (секунды) |
| `merge_key` | str | `None` | Ключевой столбец для smart_merge |

**Форматы входных данных:**
- `list[dict]` — заголовки определяются автоматически из ключей
- `list[list]` — сырые строки (установите `has_headers=True`, если первая строка — заголовок)
- `str` — путь к CSV или JSON файлу
- XCom — установите `data_xcom_task_id`

### Алгоритм Smart Merge

Smart merge считывает ключевой столбец из таблицы, сравнивает с входными данными и генерирует минимальный набор операций:

1. **Чтение** ключевого столбца для построения индекса `{значение_ключа: [номера_строк]}`
2. **Сравнение** каждого ключа: одинаковое количество → обновление, больше входящих → вставка, меньше → удаление, новый ключ → добавление
3. **Сортировка** структурных операций снизу вверх (по убыванию номера строки) для предотвращения порчи индексов
4. **Выполнение** вставок/удалений через `batchUpdate`, затем пересчёт индексов строк для обновлений значений
5. **Запись** значений через `batch_update_values` для эффективности

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

## Примеры

Готовые примеры DAG-ов в директории `examples/`:

- `example_read.py` — чтение с различными конфигурациями
- `example_write.py` — режимы перезаписи и добавления
- `example_smart_merge.py` — сценарии smart merge
- `example_manage.py` — создание таблиц и листов
- `example_sheets_to_bigquery.py` — Google Sheets → BigQuery (перезапись, добавление, обновление по диапазону дат)
- `example_bigquery_to_sheets.py` — BigQuery → Google Sheets (перезапись, smart merge по дате)

## Лицензия

MIT License
