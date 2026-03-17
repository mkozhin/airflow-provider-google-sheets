try:
    from airflow_provider_google_sheets._version import version as __version__
except ImportError:
    __version__ = "0.0.0-dev"


def get_provider_info():
    return {
        "package-name": "airflow-provider-google-sheets",
        "name": "Google Sheets",
        "description": "`Google Sheets <https://docs.google.com/spreadsheets/>`__ provider for Apache Airflow — read, write, and merge spreadsheets.",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Google Sheets",
                "external-doc-url": "https://developers.google.com/sheets/api",
                "tags": ["gcp", "service"],
            },
        ],
        "operators": [
            {
                "integration-name": "Google Sheets",
                "python-modules": [
                    "airflow_provider_google_sheets.operators.read",
                    "airflow_provider_google_sheets.operators.write",
                    "airflow_provider_google_sheets.operators.manage",
                ],
            },
        ],
        "hooks": [
            {
                "integration-name": "Google Sheets",
                "python-modules": [
                    "airflow_provider_google_sheets.hooks.google_sheets",
                ],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow_provider_google_sheets.hooks.google_sheets.GoogleSheetsHook",
                "connection-type": "google_sheets",
            },
        ],
    }
