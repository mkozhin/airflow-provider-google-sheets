__version__ = "0.1.0"


def get_provider_info():
    return {
        "package-name": "airflow-provider-google-sheets",
        "name": "Google Sheets",
        "description": "Apache Airflow provider for Google Sheets.",
        "connection-types": [
            {
                "connection-type": "google_sheets",
                "hook-class-name": "airflow_google_sheets.hooks.google_sheets.GoogleSheetsHook",
            }
        ],
        "versions": [__version__],
    }
