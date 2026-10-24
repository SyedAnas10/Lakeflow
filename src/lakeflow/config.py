"""Settings come from environment variables only. No secrets live in code or in the repo."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    lake_backend: str  # "local" | "azure"
    lake_root: str  # used when lake_backend == "local"
    azure_storage_connection_string: str | None
    azure_storage_account_url: str | None  # set in Azure: managed identity, no keys
    lake_container: str
    queue_backend: str  # "memory" | "servicebus"
    servicebus_connection_string: str | None
    servicebus_namespace: str | None  # e.g. lakeflow-bus.servicebus.windows.net
    queue_name: str
    work_dir: str
    max_delivery_count: int
    contract: str


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        database_url=env("LAKEFLOW_DATABASE_URL", "sqlite:///./lakeflow.db"),
        lake_backend=env("LAKEFLOW_LAKE_BACKEND", "local"),
        lake_root=env("LAKEFLOW_LAKE_ROOT", "./.lake"),
        azure_storage_connection_string=env("AZURE_STORAGE_CONNECTION_STRING"),
        azure_storage_account_url=env("AZURE_STORAGE_ACCOUNT_URL"),
        lake_container=env("LAKEFLOW_LAKE_CONTAINER", "lake"),
        queue_backend=env("LAKEFLOW_QUEUE_BACKEND", "memory"),
        servicebus_connection_string=env("SERVICEBUS_CONNECTION_STRING"),
        servicebus_namespace=env("SERVICEBUS_NAMESPACE"),
        queue_name=env("LAKEFLOW_QUEUE_NAME", "ingest-jobs"),
        work_dir=env("LAKEFLOW_WORK_DIR", "./.work"),
        max_delivery_count=int(env("LAKEFLOW_MAX_DELIVERY_COUNT", "5")),
        contract=env("LAKEFLOW_CONTRACT", "contracts/business_partner.json"),
    )
