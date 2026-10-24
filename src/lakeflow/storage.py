"""Lake storage behind one small interface, so the pipeline never knows where bytes live.

LocalLake      a folder on disk (unit tests, laptops)
AzureBlobLake  Azure Blob Storage / ADLS Gen2 through the Blob API (Azurite locally)

Why the Blob API and not the DFS (Data Lake) API? Azurite, the local emulator, does not
support the hierarchical-namespace DFS endpoint. An ADLS Gen2 account answers on the Blob
endpoint too, so the same code runs locally and in Azure. In production you can switch to
the DFS client to get atomic directory renames and POSIX-style ACLs.

Large files are always streamed in chunks; nothing here reads a whole file into memory.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Protocol


class Lake(Protocol):
    def upload_file(self, local_path: Path, key: str) -> None: ...
    def download_file(self, key: str, local_path: Path) -> None: ...
    def write_text(self, key: str, text: str) -> None: ...
    def exists(self, key: str) -> bool: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete_prefix(self, prefix: str) -> None: ...


class LocalLake:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def upload_file(self, local_path: Path, key: str) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        shutil.copyfile(local_path, tmp)
        os.replace(tmp, target)  # atomic on one filesystem: readers never see half a file

    def download_file(self, key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._path(key), local_path)

    def write_text(self, key: str, text: str) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())

    def delete_prefix(self, prefix: str) -> None:
        shutil.rmtree(self._path(prefix), ignore_errors=True)


class AzureBlobLake:
    """Azure Blob Storage / ADLS Gen2.

    Locally: a connection string (Azurite). In Azure: the account URL plus
    DefaultAzureCredential, which picks up the container's managed identity, so there is
    no key to store, rotate or leak. Access is then granted with an RBAC role
    ("Storage Blob Data Contributor") on the account or container.
    """

    def __init__(self, connection_string: str | None, container: str,
                 account_url: str | None = None) -> None:
        from azure.storage.blob import BlobServiceClient

        if account_url:
            from azure.identity import DefaultAzureCredential

            service = BlobServiceClient(account_url, credential=DefaultAzureCredential())
        elif connection_string:
            service = BlobServiceClient.from_connection_string(connection_string)
        else:
            raise ValueError("set AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_CONNECTION_STRING")
        self.container = service.get_container_client(container)
        if not self.container.exists():
            self.container.create_container()

    def upload_file(self, local_path: Path, key: str) -> None:
        with open(local_path, "rb") as fh:
            # The SDK splits the stream into blocks and uploads them in parallel.
            self.container.upload_blob(key, fh, overwrite=True, max_concurrency=4)

    def download_file(self, key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as fh:
            self.container.download_blob(key, max_concurrency=4).readinto(fh)

    def write_text(self, key: str, text: str) -> None:
        self.container.upload_blob(key, text.encode("utf-8"), overwrite=True)

    def exists(self, key: str) -> bool:
        return self.container.get_blob_client(key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(b.name for b in self.container.list_blobs(name_starts_with=prefix))

    def delete_prefix(self, prefix: str) -> None:
        for name in self.list_keys(prefix):
            self.container.delete_blob(name)


def make_lake(backend: str, *, root: str, connection_string: str | None, container: str,
              account_url: str | None = None) -> Lake:
    if backend == "local":
        return LocalLake(root)
    if backend == "azure":
        return AzureBlobLake(connection_string, container, account_url)
    raise ValueError(f"unknown lake backend: {backend}")
