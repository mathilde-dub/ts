import os
import hashlib
from datetime import datetime, timedelta, timezone

import requests


class ArtifactoryClient:
    """
    Client Artifactory - FILE ONLY

    Variables d'environnement requises :
        ARTIFACTORY_URL=https://host/artifactory
        ARTIFACTORY_API_KEY=xxxxxxxx
        ARTIFACTORY_REPO=my-repo
    """

    def __init__(self, timeout: int = 60):
        self.base_url = os.getenv("ARTIFACTORY_URL", "").strip().rstrip("/")
        self.api_key = os.getenv("ARTIFACTORY_API_KEY", "").strip()
        self.repository = os.getenv("ARTIFACTORY_REPO", "").strip().strip("/")
        self.timeout = timeout

        if not self.base_url:
            raise ValueError("ARTIFACTORY_URL is required")
        if not self.api_key:
            raise ValueError("ARTIFACTORY_API_KEY is required")
        if not self.repository:
            raise ValueError("ARTIFACTORY_REPO is required")

    # =========================================================
    # HEADERS
    # =========================================================
    def _headers(self) -> dict:
        return {
            "X-JFrog-Art-Api": self.api_key,
            "Accept": "application/json",
        }

    # =========================================================
    # BUILD FILE URL
    # =========================================================
    def _build_artifact_url(self, key: str) -> str:
        clean_key = (key or "").strip().lstrip("/")

        if not clean_key:
            raise ValueError("key must not be empty")

        return f"{self.base_url}/{self.repository}/{clean_key}"

    # =========================================================
    # BUILD STORAGE API URL
    # =========================================================
    def _build_storage_url(self, path: str = "") -> str:
        clean_path = (path or "").strip().strip("/")

        if clean_path:
            return f"{self.base_url}/api/storage/{self.repository}/{clean_path}"

        return f"{self.base_url}/api/storage/{self.repository}"

    # =========================================================
    # SHA256 LOCAL
    # =========================================================
    def compute_sha256_file(self, file_path: str) -> str:
        if not file_path or not file_path.strip():
            raise ValueError("file_path must not be empty")

        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        sha256 = hashlib.sha256()

        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)

        return sha256.hexdigest()

    # =========================================================
    # UPLOAD FILE
    # Checksum vérifié avec la réponse du PUT uniquement
    # =========================================================
    def upload_file(
        self,
        file_path: str,
        key: str,
        content_type: str = "text/csv",
        verify_checksum: bool = True,
    ) -> dict:
        if not file_path or not file_path.strip():
            raise ValueError("file_path must not be empty")

        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        artifact_url = self._build_artifact_url(key)
        local_sha256 = self.compute_sha256_file(file_path)

        headers = self._headers()
        headers["Content-Type"] = content_type
        headers["X-Checksum-Sha256"] = local_sha256

        with open(file_path, "rb") as f:
            response = requests.put(
                artifact_url,
                headers=headers,
                data=f,
                timeout=self.timeout,
            )

        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Upload failed: {response.status_code} {response.text}"
            )

        payload = {}
        returned_sha256 = None

        try:
            payload = response.json()
            returned_sha256 = (
                (payload.get("checksums") or {}).get("sha256")
                or (payload.get("originalChecksums") or {}).get("sha256")
            )
        except Exception:
            payload = {}

        if verify_checksum:
            if not returned_sha256:
                raise RuntimeError(
                    "Upload succeeded but sha256 was not returned by Artifactory"
                )

            if returned_sha256.lower() != local_sha256.lower():
                raise RuntimeError(
                    "SHA256 mismatch between local file and Artifactory PUT response"
                )

        return {
            "url": payload.get("downloadUri") or artifact_url,
            "storage_url": payload.get("uri"),
            "key": key,
            "sha256": local_sha256,
            "status_code": response.status_code,
        }

    # =========================================================
    # VERIFY SHA256 AGAINST FILE
    # Vérifie via upload_file(..., verify_checksum=True)
    # =========================================================
    def verify_sha256_against_object(self, file_path: str, key: str) -> bool:
        result = self.upload_file(
            file_path=file_path,
            key=key,
            verify_checksum=True,
        )
        return bool(result.get("sha256"))

    # =========================================================
    # DOWNLOAD FILE
    # =========================================================
    def download_file(self, key: str, output_path: str) -> dict:
        if not output_path or not output_path.strip():
            raise ValueError("output_path must not be empty")

        artifact_url = self._build_artifact_url(key)

        parent = os.path.dirname(output_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        response = requests.get(
            artifact_url,
            headers=self._headers(),
            stream=True,
            timeout=self.timeout,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Download failed: {response.status_code} {response.text}"
            )

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return {
            "url": artifact_url,
            "key": key,
            "output_path": output_path,
        }

    # =========================================================
    # LIST DIRECTORY
    # deep=1 => récursif
    # max_results => limite côté Python
    # max_age_days => filtre les vieux fichiers si souhaité
    # =========================================================
    def list_directory(
        self,
        path: str = "",
        deep: int = 1,
        max_results: int = 100,
        max_age_days: int | None = None,
        list_folders: int = 1,
    ) -> dict:
        if deep not in (0, 1):
            raise ValueError("deep must be 0 or 1")

        if list_folders not in (0, 1):
            raise ValueError("list_folders must be 0 or 1")

        storage_url = self._build_storage_url(path)

        response = requests.get(
            storage_url,
            headers=self._headers(),
            params={
                "list": "1",
                "deep": deep,
                "listFolders": list_folders,
            },
            timeout=self.timeout,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"List directory failed: {response.status_code} {response.text}"
            )

        payload = response.json()
        files = payload.get("files", [])

        if max_age_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            filtered_files = []

            for item in files:
                last_modified = item.get("lastModified")

                if not last_modified:
                    continue

                try:
                    dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
                    if dt >= cutoff:
                        filtered_files.append(item)
                except Exception:
                    continue

            files = filtered_files

        if max_results is not None:
            files = files[:max_results]

        return {
            "path": path or "/",
            "uri": payload.get("uri"),
            "created": payload.get("created"),
            "files": files,
        }


###############################################
# Appel final upload
from services.artifactory_client import ArtifactoryClient
import os

artifactory_client = ArtifactoryClient()

artifact_key = f"DEMO/{os.path.basename(output_path)}"

result = artifactory_client.upload_file(
    file_path=output_path,
    key=artifact_key,
    content_type="text/csv",
    verify_checksum=True,
)

self.logger.info("CSV uploaded successfully to Artifactory and SHA256 verified")

return {
    "artifact_url": result["url"],
    "row_count": row_count,
    "sha256": result["sha256"],
}
###################################################
#from services.artifactory_client import ArtifactoryClient

artifactory_client = ArtifactoryClient()

result = artifactory_client.download_file(
    key="DEMO/test.csv",
    output_path="/tmp/test_download.csv",
)

print(result)
###################################################
#list
from services.artifactory_client import ArtifactoryClient

artifactory_client = ArtifactoryClient()

listing = artifactory_client.list_directory(
    path="DEMO",
    deep=1,
    max_results=100,
    max_age_days=7,
    list_folders=1,
)

print(listing["path"])
print(listing["uri"])
print(listing["created"])

for item in listing["files"]:
    print(item["uri"], "folder:", item["folder"])
#files only
files_only = [
    item for item in listing["files"]
    if not item.get("folder", False)
]
# dir only
folders_only = [
    item for item in listing["files"]
    if item.get("folder", False)
]
