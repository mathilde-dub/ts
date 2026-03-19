import os
import hashlib
import requests


class ArtifactoryClient:
    """
    Client Artifactory - FILE ONLY

    Variables d'environnement requises :
        ARTIFACTORY_URL=https://artifactory.mycloud.intrabpce.fr:443/artifactory
        ARTIFACTORY_API_KEY=xxxxxxxxxxxxxxxx
        ARTIFACTORY_REPO=e64-generic-fircocontinuity
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
    # INTERNAL
    # =========================================================
    def _headers(self) -> dict:
        return {
            "X-JFrog-Art-Api": self.api_key
        }

    def _build_artifact_url(self, key: str) -> str:
        clean_key = (key or "").strip().lstrip("/")
        if not clean_key:
            raise ValueError("key must not be empty")

        return f"{self.base_url}/{self.repository}/{clean_key}"

    def _build_storage_url(self, path: str = "") -> str:
        clean_path = (path or "").strip().strip("/")
        if clean_path:
            return f"{self.base_url}/api/storage/{self.repository}/{clean_path}"
        return f"{self.base_url}/api/storage/{self.repository}"

    # =========================================================
    # SHA256
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
    # =========================================================
    def upload_file(
        self,
        file_path: str,
        key: str,
        content_type: str = "text/csv",
        verify_checksum: bool = True
    ) -> dict:
        if not file_path or not file_path.strip():
            raise ValueError("file_path must not be empty")

        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        artifact_url = self._build_artifact_url(key)
        local_sha256 = self.compute_sha256_file(file_path)

        headers = self._headers()
        headers.update({
            "Content-Type": content_type,
            "X-Checksum-Sha256": local_sha256
        })

        with open(file_path, "rb") as f:
            response = requests.put(
                artifact_url,
                headers=headers,
                data=f,
                timeout=self.timeout
            )

        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Upload failed: status={response.status_code}, body={response.text}"
            )

        try:
            payload = response.json()
        except ValueError:
            payload = {}

        returned_sha256 = None
        if isinstance(payload, dict):
            returned_sha256 = (
                (payload.get("checksums") or {}).get("sha256")
                or (payload.get("originalChecksums") or {}).get("sha256")
            )

        if verify_checksum:
            if returned_sha256:
                if returned_sha256.lower() != local_sha256.lower():
                    raise RuntimeError(
                        "SHA256 mismatch between local file and Artifactory response"
                    )
            else:
                remote_sha256 = self.get_remote_sha256(key)
                if remote_sha256 != local_sha256.lower():
                    raise RuntimeError("SHA256 mismatch after upload")

        return {
            "url": payload.get("downloadUri") if isinstance(payload, dict) else artifact_url,
            "storage_url": payload.get("uri") if isinstance(payload, dict) else None,
            "key": key,
            "sha256": local_sha256,
            "local_file": file_path,
            "status_code": response.status_code
        }

    # =========================================================
    # REMOTE SHA256
    # =========================================================
    def get_remote_sha256(self, key: str) -> str:
        storage_url = self._build_storage_url(key)

        response = requests.get(
            storage_url,
            headers=self._headers(),
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Get metadata failed: status={response.status_code}, body={response.text}"
            )

        payload = response.json()

        remote_sha256 = (
            (payload.get("checksums") or {}).get("sha256")
            or (payload.get("originalChecksums") or {}).get("sha256")
        )

        if not remote_sha256:
            raise RuntimeError("Remote sha256 not found in Artifactory metadata")

        return remote_sha256.lower()

    def verify_sha256_against_object(self, key: str, expected_sha256: str) -> bool:
        if not expected_sha256 or not expected_sha256.strip():
            raise ValueError("expected_sha256 must not be empty")

        remote_sha256 = self.get_remote_sha256(key)
        return remote_sha256 == expected_sha256.lower()

    # =========================================================
    # DOWNLOAD FILE
    # =========================================================
    def download_file(self, key: str, output_path: str) -> dict:
        if not output_path or not output_path.strip():
            raise ValueError("output_path must not be empty")

        artifact_url = self._build_artifact_url(key)

        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        response = requests.get(
            artifact_url,
            headers=self._headers(),
            stream=True,
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Download failed: status={response.status_code}, body={response.text}"
            )

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return {
            "url": artifact_url,
            "key": key,
            "output_path": output_path
        }

    # =========================================================
    # LIST DIRECTORY
    # =========================================================
    def list_directory(self, path: str = "", deep: int = 0, list_folders: int = 1) -> dict:
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
                "listFolders": list_folders
            },
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"List directory failed: status={response.status_code}, body={response.text}"
            )

        payload = response.json()

        return {
            "path": path or "/",
            "uri": payload.get("uri"),
            "created": payload.get("created"),
            "files": payload.get("files", [])
        }
        ####################################################################################################################################################################################################################################
        import os
from services.artifactory_client import ArtifactoryClient

artifactory_client = ArtifactoryClient()

try:
    if row_count == 0:
        raise ValueError("CSV file is empty")

    artifact_key = f"DEMO/{os.path.basename(output_path)}"

    result = artifactory_client.upload_file(
        file_path=output_path,
        key=artifact_key,
        content_type="text/csv",
        verify_checksum=True
    )

    self.logger.info("CSV uploaded successfully to Artifactory and SHA256 verified")

    return {
        "artifact_url": result["url"],
        "row_count": row_count,
        "sha256": result["sha256"]
    }

except Exception as e:
    self.logger.error(f"Failed to write/upload CSV to Artifactory: {e}", exc_info=True)
    raise RuntimeError(f"Failed to write/upload CSV to Artifactory: {e}")

finally:
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
            self.logger.info(f"Local file deleted: {output_path}")
    except Exception as cleanup_error:
        self.logger.warning(f"Failed to delete local file: {cleanup_error}")
