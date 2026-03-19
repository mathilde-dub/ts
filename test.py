import os
import hashlib
import requests


class ArtifactoryClient:
    """
    Client simple Artifactory - FILE ONLY

    Variables d'environnement :
        ARTIFACTORY_URL=https://.../artifactory
        ARTIFACTORY_API_KEY=xxxxxxxx
        ARTIFACTORY_REPO=my-repository
    """

    def __init__(self):
        self.base_url = os.getenv("ARTIFACTORY_URL", "").strip()
        self.api_key = os.getenv("ARTIFACTORY_API_KEY", "").strip()
        self.repository = os.getenv("ARTIFACTORY_REPO", "").strip()
        self.timeout = 60

        if not self.base_url:
            raise ValueError("ARTIFACTORY_URL is required")
        if not self.api_key:
            raise ValueError("ARTIFACTORY_API_KEY is required")
        if not self.repository:
            raise ValueError("ARTIFACTORY_REPO is required")

    # =========================================================
    # INTERNAL
    # =========================================================
    def _build_artifact_url(self, key: str) -> str:
        if key is None:
            raise ValueError("key must not be None")

        clean_key = key.strip().lstrip("/")
        if not clean_key:
            raise ValueError("key must not be empty")

        return (
            f"{self.base_url.rstrip('/')}/"
            f"{self.repository.strip('/')}/"
            f"{clean_key}"
        )

    def _build_storage_url(self, path: str = "") -> str:
        """
        URL API storage Artifactory pour lister un dossier.
        """
        clean_repo = self.repository.strip("/")
        clean_path = (path or "").strip().strip("/")

        if clean_path:
            return f"{self.base_url.rstrip('/')}/api/storage/{clean_repo}/{clean_path}"
        return f"{self.base_url.rstrip('/')}/api/storage/{clean_repo}"

    def _headers(self) -> dict:
        return {
            "X-JFrog-Art-Api": self.api_key
        }

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

        url = self._build_artifact_url(key)
        local_sha256 = self.compute_sha256_file(file_path)

        headers = self._headers()
        headers.update({
            "Content-Type": content_type,
            "X-Checksum-Sha256": local_sha256
        })

        with open(file_path, "rb") as f:
            response = requests.put(
                url,
                headers=headers,
                data=f,
                timeout=self.timeout
            )

        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Upload failed: status={response.status_code}, body={response.text}"
            )

        if verify_checksum:
            if not self.verify_sha256_against_object(key=key, expected_sha256=local_sha256):
                raise RuntimeError("SHA256 mismatch after upload")

        return {
            "url": url,
            "key": key,
            "sha256": local_sha256,
            "local_file": file_path,
            "status_code": response.status_code
        }

    # =========================================================
    # VERIFY SHA256
    # =========================================================
    def verify_sha256_against_object(self, key: str, expected_sha256: str) -> bool:
        if not expected_sha256 or not expected_sha256.strip():
            raise ValueError("expected_sha256 must not be empty")

        url = self._build_artifact_url(key)

        response = requests.head(
            url,
            headers=self._headers(),
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Verification failed: status={response.status_code}, body={response.text}"
            )

        remote_sha256 = (
            response.headers.get("X-Checksum-Sha256")
            or response.headers.get("x-checksum-sha256")
        )

        if not remote_sha256:
            raise RuntimeError("SHA256 header not found in Artifactory response")

        return remote_sha256.lower() == expected_sha256.lower()

    # =========================================================
    # DOWNLOAD FILE
    # =========================================================
    def download_file(self, key: str, output_path: str) -> dict:
        if not output_path or not output_path.strip():
            raise ValueError("output_path must not be empty")

        url = self._build_artifact_url(key)

        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        response = requests.get(
            url,
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
            "url": url,
            "key": key,
            "output_path": output_path
        }

    # =========================================================
    # LIST DIRECTORY
    # =========================================================
    def list_directory(
        self,
        path: str = "",
        deep: int = 0,
        list_folders: int = 1
    ) -> dict:
        """
        Liste le contenu d'un répertoire Artifactory.

        Exemples:
            list_directory()
            list_directory("DEMO")
            list_directory("DEMO/subfolder", deep=1)
        """
        if deep not in (0, 1):
            raise ValueError("deep must be 0 or 1")

        if list_folders not in (0, 1):
            raise ValueError("list_folders must be 0 or 1")

        url = self._build_storage_url(path)
        params = {
            "list": "",
            "deep": deep,
            "listFolders": list_folders
        }

        response = requests.get(
            url,
            headers=self._headers(),
            params=params,
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
