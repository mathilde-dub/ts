import os
import hashlib
import requests


class ArtifactoryClient:
    """
    Client Artifactory - FILE ONLY (version simplifiée stable)

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
    def _headers(self):
        return {
            "X-JFrog-Art-Api": self.api_key,
            "Accept": "application/json",
        }

    # =========================================================
    # BUILD FILE URL
    # =========================================================
    def _build_artifact_url(self, key: str):

        clean_key = key.strip().lstrip("/")

        if not clean_key:
            raise ValueError("key must not be empty")

        return f"{self.base_url}/{self.repository}/{clean_key}"

    # =========================================================
    # BUILD STORAGE API URL (listing)
    # =========================================================
    def _build_storage_url(self, path: str = ""):

        clean_path = path.strip().strip("/")

        if clean_path:
            return f"{self.base_url}/api/storage/{self.repository}/{clean_path}"

        return f"{self.base_url}/api/storage/{self.repository}"

    # =========================================================
    # SHA256 LOCAL
    # =========================================================
    def compute_sha256_file(self, file_path):

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
        file_path,
        key,
        content_type="text/csv",
        verify_checksum=True,
    ):

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

        returned_sha256 = None

        try:

            payload = response.json()

            returned_sha256 = (
                payload.get("checksums", {}).get("sha256")
                or payload.get("originalChecksums", {}).get("sha256")
            )

        except Exception:

            payload = {}

        if verify_checksum and returned_sha256:

            if returned_sha256.lower() != local_sha256.lower():

                raise RuntimeError(
                    "SHA256 mismatch between local file and Artifactory response"
                )

        return {
            "url": payload.get("downloadUri") or artifact_url,
            "key": key,
            "sha256": local_sha256,
            "status_code": response.status_code,
        }

    # =========================================================
    # DOWNLOAD FILE
    # =========================================================
    def download_file(self, key, output_path):

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

            for chunk in response.iter_content(8192):

                if chunk:
                    f.write(chunk)

        return {
            "url": artifact_url,
            "key": key,
            "output_path": output_path,
        }

    # =========================================================
    # LIST DIRECTORY
    # =========================================================
    def list_directory(self, path="", deep=0, list_folders=1):

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

        return payload.get("files", [])
