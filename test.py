import os
import hashlib
import requests


class ArtifactoryClient:
    """
    Client Artifactory - FILE ONLY

    Variables d'environnement possibles :
        ARTIFACTORY_URL=https://host/artifactory
        ARTIFACTORY_API_KEY=xxxxxxxx
        ARTIFACTORY_REPO=e64-generic-fircocontinuity

    Optionnel :
        ARTIFACTORY_VERIFY_SSL=true
        ARTIFACTORY_CA_BUNDLE=/path/to/ca.pem
    """

    def __init__(self, timeout: int = 60):
        self.base_url = os.getenv("ARTIFACTORY_URL", "").strip().rstrip("/")
        self.api_key = os.getenv("ARTIFACTORY_API_KEY", "").strip()
        self.repository = os.getenv("ARTIFACTORY_REPO", "").strip().strip("/")
        self.timeout = timeout

        verify_ssl_env = os.getenv("ARTIFACTORY_VERIFY_SSL", "true").strip().lower()
        ca_bundle = os.getenv("ARTIFACTORY_CA_BUNDLE", "").strip()

        if ca_bundle:
            self.verify = ca_bundle
        else:
            self.verify = verify_ssl_env not in ("false", "0", "no")

        if not self.base_url:
            raise ValueError("ARTIFACTORY_URL is required")
        if not self.api_key:
            raise ValueError("ARTIFACTORY_API_KEY is required")
        if not self.repository:
            raise ValueError("ARTIFACTORY_REPO is required")

    def _headers(self) -> dict:
        return {
            "X-JFrog-Art-Api": self.api_key,
            "Accept": "application/json",
        }

    def _build_artifact_url(self, key: str) -> str:
        clean_key = (key or "").strip().lstrip("/")
        if not clean_key:
            raise ValueError("key must not be empty")

        base = self.base_url

        # Si l'utilisateur a déjà mis le repo dans ARTIFACTORY_URL,
        # on n'ajoute pas le repo une 2e fois.
        if base.endswith("/" + self.repository):
            return f"{base}/{clean_key}"

        return f"{base}/{self.repository}/{clean_key}"

    def _build_storage_url(self, path: str = "") -> str:
        clean_path = (path or "").strip().strip("/")

        base = self.base_url
        if base.endswith("/" + self.repository):
            repo_base = base
        else:
            repo_base = f"{base}/{self.repository}"

        # api/storage sert au listing de dossier
        # ex: .../artifactory/api/storage/<repo>?list
        if clean_path:
            repo_name = repo_base.rsplit("/", 1)[-1]
            root = repo_base[: -(len(repo_name) + 1)]
            return f"{root}/api/storage/{repo_name}/{clean_path}"

        repo_name = repo_base.rsplit("/", 1)[-1]
        root = repo_base[: -(len(repo_name) + 1)]
        return f"{root}/api/storage/{repo_name}"

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
        headers["Content-Type"] = content_type
        headers["X-Checksum-Sha256"] = local_sha256

        with open(file_path, "rb") as f:
            data = f.read()

        response = requests.put(
            artifact_url,
            headers=headers,
            data=data,
            timeout=self.timeout,
            verify=self.verify
        )

        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Upload failed: url={artifact_url}, status={response.status_code}, body={response.text}"
            )

        payload = {}
        returned_sha256 = None

        # Chez toi, le curl renvoie du JSON avec checksums.sha256.
        # Si ce n'est pas le cas côté requests, on ne casse pas l'upload.
        try:
            payload = response.json()
        except ValueError:
            payload = {}

        if isinstance(payload, dict):
            returned_sha256 = (
                (payload.get("checksums") or {}).get("sha256")
                or (payload.get("originalChecksums") or {}).get("sha256")
            )

        if verify_checksum and returned_sha256:
            if returned_sha256.lower() != local_sha256.lower():
                raise RuntimeError(
                    "SHA256 mismatch between local file and Artifactory PUT response"
                )

        return {
            "url": payload.get("downloadUri") or artifact_url,
            "key": key,
            "sha256": local_sha256,
            "status_code": response.status_code,
            "response_text": response.text
        }

    def verify_sha256_against_object(self, file_path: str, key: str) -> bool:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        local_sha256 = self.compute_sha256_file(file_path)

        result = self.upload_file(
            file_path=file_path,
            key=key,
            verify_checksum=False
        )

        response_text = result.get("response_text", "")
        try:
            payload = requests.models.complexjson.loads(response_text)
        except Exception:
            return False

        remote_sha256 = (
            (payload.get("checksums") or {}).get("sha256")
            or (payload.get("originalChecksums") or {}).get("sha256")
        )

        if not remote_sha256:
            return False

        return remote_sha256.lower() == local_sha256.lower()

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
            timeout=self.timeout,
            verify=self.verify
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Download failed: url={artifact_url}, status={response.status_code}, body={response.text}"
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
            timeout=self.timeout,
            verify=self.verify
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"List directory failed: url={storage_url}, status={response.status_code}, body={response.text}"
            )

        payload = response.json()

        return {
            "path": path or "/",
            "uri": payload.get("uri"),
            "created": payload.get("created"),
            "files": payload.get("files", [])
        }






#####################################################################"
ARTIFACTORY_VERIFY_SSL=false


############################################################
from services.artifactory_client import ArtifactoryClient

client = ArtifactoryClient()

artifact_key = "DEMO/test.csv"

result = client.download_file(
    key=artifact_key,
    output_path="/tmp/test.csv"
)

print("Download OK")
print(result)
#################################################


from services.artifactory_client import ArtifactoryClient

client = ArtifactoryClient()

listing = client.list_directory("DEMO")

print("Listing complet :")
for item in listing["files"]:
    print(item)
#######################################################
#Only files 
listing = client.list_directory("DEMO")

files_only = [
    item["uri"]
    for item in listing["files"]
    if not item["folder"]
]

print("Fichiers :")
for f in files_only:
    print(f)
#########################################
# only dir
listing = client.list_directory("DEMO")

folders_only = [
    item["uri"]
    for item in listing["files"]
    if item["folder"]
]

print("Dossiers :")
for folder in folders_only:
    print(folder)
########################################
# recursif
listing = client.list_directory("DEMO", deep=1)

for item in listing["files"]:
    print(item["uri"], "| folder:", item["folder"])
