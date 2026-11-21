"""Publish new desktop installers from dist and stage their website metadata."""

import base64
from dataclasses import dataclass
from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
DOWNLOADS = Path("octopus-homepage/site/downloads.js")
BUCKET = "octopusapp-1322755166"
REGION = "ap-hongkong"
COS_HOST = f"{BUCKET}.cos.{REGION}.myqcloud.com"
TARGETS = {
    "macos": ("octopus_DSH.dmg", "macos-arm64"),
    "windows": ("octopus_DSH_Setup.exe", "windows-x64"),
}


@dataclass(frozen=True)
class Installer:
    platform: str
    path: Path
    commit: str
    version: str
    size: int
    sha256: str

    @property
    def key(self):
        return f"releases/{self.commit}/{self.sha256}/{self.path.name}"

    def website_entry(self, url):
        return {
            "url": url,
            "file": self.path.name,
            "version": self.version,
            "commit": self.commit[:10],
            "bytes": self.size,
            "sha256": self.sha256,
        }


def downloads_source(installers):
    return "// Installer URLs and release metadata for the download page.\nconst installers = " + json.dumps(installers, indent=2) + ";\n"


def candidates(dist, installers, newer):
    releases = []
    for platform, (filename, target) in TARGETS.items():
        path = dist / filename
        manifest = dist / f"{filename}.release.json"
        if not manifest.is_file():
            continue
        record = json.loads(manifest.read_text())
        artifact = record["artifact"]
        published = installers[platform]
        if artifact["sha256"] == published["sha256"]:
            continue
        version = record["release"]["version"]
        commit = record["release"]["commit"]
        digest = artifact["sha256"]
        if (record["schemaVersion"] != 1 or record["target"] != target
                or artifact["name"] != filename or not re.fullmatch(r"[a-f0-9]{40}", commit)
                or not re.fullmatch(r"[a-f0-9]{64}", digest)):
            raise ValueError(f"Unexpected release manifest for {filename}")
        if not newer(published["commit"], commit):
            continue
        size = artifact["bytes"]
        with path.open("rb") as source:
            if path.stat().st_size != size or file_digest(source, "sha256").hexdigest() != digest:
                raise ValueError(f"{filename} differs from its release manifest")
        releases.append(Installer(platform, path, commit, version, size, digest))
    return releases


def publish(installers, releases, upload):
    updated = installers.copy()
    for release in releases:
        url = upload(release)
        updated[release.platform] = release.website_entry(url)
    return updated


def verify_public_object(url, release, *, missing_ok=False):
    try:
        with urlopen(Request(url, method="HEAD"), timeout=15) as response:
            if (response.status != 200 or response.headers.get("Content-Length") != str(release.size)
                    or response.headers.get("x-cos-meta-sha256") != release.sha256):
                raise RuntimeError(f"COS object differs from the release manifest: {release.key}")
    except HTTPError as error:
        if missing_ok and error.code in (403, 404):
            return False
        raise
    return True


def cos_credentials():
    secret_id = os.environ.get("COS_SECRET_ID")
    secret_key = os.environ.get("COS_SECRET_KEY")
    if secret_id and secret_key:
        return secret_id, secret_key, os.environ.get("COS_SESSION_TOKEN")
    if secret_id or secret_key:
        raise RuntimeError("Set both COS_SECRET_ID and COS_SECRET_KEY in the publishing shell")
    if sys.platform == "darwin":
        account = git("config", "--get", "octopus.cosSecretId", check=False).stdout.strip()
        if account:
            lookup = subprocess.run(
                ["security", "find-generic-password", "-s", "octopus-cos-publisher", "-a", account, "-w"],
                capture_output=True, text=True, check=False,
            )
            if lookup.returncode == 0 and lookup.stdout.strip():
                return account, lookup.stdout.strip(), None
    raise RuntimeError("Configure the Octopus COS publishing credentials in the environment or macOS Keychain")


def make_uploader():
    secret_id, secret_key, token = cos_credentials()
    try:
        from qcloud_cos import CosConfig, CosS3Client
    except ModuleNotFoundError as error:
        raise RuntimeError("Install cos-python-sdk-v5 in the configured Python environment") from error
    client = CosS3Client(CosConfig(
        Region=REGION, Scheme="https", SecretId=secret_id, SecretKey=secret_key, Token=token,
    ))

    def upload(release):
        url = f"https://{COS_HOST}/{release.key}"
        if not verify_public_object(url, release, missing_ok=True):
            with release.path.open("rb") as source:
                if file_digest(source, "sha256").hexdigest() != release.sha256:
                    raise ValueError(f"{release.path.name} changed while publishing")
                source.seek(0)
                content_md5 = base64.b64encode(file_digest(source, "md5").digest()).decode("ascii")
                source.seek(0)
                client.put_object(
                    Bucket=BUCKET, Key=release.key, Body=source, ACL="public-read",
                    StorageClass="STANDARD", ContentMD5=content_md5, ForbidOverwrite="true",
                    ContentType="application/octet-stream",
                    ContentDisposition=f'attachment; filename="{release.path.name}"',
                    CacheControl="public, max-age=31536000, immutable",
                    Metadata={"x-cos-meta-sha256": release.sha256},
                )
            verify_public_object(url, release)
        print(f"Verified public download: {url}")
        return url

    return upload


def git(*args, check=True):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=check)


def newer(previous, candidate):
    if candidate.startswith(previous):
        return False
    return all(git("merge-base", "--is-ancestor", a, b, check=False).returncode == 0
               for a, b in ((previous, candidate), (candidate, "HEAD")))


def main():
    if git("symbolic-ref", "--quiet", "--short", "HEAD", check=False).stdout.strip() != "master":
        return
    indexed = git("show", f":{DOWNLOADS.as_posix()}").stdout
    match = re.search(r"^const installers = (\{.*\});\s*\Z", indexed, re.MULTILINE | re.DOTALL)
    if not match:
        raise ValueError("The staged downloads.js does not contain the expected installer metadata")
    installers = json.loads(match[1])
    releases = candidates(ROOT / "dist", installers, newer)
    if not releases:
        return
    destination = ROOT / DOWNLOADS
    if destination.read_text() != indexed:
        raise RuntimeError("Stage or save your downloads.js edits before publishing installers")
    updated = publish(installers, releases, make_uploader())
    destination.write_text(downloads_source(updated))
    try:
        git("add", "--", DOWNLOADS.as_posix())
    except subprocess.CalledProcessError:
        destination.write_text(indexed)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        sys.exit(f"Octopus installer publication failed: {error}")
