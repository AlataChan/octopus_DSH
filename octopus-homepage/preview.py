"""Serve the static homepage and the verified installer files on loopback."""

import argparse
from functools import partial
from hashlib import file_digest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
SITE = ROOT / "site"
FILES = {
    "macos": "octopus_DSH.dmg",
    "windows": "octopus_DSH_Setup.exe",
}


def preview_config():
    """Resolve installer identity from the current dist files and manifests."""
    installers = {}
    downloads = {}
    for platform, filename in FILES.items():
        artifact = ROOT.parent / "dist" / filename
        manifest = json.loads(artifact.with_suffix(artifact.suffix + ".release.json").read_text())
        with artifact.open("rb") as source:
            digest = file_digest(source, "sha256").hexdigest()
        if artifact.stat().st_size != manifest["artifact"]["bytes"] or digest != manifest["artifact"]["sha256"]:
            raise SystemExit(f"Installer differs from its release manifest: {artifact}")
        url = f"/__installers/{filename}"
        downloads[url] = artifact
        installers[platform] = {
            "url": url,
            "file": filename,
            "version": manifest["release"]["version"],
            "commit": manifest["release"]["commit"][:10],
            "bytes": manifest["artifact"]["bytes"],
            "sha256": digest,
        }
    return ("const installers = " + json.dumps(installers) + ";\n").encode(), downloads


class PreviewHandler(SimpleHTTPRequestHandler):
    """Expose only the static directory and two exact installer paths."""

    def __init__(self, *args, config, downloads, **kwargs):
        self.config = config
        self.downloads = downloads
        super().__init__(*args, directory=str(SITE), **kwargs)

    def send_head(self):
        path = urlsplit(self.path).path
        if path == "/downloads.js":
            self.send_response(200)
            self.send_header("Content-Type", "text/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(self.config)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return io.BytesIO(self.config)
        if path in self.downloads:
            artifact = self.downloads[path]
            source = artifact.open("rb")
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(artifact.stat().st_size))
            self.send_header("Content-Disposition", f'attachment; filename="{artifact.name}"')
            self.end_headers()
            return source
        return super().send_head()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=4173)
    args = parser.parse_args()
    config, downloads = preview_config()
    handler = partial(PreviewHandler, config=config, downloads=downloads)
    with ThreadingHTTPServer(("127.0.0.1", args.port), handler) as server:
        print(f"Octopus Homepage: http://127.0.0.1:{server.server_port}/", flush=True)
        server.serve_forever()
