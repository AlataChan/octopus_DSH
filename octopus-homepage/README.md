# Octopus Homepage

English | [中文](README.zh.md)

Run this command from the repository root.

```sh
python3 octopus-homepage/preview.py
```

Open <http://127.0.0.1:4173/>. The preview server checks the size and SHA-256 of both installers against their release manifests in `dist` before serving local download links. Pass `--port` to use a different port.

Set `site/` as the static site root in EdgeOne Pages. Upload the installers to COS and enter each HTTPS download URL in `site/downloads.js`. When replacing an installer, match the version, commit, size, and SHA-256 to its release manifest. The site shows a pending status until download URLs are configured.

The homepage uses the repository's existing Team Room recording, product screenshots, and team deliverable example.
