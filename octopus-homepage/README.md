# Octopus Homepage

English | [中文](README.zh.md)

Run this command from the repository root.

```sh
python3 octopus-homepage/preview.py
```

Open <http://127.0.0.1:4173/>. The preview server checks the size and SHA-256 of both installers against their release manifests in `dist` before serving local download links. Pass `--port` to use a different port.

Set `site/` as the static site root in EdgeOne Pages. Upload the installers to COS and enter each HTTPS download URL in `site/downloads.js`. When replacing an installer, match the version, commit, size, and SHA-256 to its release manifest. The site shows a pending status until download URLs are configured.

The homepage uses the repository's existing Team Room recording, product screenshots, and team deliverable example.

## Publish new installers

Set up the publishing workstation once. Create a Python environment with the Tencent COS SDK and point the repository's commit hook to that interpreter.

```sh
python3 -m venv .git/octopus-cos
.git/octopus-cos/bin/python -m pip install 'cos-python-sdk-v5==1.9.45'
git config --local octopus.cosPython "$(git rev-parse --show-toplevel)/.git/octopus-cos/bin/python"
```

Provide `COS_SECRET_ID` and `COS_SECRET_KEY` in the shell that commits releases. For temporary CAM credentials, also provide `COS_SESSION_TOKEN`. Use a CAM identity scoped to uploads and object ACLs under `releases/` in `octopusapp-1322755166`. The hook runs on `master` and uses the repository's existing Lefthook setup. On macOS, for persistent credentials, save the SecretKey in a generic password item in Keychain Access named `octopus-cos-publisher`, with the SecretId as the account name. Run `git config --local octopus.cosSecretId YOUR_SECRET_ID` once.

Build and verify each platform you want to release and place its canonical installer and release manifest in `dist/`. Keep any platform you are leaving on the old release as-is. The source commit in each new manifest must already be in the current branch and descend from the currently published source for that platform. When you commit, the hook checks the files against their manifests, uploads changed installers to immutable paths, verifies anonymous access and stages `site/downloads.js` in the same commit. Push that commit to the repository and branch connected to EdgeOne. Commits without eligible new installers perform no upload. To publish installers alone, run `git commit -m "release: update installer downloads"` and then push.
