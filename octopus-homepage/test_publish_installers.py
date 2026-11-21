"""Regression tests for selecting and publishing Octopus download artifacts."""

import json
import subprocess
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import publish_installers as publisher

from publish_installers import candidates, downloads_source, publish


class PublishTests(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.dist = Path(root.name)
        self.old_commit = "a" * 40
        self.new_commit = "b" * 40
        self.installers = {"macos": {"url": "https://previous", "commit": self.old_commit[:10], "sha256": "0" * 64}}

    def artifact(self, name="octopus_DSH.dmg", target="macos-arm64", commit=None, data=b"installer"):
        path = self.dist / name
        path.write_bytes(data)
        digest = sha256(data).hexdigest()
        record = {
            "schemaVersion": 1,
            "target": target,
            "release": {"version": "1.2.3", "commit": commit or self.new_commit},
            "artifact": {"name": name, "bytes": len(data), "sha256": digest},
        }
        Path(str(path) + ".release.json").write_text(json.dumps(record))
        return path, digest

    def select(self):
        return candidates(self.dist, self.installers, lambda previous, new: previous == self.old_commit[:10] and new == self.new_commit)

    def test_old_or_absent_build_does_not_replace_download(self):
        self.assertFalse(self.select())
        self.artifact(commit=self.old_commit)
        self.assertFalse(self.select())

    def test_changed_binary_is_rejected_before_upload(self):
        path, _ = self.artifact()
        path.write_bytes(b"corrupted")
        with self.assertRaisesRegex(ValueError, "differs from"):
            self.select()

    def test_only_new_platform_changes_after_verified_upload(self):
        self.installers["windows"] = {"url": "https://previous/windows", "commit": self.old_commit[:10], "sha256": "1" * 64}
        path, digest = self.artifact()
        uploads = []
        updated = publish(self.installers, self.select(), lambda release: uploads.append(release) or "https://new/mac")
        self.assertEqual(uploads[0].path, path)
        self.assertEqual(uploads[0].key, f"releases/{self.new_commit}/{digest}/{path.name}")
        self.assertEqual(updated["macos"]["url"], "https://new/mac")
        self.assertEqual(updated["macos"]["sha256"], digest)
        self.assertEqual(updated["windows"], self.installers["windows"])
        self.assertIn("const installers = ", downloads_source(updated))

    def test_failed_upload_preserves_previous_website_metadata(self):
        self.artifact()
        def fail(_release):
            raise RuntimeError("denied")
        with self.assertRaisesRegex(RuntimeError, "denied"):
            publish(self.installers, self.select(), fail)
        self.assertEqual(self.installers["macos"]["url"], "https://previous")


    def test_git_hook_updates_only_the_download_file_in_the_index(self):
        root = self.dist
        subprocess.run(["git", "init", "--quiet", "--initial-branch=master", "--template=", str(root)], check=True)
        self.dist = root / "dist"
        self.dist.mkdir()
        source = root / publisher.DOWNLOADS
        source.parent.mkdir(parents=True)
        with patch.object(publisher, "ROOT", root):
            def commit(message):
                publisher.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.test",
                              "-c", f"core.hooksPath={root / 'hooks'}", "commit", "-qm", message)

            source.write_text(downloads_source({"macos": self.installers["macos"]}))
            publisher.git("add", "--", publisher.DOWNLOADS.as_posix())
            commit("Prepare download page")
            baseline = publisher.git("rev-parse", "HEAD").stdout.strip()
            self.installers["macos"]["commit"] = baseline[:10]
            source.write_text(downloads_source(self.installers))
            publisher.git("add", "--", publisher.DOWNLOADS.as_posix())
            commit("Commit new product build source")
            build_commit = publisher.git("rev-parse", "HEAD").stdout.strip()
            _, digest = self.artifact(commit=build_commit)
            (root / "local-notes.txt").write_text("leave this out")
            with patch.object(publisher, "make_uploader", return_value=lambda item: f"https://cos.test/{item.key}"):
                publisher.main()
            self.assertEqual(publisher.git("diff", "--cached", "--name-only").stdout.strip(), str(publisher.DOWNLOADS))
            self.assertIn(digest, publisher.git("show", f":{publisher.DOWNLOADS.as_posix()}").stdout)
            self.assertIn("?? local-notes.txt", publisher.git("status", "--short").stdout)


if __name__ == "__main__":
    unittest.main()
