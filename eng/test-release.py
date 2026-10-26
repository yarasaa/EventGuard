#!/usr/bin/env python3
"""Regression checks for the publication boundary; no network or credentials used."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("release", Path(__file__).with_name("verify-release.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
COMMIT = "a" * 40


class ReleaseChecks(unittest.TestCase):
    def run_record(self):
        return {"status": "completed", "conclusion": "success", "head_sha": COMMIT,
                "head_branch": "main", "path": ".github/workflows/ci.yml", "event": "push",
                "repository": {"full_name": release.REPOSITORY},
                "head_repository": {"full_name": release.REPOSITORY}}

    def test_completed_push_and_manual_runs_qualify(self):
        for event in ("push", "workflow_dispatch"):
            with self.subTest(event=event):
                release.validate_run(self.run_record() | {"event": event}, COMMIT)

    def test_failed_unfinished_stale_and_untrusted_runs_are_rejected(self):
        invalid = [("status", "in_progress"), ("conclusion", "failure"),
                   ("conclusion", "cancelled"), ("head_sha", "b" * 40),
                   ("head_branch", "feature"), ("path", ".github/workflows/other.yml"),
                   ("event", "pull_request"), ("event", "pull_request_target"),
                   ("repository", {"full_name": "someone/EventGuard"}),
                   ("head_repository", {"full_name": "someone/EventGuard"})]
        for field, value in invalid:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                release.validate_run(self.run_record() | {field: value}, COMMIT)

    def test_version_and_commit_must_match_preview_source(self):
        release.validate_candidate(release.source_version(), COMMIT)
        for version, commit in [("1.0.0", COMMIT), ("0.1.0-other", COMMIT),
                                (release.source_version(), "main"), (release.source_version(), "abc123")]:
            with self.subTest(version=version, commit=commit), self.assertRaises(ValueError):
                release.validate_candidate(version, commit)

    def test_missing_run_fields_cannot_qualify(self):
        for field in self.run_record():
            run = self.run_record()
            del run[field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                release.validate_run(run, COMMIT)

    def test_package_manifest_detects_tampering_and_mixed_inventory(self):
        version = release.source_version()
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            for name in release.package_names(version):
                (folder / name).write_bytes(b"synthetic package bytes")
            manifest = {"schemaVersion": 1, "repository": release.REPOSITORY, "commit": COMMIT,
                        "version": version, "sha256": release.package_hashes(folder, version)}
            receipt = folder / "release.json"
            receipt.write_text(json.dumps(manifest))
            release.verify_manifest(folder, version, COMMIT)
            package = next(folder.glob("*.nupkg"))
            original = package.read_bytes()
            for kind in ("changed", "missing", "extra", "symlink"):
                with self.subTest(kind=kind):
                    if kind == "changed": package.write_bytes(b"different bytes")
                    elif kind == "missing": package.unlink()
                    elif kind == "extra": (folder / "Unrelated.1.0.0.nupkg").write_bytes(original)
                    else:
                        package.unlink()
                        package.symlink_to(receipt)
                    with self.assertRaises(ValueError):
                        release.verify_manifest(folder, version, COMMIT)
                    if package.is_symlink(): package.unlink()
                    package.write_bytes(original)
                    (folder / "Unrelated.1.0.0.nupkg").unlink(missing_ok=True)
            for key, value in [("commit", "b" * 40), ("version", "1.0.0"),
                               ("repository", "someone/EventGuard"), ("schemaVersion", 2)]:
                with self.subTest(field=key), self.assertRaises(ValueError):
                    altered = copy.deepcopy(manifest)
                    altered[key] = value
                    receipt.write_text(json.dumps(altered))
                    release.verify_manifest(folder, version, COMMIT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
