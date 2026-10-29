#!/usr/bin/env python3
"""Regression checks for the publication boundary; no network or credentials used."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

from package_fixture import candidate_nuget_config, verify_restored_candidates

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


class PackageFixtureChecks(unittest.TestCase):
    def test_eventguard_candidates_are_mapped_to_the_local_feed(self):
        config = ET.fromstring(candidate_nuget_config(Path('candidate & packages')))
        sources = {node.get('key'): node.get('value') for node in config.findall('./packageSources/add')}
        patterns = {node.get('key'): [p.get('pattern') for p in node] for node in config.findall('./packageSourceMapping/packageSource')}
        self.assertEqual(sources['candidate'], str(Path('candidate & packages').resolve()))
        self.assertEqual(patterns, {'candidate': ['EventGuard.*'], 'nuget.org': ['*']})

    def test_package_only_evidence_rejects_stale_archives_assemblies_and_wrong_versions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            packages, cache = root / 'packages', root / 'cache'
            packages.mkdir()
            directory = cache / 'eventguard.probe/1.0.0'
            directory.mkdir(parents=True)
            candidate = packages / 'EventGuard.Probe.1.0.0.nupkg'
            asset = 'lib/net8.0/EventGuard.Probe.dll'
            with zipfile.ZipFile(candidate, 'w') as package:
                package.writestr(asset, b'current candidate assembly')
            restored = directory / 'eventguard.probe.1.0.0.nupkg'
            extracted = directory / asset
            extracted.parent.mkdir(parents=True)
            assets_file = root / 'project.assets.json'
            library = {'type': 'package', 'path': 'eventguard.probe/1.0.0'}
            for fault in (None, 'archive', 'missing archive', 'assembly', 'missing assembly', 'version', 'project', 'escape', 'empty'):
                with self.subTest(fault=fault):
                    restored.write_bytes(candidate.read_bytes())
                    extracted.write_bytes(b'current candidate assembly')
                    assets = {'libraries': {'EventGuard.Probe/1.0.0': library.copy()}}
                    if fault == 'archive': restored.write_bytes(b'old package at the same version')
                    if fault == 'missing archive': restored.unlink()
                    if fault == 'assembly': extracted.write_bytes(b'old extracted assembly')
                    if fault == 'missing assembly': extracted.unlink()
                    if fault == 'version': assets['libraries'] = {'EventGuard.Probe/0.9.0': library.copy()}
                    if fault == 'project': assets['libraries']['EventGuard.Probe/1.0.0']['type'] = 'project'
                    if fault == 'escape': assets['libraries']['EventGuard.Probe/1.0.0']['path'] = '../outside-cache'
                    if fault == 'empty': assets['libraries'] = {}
                    assets_file.write_text(json.dumps(assets))
                    if fault is None:
                        evidence = verify_restored_candidates(assets_file, packages, cache, '1.0.0')
                        self.assertEqual(len(evidence), 1)
                        self.assertEqual(evidence[0]['id'], 'EventGuard.Probe')
                    else:
                        with self.assertRaises(RuntimeError):
                            verify_restored_candidates(assets_file, packages, cache, '1.0.0')


if __name__ == "__main__":
    unittest.main(verbosity=2)
