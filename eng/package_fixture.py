"""Keep package-only integration fixtures tied to the exact candidate artifacts."""
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile


def candidate_nuget_config(packages):
    root = ET.Element('configuration')
    sources = ET.SubElement(root, 'packageSources')
    ET.SubElement(sources, 'clear')
    ET.SubElement(sources, 'add', key='candidate', value=str(Path(packages).resolve()))
    ET.SubElement(sources, 'add', key='nuget.org', value='https://api.nuget.org/v3/index.json')
    mapping = ET.SubElement(root, 'packageSourceMapping')
    # The specific EventGuard.* pattern wins over *. Feed order does not pin a package source.
    ET.SubElement(ET.SubElement(mapping, 'packageSource', key='candidate'), 'package', pattern='EventGuard.*')
    ET.SubElement(ET.SubElement(mapping, 'packageSource', key='nuget.org'), 'package', pattern='*')
    return ET.tostring(root, encoding='unicode')


def verify_restored_candidates(assets_file, packages, cache, version):
    packages, cache = Path(packages).resolve(), Path(cache).resolve()
    restored = json.loads(Path(assets_file).read_text())['libraries']
    evidence = []
    for identity, library in sorted(restored.items()):
        name, restored_version = identity.split('/', 1)
        if not name.lower().startswith('eventguard.'):
            continue
        if library['type'] != 'package' or restored_version != version:
            raise RuntimeError('Expected a candidate package at the selected version: ' + identity)
        candidate = packages / f'{name}.{version}.nupkg'
        directory = (cache / library['path']).resolve()
        if not directory.is_relative_to(cache):
            raise RuntimeError('Restored package is outside the isolated cache: ' + identity)
        archive = directory / f'{name.lower()}.{version.lower()}.nupkg'
        expected = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if not archive.is_file() or hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
            raise RuntimeError('Restored artifact differs from the candidate: ' + identity)
        # Checking the archive alone would miss stale/replaced extracted assemblies used by the compiler.
        with zipfile.ZipFile(candidate) as package:
            for entry in package.namelist():
                if entry.startswith(('lib/', 'ref/')) and not entry.endswith('/'):
                    extracted = (directory / entry).resolve()
                    if not extracted.is_relative_to(directory) or not extracted.is_file() or extracted.read_bytes() != package.read(entry):
                        raise RuntimeError('Restored assembly asset differs from the candidate: ' + identity + ':' + entry)
        evidence.append({'id': name, 'version': version, 'sha256': expected})
    if not evidence:
        raise RuntimeError('No EventGuard candidate packages were restored.')
    return evidence
