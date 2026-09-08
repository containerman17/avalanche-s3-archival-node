#!/usr/bin/env python3
"""Prepare patched subnet-evm sources in a private module cache."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile


MODULES = [
    ("github.com/ava-labs/avalanchego/graft/subnet-evm",
     "github.com/ava-labs/avalanchego/graft/subnet-evm",
     "v1.14.3-0.20260804141953-6dc4c3b395b6", "sources.json", "subnet-evm.patch"),
    ("github.com/ava-labs/libevm", "github.com/containerman17/libevm",
     "v1.13.15-0.20260817022927-4c8a6553b55f", "libevm-sources.json", "libevm.patch"),
]


def copy_module_branches(original, target, paths):
    """Copy selected modules and link unchanged branches to the original cache."""
    target.mkdir()
    for child in original.iterdir():
        destination = target / child.name
        matches = [parts for parts in paths if child.name == parts[0]]
        if not matches:
            destination.symlink_to(child, target_is_directory=child.is_dir())
        elif any(len(parts) == 1 for parts in matches):
            shutil.copytree(child, destination)
        else:
            copy_module_branches(child, destination, [parts[1:] for parts in matches])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new directory for generated sources")
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    repo = here.parent.parent
    env = dict(os.environ, GOWORK="off")
    cache = Path(subprocess.check_output(
        ["go", "env", "GOMODCACHE"], cwd=repo, env=env, text=True
    ).strip())
    modules = []
    for requested, expected_path, version, manifest_name, patch_name in MODULES:
        module = json.loads(subprocess.check_output(
            ["go", "list", "-m", "-json", requested], cwd=repo, env=env, text=True
        ))
        resolved = module.get("Replace", module)
        if resolved.get("Path") != expected_path or resolved.get("Version") != version:
            raise SystemExit(f"expected {requested} to resolve to {expected_path}@{version}")
        source = Path(resolved["Dir"])
        manifest = json.loads((here / manifest_name).read_text())
        for name, expected in manifest.items():
            if expected is None:
                if (source / name).exists():
                    raise SystemExit(f"patch adds a source file that already exists: {name}")
                continue
            actual = hashlib.sha256((source / name).read_bytes()).hexdigest()
            if actual != expected:
                raise SystemExit(f"pinned source checksum differs: {requested}/{name}")
        modules.append((source.relative_to(cache), manifest, patch_name))

    if args.output is None:
        output = Path(tempfile.mkdtemp(prefix="epochdb-subnet-source-"))
    else:
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
    private_cache = output / "mod"
    copy_module_branches(cache, private_cache, [relative.parts for relative, _, _ in modules])
    for relative, manifest, patch_name in modules:
        patched = private_cache / relative
        for name in manifest:
            target = patched / name
            target.parent.chmod(0o755)
            if target.exists():
                target.chmod(0o644)
        subprocess.run([
            "patch", "--batch", "--forward", "--fuzz=0", "--no-backup-if-mismatch",
            "-p1", "-d", str(patched), "-i", str(here / patch_name),
        ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    build_env = output / "env.sh"
    build_env.write_text(
        f"export GOMODCACHE={shlex.quote(str(private_cache))}\n"
        "export GOWORK=off\n"
    )
    print(build_env)


if __name__ == "__main__":
    main()
