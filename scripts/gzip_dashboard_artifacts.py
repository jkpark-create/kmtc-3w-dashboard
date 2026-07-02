#!/usr/bin/env python3
"""Write deterministic gzip copies of large dashboard JSON artifacts."""

from __future__ import annotations

import argparse
import gzip
import shutil
from pathlib import Path


def gzip_file(path: Path, output: Path | None = None) -> Path:
    path = path.resolve()
    output = (output or path.with_suffix(path.suffix + ".gz")).resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    with path.open("rb") as src, tmp.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as gz:
            shutil.copyfileobj(src, gz, length=1024 * 1024)
    tmp.replace(output)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", help="JSON files to gzip beside the source file.")
    args = parser.parse_args()

    for raw in args.paths:
        source = Path(raw)
        out = gzip_file(source)
        print(f"{source} -> {out} ({out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
