"""Run Stage 1 from approved archives, or rebuild the panel from measured inputs."""

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from credit_risk.ingest import emit, file_hash, ingest_archives, validate_cached_ingestion


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/stage1.json")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--ingest-only", action="store_true")
    mode.add_argument("--panel-only", action="store_true")
    args = parser.parse_args()
    config = json.loads((ROOT / args.config).read_text(encoding="utf-8"))
    started = time.perf_counter()
    (ROOT / config["report_dir"] / "panel_success.json").unlink(missing_ok=True)
    emit("run_configuration", config=config, config_sha256=file_hash(ROOT / args.config))
    if not args.panel_only:
        ingest_archives(ROOT, config)
    else:
        validate_cached_ingestion(ROOT, config)
    if not args.ingest_only:
        from credit_risk.panel import build_panel

        build_panel(ROOT, config)
    emit("run_finished", wall_seconds=time.perf_counter() - started, mode="ingest_only" if args.ingest_only else "panel_only" if args.panel_only else "complete")


if __name__ == "__main__":
    main()
