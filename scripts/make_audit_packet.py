"""Generate audit evidence without discovering or extracting undeclared datasets.

Manifest paths are relative to --repo. Only named commands and datasets are run
or inspected. Narrative is a separate, local Markdown input. Exit 1 means the
saved packet contains red flags, not necessarily that its current tests failed.
"""

import argparse
from contextlib import ExitStack
import csv
from datetime import date, datetime, timezone
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from zipfile import ZipFile


def sha256_file(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def json_text(value):
    return json.dumps(value, indent=2, ensure_ascii=False)


def fenced(text, language="text"):
    # Longer fences keep arbitrary program output inside its own block.
    longest = max((len(match[0]) for match in re.finditer(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    separator = "" if text.endswith("\n") else "\n"
    return f"{fence}{language}\n{text}{separator}{fence}\n"


def fingerprint_dataset(repo, spec):
    """Measure one explicit Parquet file or delimited file/archive member.

    All rows are counted. Date bounds exclude null/empty dates. Nested ZIPs are
    opened as streams. Hashing and scanning are separate passes, with bounded
    memory; compressed streams may therefore be decompressed more than once.
    """
    path = repo / spec["path"]
    dates = spec["date_columns"]
    before = path.stat()
    result = {"path": str(path.resolve()), "members": spec.get("members", []),
              "format": spec["format"], "role": spec.get("role", "input")}
    if spec.get("members"):
        result["container_sha256"] = sha256_file(path)
    with ExitStack() as stack:
        stream = stack.enter_context(path.open("rb"))
        for member in spec.get("members", []):
            archive = stack.enter_context(ZipFile(stream))
            stream = stack.enter_context(archive.open(member))
        result["sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        stream.seek(0)
        bounds = {column: {"min": None, "max": None} for column in dates}

        def update(column, value):
            if value is None or value == "":
                return
            if not isinstance(value, (date, datetime)):
                value = datetime.strptime(str(value), spec.get("date_format", "%Y-%m-%d")).date()
            value = value.isoformat()
            current = bounds[column]
            current["min"] = value if current["min"] is None else min(current["min"], value)
            current["max"] = value if current["max"] is None else max(current["max"], value)

        if spec["format"] == "parquet":
            import pyarrow.parquet as pq

            parquet = pq.ParquetFile(stream)
            columns = parquet.schema_arrow.names
            if len(set(columns)) != len(columns) or set(dates) - set(columns):
                raise ValueError("Duplicate columns or missing declared date columns")
            rows = parquet.metadata.num_rows
            for batch in parquet.iter_batches(batch_size=65536, columns=dates) if dates else ():
                for column in dates:
                    for value in batch.column(column).to_pylist():
                        update(column, value)
        elif spec["format"] == "delimited":
            text = stack.enter_context(io.TextIOWrapper(stream, encoding=spec.get("encoding", "utf-8"), newline=""))
            reader = csv.reader(text, delimiter=spec.get("delimiter", ","))
            columns = next(reader) if spec.get("header", True) else spec["columns"]
            if len(set(columns)) != len(columns) or set(dates) - set(columns):
                raise ValueError("Duplicate columns or missing declared date columns")
            positions = {column: columns.index(column) for column in dates}
            rows = 0
            for row in reader:
                if len(row) != len(columns):
                    raise ValueError(f"Record {rows + 1} has {len(row)} fields; expected {len(columns)}")
                rows += 1
                for column, position in positions.items():
                    update(column, row[position])
        else:
            raise ValueError(f"Unsupported dataset format: {spec['format']}")
        result.update(row_count=rows, columns=columns, dates=bounds,
                      date_columns_declared=dates, container_bytes=before.st_size)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("Dataset changed while its fingerprint was being measured")
    return result


class Recorder:
    def __init__(self, repo):
        self.repo = repo
        self.commands = []
        self.flags = []
        self.overrides = {"PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "PYTHONHASHSEED": "0",
                          "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
        preferred_temp = Path(os.environ.get("LOCALAPPDATA", "")) / "Temp/opencode"
        if preferred_temp.is_dir():
            self.overrides.update(TEMP=str(preferred_temp), TMP=str(preferred_temp))

    def run(self, name, argv, category="git", stdin=None):
        argv = [sys.executable if part == "{python}" else part for part in argv]
        record = {"name": name, "category": category, "argv": argv, "cwd": str(self.repo),
                  "environment_overrides": self.overrides,
                  "started_at": datetime.now(timezone.utc).isoformat()}
        begin = time.perf_counter()
        try:
            completed = subprocess.run(argv, cwd=self.repo, input=stdin, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, env={**os.environ, **self.overrides})
            raw, code = completed.stdout, completed.returncode
        except OSError as error:
            raw, code = f"{type(error).__name__}: {error}\n".encode(), 127
        record.update(wall_seconds=time.perf_counter() - begin, returncode=code,
                      output_sha256=hashlib.sha256(raw).hexdigest(), output_bytes=len(raw))
        try:
            record["output"] = raw.decode("utf-8")
        except UnicodeDecodeError:
            # Keep the bytes in the JSON sidecar rather than silently replacing them.
            record["output_hex"] = raw.hex()
            record["output"] = "Output is not UTF-8; exact bytes are in output_hex."
            self.flags.append(f"{name}: output is not UTF-8; see sidecar bytes")
        if stdin is not None:
            record["stdin_hex"] = stdin.hex()
        self.commands.append(record)
        if code:
            self.flags.append(f"{name}: return code {code}")
        return record

    def git(self, name, *args, stdin=None):
        return self.run(name, ["git", "-c", "core.quotepath=false", "--no-pager", *args], stdin=stdin)


def command_block(record):
    metadata = {key: value for key, value in record.items() if key not in {"output", "render_output"}}
    return f"### {record['name']}\n\n" + fenced(json_text(metadata), "json") + "\n" + fenced(record["output"])


def build_packet(args):
    repo = Path(args.repo).resolve()
    manifest_path = repo / args.manifest
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    stage = manifest["stage"]
    if not isinstance(stage, int) or not 1 <= stage <= 8:
        raise ValueError("Manifest stage must be an integer from 1 to 8")
    for key in ("scope", "random_seeds", "datasets", "commands", "important_diff_files"):
        if key not in manifest:
            raise ValueError(f"Manifest is missing {key}")
    recorder = Recorder(repo)
    head = recorder.git("Commit hash", "rev-parse", "HEAD")["output"].strip()
    if not re.fullmatch(r"[0-9a-f]{40,64}", head):
        raise ValueError("A committed Git repository is required")
    recorder.git("Commit message and timestamp", "show", "-s", "--format=%H%n%B%n%cI", "HEAD")
    branch = recorder.git("Branch", "rev-parse", "--abbrev-ref", "HEAD")["output"].strip()
    status_start = recorder.git("Working tree at start", "status", "--porcelain=v1", "--untracked-files=all")["output"]
    if status_start:
        recorder.flags.append("Working tree was not clean at the start; tests may cover uncommitted code")
    parents = recorder.git("Parent commits", "rev-list", "--parents", "-n", "1", "HEAD")["output"].split()
    base = parents[1] if len(parents) > 1 else recorder.git("Empty tree for root-commit comparison", "hash-object", "-t", "tree", "--stdin", stdin=b"")["output"].strip()
    recorder.git("Diff stat against first parent (empty tree for root commit)", "diff", "--stat", base, head)
    recorder.git("All changed files", "diff", "--name-status", base, head)
    diff = recorder.git("Full src and tests diff", "diff", "--no-ext-diff", "--no-textconv", base, head, "--", "src/", "tests/")
    if len(diff["output"].splitlines()) >= 400:
        diff["render_output"] = False
        selected = manifest["important_diff_files"]
        if len(selected) != 3 or len(set(selected)) != 3:
            recorder.flags.append("Diff has at least 400 lines but three distinct important files were not declared")
        for name in selected[:3]:
            recorder.git(f"Selected full diff: {name}", "diff", "--no-ext-diff", "--no-textconv", base, head, "--", name)
    # The generator is outside src/. Include its implementation as extra evidence.
    recorder.git("Additional scripts diff", "diff", "--no-ext-diff", "--no-textconv", base, head, "--", "scripts/")
    versions = sorted(
        ({"name": dist.metadata.get("Name", "metadata name missing"), "version": dist.version}
         for dist in importlib.metadata.distributions()), key=lambda item: (item["name"].lower(), item["version"])
    )
    environment = {"python": sys.version, "executable": sys.executable, "platform": platform.platform(),
                   "installed_libraries": versions, "declared_random_seeds": manifest["random_seeds"],
                   "subprocess_environment_overrides": recorder.overrides}
    configs = []
    config_paths = set((repo / "configs").rglob("*")) if (repo / "configs").is_dir() else set()
    config_paths.update(repo.glob("requirements*.txt"))
    config_paths.update(repo / name for name in ("pyproject.toml", "pytest.ini", "setup.cfg"))
    config_paths.add(manifest_path)
    for path in sorted(config_paths):
        if path.is_file() and path.name != ".gitkeep":
            configs.append({"path": str(path.relative_to(repo)), "sha256": sha256_file(path),
                            "contents": path.read_bytes().decode("utf-8")})
    fingerprints = []

    def measure(spec, phase):
        begin = time.perf_counter()
        try:
            item = fingerprint_dataset(repo, spec)
        except Exception as error:
            item = {"path": spec.get("path"), "error": f"{type(error).__name__}: {error}"}
            recorder.flags.append(f"Dataset fingerprint failed ({phase}): {item['path']}: {item['error']}")
        item.update(phase=phase, wall_seconds=time.perf_counter() - begin)
        fingerprints.append(item)
        return item

    inputs = {}
    for index, spec in enumerate(manifest["datasets"]):
        if spec.get("role", "input") not in {"input", "output", "inout"}:
            recorder.flags.append(f"Invalid dataset role: {spec.get('role')}")
        if spec.get("role", "input") != "output":
            inputs[index] = measure(spec, "before commands")
    for command in manifest["commands"]:
        recorder.run(command["name"], command["argv"], category="stage")
    test_run = recorder.run("Full pytest run", ["{python}", "-m", "pytest", "-vv", "-ra", "--color=no",
                                              "--tb=short", "--show-capture=all", "-p", "no:cacheprovider"], category="test")
    for index, spec in enumerate(manifest["datasets"]):
        after = measure(spec, "after commands")
        before = inputs.get(index, {})
        if spec.get("role", "input") == "input" and before.get("sha256") != after.get("sha256"):
            recorder.flags.append(f"Input content changed during the run: {spec['path']}")
    previous = []
    for name in args.include_run:
        path = repo / name
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            for required in ("name", "argv", "cwd", "wall_seconds", "returncode", "output"):
                if required not in record:
                    raise ValueError(f"Historical record is missing {required}")
            record["record_file"] = str(path)
            record["record_file_sha256"] = sha256_file(path)
            record["provenance"] = "Imported local record; hash is not proof of historical execution"
            previous.append(record)
            if record["returncode"]:
                recorder.flags.append(f"Historical run {record['name']}: return code {record['returncode']}; see B, not a current test result")
        except Exception as error:
            recorder.flags.append(f"Could not import historical record {name}: {type(error).__name__}: {error}")
    status_end = recorder.git("Working tree at end", "status", "--porcelain=v1", "--untracked-files=all")["output"]
    end_head = recorder.git("Commit hash at end", "rev-parse", "HEAD")["output"].strip()
    if status_end:
        recorder.flags.append("Working tree was not clean at the end")
    if head != end_head:
        recorder.flags.append("HEAD changed during evidence collection")
    if args.narrative:
        narrative_path = repo / args.narrative
        narrative = narrative_path.read_bytes().decode("utf-8")
        if "## C. Narrative" not in narrative or "## D. Reproduction" not in narrative:
            recorder.flags.append("Narrative input is missing section C or D")
        narrative_hash = sha256_file(narrative_path)
    else:
        narrative = "## C. Narrative\n\nnot written\n\n## D. Reproduction\n\nnot written\n"
        narrative_hash = None
        recorder.flags.append("Draft only: narrative and reproduction instructions were not supplied")
    recorder.flags.extend(args.red_flag)
    metadata = {"commit": head, "branch": branch, "comparison_base": base,
                "generated_at": datetime.now(timezone.utc).isoformat(), "stage": stage,
                "scope": manifest["scope"], "working_tree_clean_at_start": not bool(status_start),
                "working_tree_clean_at_end": not bool(status_end), "narrative_sha256": narrative_hash,
                "audit_invocation": [sys.executable, *sys.argv], "red_flags": recorder.flags}
    scope = {"declared_stage_commands": len(manifest["commands"]), "declared_datasets": len(manifest["datasets"]),
             "financial_metrics": "not run by this invocation" if not manifest["commands"] else "see command output below",
             "filter_and_join_counts": "not run by this invocation" if not manifest["commands"] else "see command output below",
             "current_pytest_returncode": test_run["returncode"],
             "coverage_basis": "explicit manifest, not operating-system file access tracing"}
    parts = [f"# Audit packet: {head[:7]}, stage {stage}\n"]
    if args.findings_response:
        parts.append("## AUDITOR FINDINGS RESPONSE\n\n" + (repo / args.findings_response).read_text(encoding="utf-8"))
    if recorder.flags:
        parts.append("## RED FLAGS\n\n" + "\n".join(f"- {flag}" for flag in recorder.flags))
    parts.append("## A. Mechanical section\n\nGenerated by the committed script. Code fences contain unedited UTF-8 output; a separator newline is added only when needed to close a fence. Ignored files are outside Git's clean-tree check.\n\n" + fenced(json_text(metadata), "json"))
    for record in recorder.commands:
        if record["category"] == "git":
            if record.get("render_output", True):
                parts.append(command_block(record))
            else:
                parts.append("### Large src/tests diff selection\n\nFull diff has at least 400 lines. Its bytes are retained in the JSON sidecar; the three manifest-selected file diffs are printed below.\n\n" + fenced(json_text({key: value for key, value in record.items() if key != "output"}), "json"))
    parts.append("### Environment and seeds\n\n" + fenced(json_text(environment), "json"))
    parts.append("### Configuration contents\n")
    for config in configs:
        parts.append(f"#### {config['path']}\n\nSHA-256: `{config['sha256']}`\n\n" + fenced(config["contents"]))
    parts.append(command_block(test_run))
    parts.append("### Dataset fingerprints\n\nOnly manifest entries are inspected. An empty list means no datasets were declared, not proof that no other process accessed data. Test fixtures are disposable and are not financial observations.\n\n" + fenced(json_text(fingerprints), "json"))
    parts.append("## B. Raw results\n\n### Invocation scope (program-generated)\n\n" + fenced(json_text(scope), "json"))
    for record in previous:
        parts.append(command_block(record))
    for record in recorder.commands:
        if record["category"] in {"stage", "test"}:
            parts.append(command_block(record))
    parts.append(narrative)
    packet_text = "\n".join(parts)
    folder = repo / "docs/audit"
    folder.mkdir(parents=True, exist_ok=True)
    numbers = [int(match[1]) for path in folder.iterdir()
               if (match := re.match(r"^(\d+)_.*_stage\d+\.md$", path.name))]
    number = max(numbers, default=0) + 1
    destination = folder / f"{number:03d}_{head[:7]}_stage{stage}.md"
    evidence = {"metadata": metadata, "environment": environment, "configs": configs,
                "fingerprints": fingerprints, "commands": recorder.commands, "historical_runs": previous,
                "scope": scope, "packet_sha256": hashlib.sha256(packet_text.encode("utf-8")).hexdigest()}
    with destination.open("x", encoding="utf-8", newline="") as stream:
        stream.write(packet_text)
    with destination.with_suffix(".json").open("x", encoding="utf-8", newline="") as stream:
        stream.write(json_text(evidence) + "\n")
    print(json_text({"packet": str(destination), "packet_sha256": evidence["packet_sha256"],
                     "red_flags": recorder.flags, "current_pytest_returncode": test_run["returncode"]}))
    return 1 if recorder.flags else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--narrative", help="Local Markdown containing sections C and D")
    parser.add_argument("--include-run", action="append", default=[], help="Previously captured command JSON")
    parser.add_argument("--red-flag", action="append", default=[])
    parser.add_argument("--findings-response", help="Local text answering auditor findings")
    args = parser.parse_args()
    try:
        return build_packet(args)
    except Exception as error:
        print(f"Audit packet generation failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
