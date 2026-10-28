"""Audit-tool tests use tiny, invented fixtures, never mortgage observations."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import date
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "make_audit_packet.py"


def load_auditor():
    assert SCRIPT.is_file(), "The requested audit generator has not been implemented"
    spec = importlib.util.spec_from_file_location("audit_generator", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def audit_repo(tmp_path):
    """A separate disposable repository, not the real project or its data."""
    git(tmp_path, "init", "--initial-branch=main")
    (tmp_path / ".gitignore").write_text("docs/\n__pycache__/\n.pytest_cache/\n")
    for directory in ("src", "tests", "configs", "docs/audit"):
        (tmp_path / directory).mkdir(parents=True, exist_ok=True)
    (tmp_path / "src/example.py").write_text("VALUE = 1\n")
    (tmp_path / "tests/test_example.py").write_text(
        "def test_fixture_passes():\n    assert 2 + 2 == 4\n"
    )
    (tmp_path / "configs/audit.json").write_text(json.dumps({
        "stage": 1,
        "scope": "invented audit test fixture, not a financial model",
        "random_seeds": {},
        "datasets": [],
        "commands": [],
        "important_diff_files": ["src/example.py", "tests/test_example.py", "configs/audit.json"],
    }))
    (tmp_path / "docs/audit/narrative.md").write_text(
        "## C. Narrative\n\nTest fixture only.\n\n## D. Reproduction\n\nTest fixture only.\n"
    )
    commit(tmp_path, "Initial test fixture")
    return tmp_path


def commit(repo, message):
    git(repo, "add", "--all")
    git(repo, "-c", "user.name=Audit Test", "-c", "user.email=audit-test@example.invalid",
        "commit", "-m", message)


def configure(repo, **changes):
    path = repo / "configs/audit.json"
    config = json.loads(path.read_text())
    config.update(changes)
    path.write_text(json.dumps(config, indent=2))
    commit(repo, "Change audit test configuration")


def run_packet(repo, *extra):
    assert SCRIPT.is_file(), "The requested audit generator has not been implemented"
    process = subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo),
         "--manifest", "configs/audit.json", "--narrative", "docs/audit/narrative.md", *extra],
        capture_output=True, env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
    )
    packets = sorted((repo / "docs/audit").glob("[0-9][0-9][0-9]_*_stage1.md"))
    assert packets, process.stdout.decode() + process.stderr.decode()
    packet = packets[-1]
    return process, packet, packet.read_bytes().decode("utf-8")


def test_parquet_fingerprint_measures_rows_dates_and_changed_content(tmp_path):
    auditor = load_auditor()
    path = tmp_path / "fixture.parquet"
    table = pa.table({"loan": [1, 2, 3], "month": [date(2006, 1, 1), None, date(2007, 3, 1)]})
    pq.write_table(table, path)
    report = auditor.fingerprint_dataset(tmp_path, {
        "path": "fixture.parquet", "format": "parquet", "date_columns": ["month"]
    })
    assert report["row_count"] == 3
    assert report["columns"] == ["loan", "month"]
    assert report["dates"] == {"month": {"min": "2006-01-01", "max": "2007-03-01"}}
    assert report["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    original_hash = report["sha256"]
    pq.write_table(table.slice(0, 1), path)
    changed = auditor.fingerprint_dataset(tmp_path, {
        "path": "fixture.parquet", "format": "parquet", "date_columns": ["month"]
    })
    assert changed["sha256"] != original_hash
    assert changed["row_count"] == 1


def test_nested_zip_fingerprint_counts_declared_text_member_without_extraction(tmp_path):
    auditor = load_auditor()
    payload = b"A|200601\nB|200803\n"
    inner_bytes = io.BytesIO()
    with ZipFile(inner_bytes, "w") as inner:
        inner.writestr("fixture.txt", payload)
    with ZipFile(tmp_path / "outer.zip", "w") as outer:
        outer.writestr("inner.zip", inner_bytes.getvalue())
    result = auditor.fingerprint_dataset(tmp_path, {
        "path": "outer.zip", "members": ["inner.zip", "fixture.txt"],
        "format": "delimited", "delimiter": "|", "columns": ["loan", "month"],
        "header": False, "date_columns": ["month"], "date_format": "%Y%m",
    })
    assert result["row_count"] == 2
    assert result["dates"]["month"] == {"min": "2006-01-01", "max": "2008-03-01"}
    assert result["sha256"] == hashlib.sha256(payload).hexdigest()
    assert result["container_sha256"] == hashlib.sha256((tmp_path / "outer.zip").read_bytes()).hexdigest()
    assert sorted(path.name for path in tmp_path.iterdir()) == ["outer.zip"]


def test_recorded_archive_counts_require_unchanged_container_and_complete_inventory(tmp_path):
    auditor = load_auditor()
    inner = io.BytesIO()
    with ZipFile(inner, "w") as quarter:
        quarter.writestr("loans.txt", "A|200601\nB|200602\n")
    archive = tmp_path / "annual.zip"
    with ZipFile(archive, "w") as annual:
        annual.writestr("quarter.zip", inner.getvalue())
    member = {"quarter_archive": "quarter.zip", "member": "loans.txt", "bytes": 18,
              "row_count": 2, "columns": ["loan", "month"],
              "dates": {"month": {"min": "2006-01-01", "max": "2006-02-01"}},
              "sha256": hashlib.sha256(b"A|200601\nB|200602\n").hexdigest()}
    member["bytes"] = len(b"A|200601\nB|200602\n")
    source = {"path": "annual.zip", "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "bytes": archive.stat().st_size, "members": [member]}
    manifest = tmp_path / "scan.json"
    manifest.write_text(json.dumps({"sources": [source], "measured_at": "fixture"}))
    spec = {"path": "annual.zip", "format": "recorded_zip", "record_path": "scan.json", "date_columns": []}
    result = auditor.fingerprint_dataset(tmp_path, spec)
    assert result["member_fingerprints"][0]["row_count"] == 2
    assert result["member_fingerprints"][0]["dates"]["month"]["min"] == "2006-01-01"
    assert result["measurement_basis"] == "recorded full scan; current container hash and inventory verified"
    source["members"] = []
    manifest.write_text(json.dumps({"sources": [source]}))
    with pytest.raises(ValueError, match="inventory"):
        auditor.fingerprint_dataset(tmp_path, spec)
    source["members"] = [member]
    manifest.write_text(json.dumps({"sources": [source]}))
    with ZipFile(archive, "a") as annual:
        annual.writestr("extra.txt", "changed")
    with pytest.raises(ValueError, match="changed"):
        auditor.fingerprint_dataset(tmp_path, spec)


def test_recorded_scan_changes_are_flagged_even_when_the_archive_is_unchanged(audit_repo):
    inner = io.BytesIO()
    payload = b"A|200601\n"
    with ZipFile(inner, "w") as quarter:
        quarter.writestr("loans.txt", payload)
    archive = audit_repo / "annual.zip"
    with ZipFile(archive, "w") as annual:
        annual.writestr("quarter.zip", inner.getvalue())
    record = {"sources": [{"path": "annual.zip", "bytes": archive.stat().st_size,
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "members": [{
            "quarter_archive": "quarter.zip", "member": "loans.txt", "bytes": len(payload),
            "row_count": 1, "columns": ["loan", "month"], "dates": {},
            "sha256": hashlib.sha256(payload).hexdigest()}]}]}
    (audit_repo / "docs/scan.json").write_text(json.dumps(record))
    configure(audit_repo, datasets=[{"path": "annual.zip", "format": "recorded_zip",
        "record_path": "docs/scan.json", "date_columns": [], "role": "input"}], commands=[{
        "name": "Modify ignored recorded counts without modifying source archive", "argv": ["{python}", "-c",
        "import json;from pathlib import Path;p=Path('docs/scan.json');d=json.loads(p.read_text());d['sources'][0]['members'][0]['row_count']=200;p.write_text(json.dumps(d))"]}])
    process, packet, text = run_packet(audit_repo)
    assert process.returncode == 1
    assert "Input content changed" in text
    evidence = json.loads(packet.with_suffix(".json").read_text())
    assert evidence["metadata"]["working_tree_clean_at_end"]
    before, after = evidence["fingerprints"]
    assert before["sha256"] == after["sha256"]
    assert before["record_sha256"] != after["record_sha256"]


def test_bad_columns_and_dates_cannot_get_successful_fingerprints(tmp_path):
    auditor = load_auditor()
    (tmp_path / "fixture.csv").write_text("loan,month\nA,not-a-date\n")
    with pytest.raises(ValueError):
        auditor.fingerprint_dataset(tmp_path, {
            "path": "fixture.csv", "format": "delimited", "header": True,
            "date_columns": ["month"], "date_format": "%Y-%m-%d",
        })
    with pytest.raises(ValueError):
        auditor.fingerprint_dataset(tmp_path, {
            "path": "fixture.csv", "format": "delimited", "header": True,
            "date_columns": ["missing"],
        })


def test_root_commit_packet_has_git_config_tests_and_immutable_raw_output(audit_repo):
    process, path, text = run_packet(audit_repo)
    assert process.returncode == 0, text
    assert git(audit_repo, "rev-parse", "HEAD") in text
    assert "Initial test fixture" in text
    assert '"working_tree_clean_at_start": true' in text
    assert '"random_seeds": {}' in text
    assert "test_fixture_passes PASSED" in text
    assert "+VALUE = 1" in text
    evidence = json.loads(path.with_suffix(".json").read_text())
    assert evidence["commands"]
    for command in evidence["commands"]:
        assert command["wall_seconds"] >= 0
        assert command["output"] in text
    assert "## A. Mechanical section" in text
    assert "## B. Raw results" in text
    assert text.index("## A.") < text.index("## B.") < text.index("## C.") < text.index("## D.")


def test_failed_commands_skips_xfails_and_warnings_are_never_hidden(audit_repo):
    (audit_repo / "tests/test_example.py").write_text(
        "import pytest\nimport warnings\n"
        "def test_warns():\n    warnings.warn('fixture warning', UserWarning)\n"
        "@pytest.mark.skip(reason='fixture skip reason')\ndef test_skipped():\n    pass\n"
        "@pytest.mark.xfail(reason='fixture xfail reason', strict=True)\n"
        "def test_xfailed():\n    assert False\n"
    )
    configure(audit_repo, commands=[{
        "name": "deliberately failing fixture command",
        "argv": ["{python}", "-c", "import sys; print('fixture NaN inf clipped=2'); print('fixture stderr', file=sys.stderr); sys.exit(3)"],
    }])
    process, _, text = run_packet(audit_repo)
    assert process.returncode == 1
    assert "## RED FLAGS" in text
    assert "fixture NaN inf clipped=2" in text
    assert "fixture stderr" in text
    assert "fixture skip reason" in text
    assert "fixture xfail reason" in text
    assert "fixture warning" in text
    assert '"returncode": 3' in text


def test_missing_dataset_is_flagged_and_previous_packet_is_not_overwritten(audit_repo):
    configure(audit_repo, datasets=[{
        "path": "absent.parquet", "format": "parquet", "date_columns": ["month"], "role": "input"
    }])
    process, first, text = run_packet(audit_repo)
    assert process.returncode == 1
    assert "## RED FLAGS" in text and "absent.parquet" in text
    original = first.read_bytes()
    _, second, _ = run_packet(audit_repo)
    assert first != second
    assert first.read_bytes() == original


def test_dirty_tree_and_changes_during_run_are_reported(audit_repo):
    configure(audit_repo, commands=[{
        "name": "deliberate test mutation",
        "argv": ["{python}", "-c", "from pathlib import Path; Path('src/example.py').write_text('VALUE = 9\\n')"],
    }])
    process, _, text = run_packet(audit_repo)
    assert process.returncode == 1
    assert '"working_tree_clean_at_start": true' in text
    assert '"working_tree_clean_at_end": false' in text
    assert "## RED FLAGS" in text
    second, _, text = run_packet(audit_repo)
    assert second.returncode == 1
    assert '"working_tree_clean_at_start": false' in text


def test_large_diff_includes_selected_files_and_names_every_changed_file(audit_repo):
    for name in ("a.py", "b.py", "c.py", "d.py"):
        (audit_repo / "src" / name).write_text("\n".join(f"v{i} = {i}" for i in range(110)) + "\n")
    configure(audit_repo, important_diff_files=["src/a.py", "src/b.py", "src/c.py"])
    process, _, text = run_packet(audit_repo)
    assert process.returncode == 0, text
    assert "src/d.py" in text
    for name in ("a.py", "b.py", "c.py"):
        assert f"diff --git a/src/{name} b/src/{name}" in text
    assert "diff --git a/src/d.py b/src/d.py" not in text
    assert "Change audit test configuration" in text


def test_actual_pytest_failure_and_imported_output_remain_in_packet(audit_repo):
    (audit_repo / "tests/test_example.py").write_text(
        "def test_deliberate_failure():\n    assert False, 'deliberate pytest failure sentinel'\n"
    )
    commit(audit_repo, "Introduce deliberate fixture failure")
    raw = "Historical fixture output\tNaN\nnot a financial result\n"
    record = {"name": "invented log-format fixture", "argv": ["fixture-only"],
              "cwd": str(audit_repo), "wall_seconds": 0, "returncode": 7, "output": raw}
    (audit_repo / "docs/history.json").write_text(json.dumps(record))
    process, _, text = run_packet(audit_repo, "--include-run", "docs/history.json")
    assert process.returncode == 1
    assert "test_deliberate_failure FAILED" in text
    assert "deliberate pytest failure sentinel" in text
    assert raw in text
    assert "not proof of historical execution" in text
    assert '"returncode": 7' in text


def test_changed_ignored_input_is_detected_even_when_git_stays_clean(audit_repo):
    data = audit_repo / "docs/fixture.csv"
    data.write_text("loan,month\nA,2006-01-01\n")
    configure(audit_repo, datasets=[{
        "path": "docs/fixture.csv", "format": "delimited", "date_columns": ["month"],
        "role": "input",
    }], commands=[{
        "name": "deliberately change an ignored test input",
        "argv": ["{python}", "-c", "from pathlib import Path; Path('docs/fixture.csv').write_text('loan,month\\nB,2008-01-01\\n')"],
    }])
    process, path, text = run_packet(audit_repo)
    assert process.returncode == 1
    evidence = json.loads(path.with_suffix(".json").read_text())
    assert evidence["metadata"]["working_tree_clean_at_start"] is True
    assert evidence["metadata"]["working_tree_clean_at_end"] is True
    before, after = evidence["fingerprints"]
    assert before["sha256"] != after["sha256"]
    assert before["dates"]["month"]["min"] == "2006-01-01"
    assert after["dates"]["month"]["min"] == "2008-01-01"
    assert "Input content changed during the run" in text
