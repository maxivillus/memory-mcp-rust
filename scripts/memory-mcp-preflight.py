#!/usr/bin/env python3
"""Run the copy-first cutover gate for legacy and Rust memory-mcp servers.

The checker intentionally launches both servers in SQLite-only disposable
databases. It never receives Redis credentials and never prints command lines,
paths, environment values, or memory payloads. A failed check is a hard stop
for cutover.
"""

from __future__ import annotations

import argparse
import json
import os
import select
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

RPC_TIMEOUT_SECONDS = 5.0
SAFE_ENV_NAMES = ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "XDG_RUNTIME_DIR")


class PreflightError(RuntimeError):
    """A bounded, expected preflight failure."""


class RpcClient:
    def __init__(self, command: str, database: Path):
        argv = shlex.split(command)
        if not argv:
            raise PreflightError("server command is empty")
        environment = {
            name: os.environ[name]
            for name in SAFE_ENV_NAMES
            if os.environ.get(name)
        }
        environment["MEMORY_MCP_DB"] = str(database)
        self.process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            text=True,
            bufsize=1,
        )
        self.next_id = 0

    def call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.next_id += 1
        request_id = self.next_id
        request: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            request["params"] = params
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self.process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        deadline = time.monotonic() + RPC_TIMEOUT_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PreflightError(f"RPC timeout for {method}")
            readable, _, _ = select.select([self.process.stdout], [], [], remaining)
            if not readable:
                raise PreflightError(f"RPC timeout for {method}")
            line = self.process.stdout.readline()
            if not line:
                raise PreflightError(f"server closed during {method}")
            try:
                response = json.loads(line)
            except json.JSONDecodeError as error:
                raise PreflightError(f"server returned invalid JSON during {method}") from error
            if not isinstance(response, dict):
                raise PreflightError(f"server returned an invalid response for {method}")
            if response.get("id") == request_id:
                return response

    def close(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=2)


def disposable_contract(command: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with tempfile.TemporaryDirectory(prefix="memory-mcp-contract-") as directory:
        client = RpcClient(command, Path(directory) / "facts.db")
        try:
            initialized = client.call(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "preflight", "version": "1"},
                },
            )
            listed = client.call("tools/list", {})
        finally:
            client.close()
    try:
        server_info = initialized["result"]["serverInfo"]
        tools = listed["result"]["tools"]
    except (KeyError, TypeError) as error:
        raise PreflightError("server did not return initialize/tools-list results") from error
    if not isinstance(server_info, dict) or not isinstance(tools, list):
        raise PreflightError("server returned an invalid initialize/tools-list shape")
    if not all(isinstance(tool, dict) and isinstance(tool.get("name"), str) for tool in tools):
        raise PreflightError("tools/list contains an invalid tool descriptor")
    return server_info, tools


def compare_contracts(
    legacy_info: dict[str, Any],
    legacy_tools: list[dict[str, Any]],
    rust_info: dict[str, Any],
    rust_tools: list[dict[str, Any]],
) -> dict[str, Any]:
    legacy_by_name = {tool["name"]: tool for tool in legacy_tools}
    rust_by_name = {tool["name"]: tool for tool in rust_tools}
    legacy_names = set(legacy_by_name)
    rust_names = set(rust_by_name)
    duplicate_names = len(legacy_names) != len(legacy_tools) or len(rust_names) != len(rust_tools)
    schema_mismatches = sorted(
        name
        for name in legacy_names & rust_names
        if legacy_by_name[name] != rust_by_name[name]
    )
    passed = (
        legacy_info == rust_info
        and len(legacy_tools) == 80
        and len(rust_tools) == 80
        and not duplicate_names
        and not (legacy_names - rust_names)
        and not (rust_names - legacy_names)
        and not schema_mismatches
    )
    return {
        "passed": passed,
        "legacy_server": legacy_info,
        "rust_server": rust_info,
        "legacy_tools": len(legacy_tools),
        "rust_tools": len(rust_tools),
        "duplicate_names": duplicate_names,
        "missing_in_rust": sorted(legacy_names - rust_names),
        "extra_in_rust": sorted(rust_names - legacy_names),
        "schema_mismatches": schema_mismatches,
    }


def sqlite_snapshot(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PreflightError("database path is not an existing file")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        names = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        for name in names:
            if "_fts" in name:
                continue
            escaped = name.replace("'", "''")
            integrity = connection.execute(
                f"PRAGMA quick_check('{escaped}')"
            ).fetchone()[0]
            if integrity != "ok":
                raise PreflightError("SQLite quick check failed")
        integrity = "ok"
        counts = {}
        for name in names:
            quoted = '"' + name.replace('"', '""') + '"'
            counts[name] = connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
        return {
            "quick_check": integrity,
            "tables": len(names),
            "rows": sum(counts.values()),
            "counts": counts,
        }
    finally:
        connection.close()


def compare_databases(legacy: Path, rust: Path) -> dict[str, Any]:
    legacy_snapshot = sqlite_snapshot(legacy)
    rust_snapshot = sqlite_snapshot(rust)
    changed_tables = sorted(
        name
        for name, count in legacy_snapshot["counts"].items()
        if "_fts" not in name
        if rust_snapshot["counts"].get(name) != count
    )
    missing_tables = sorted(
        name
        for name in legacy_snapshot["counts"]
        if "_fts" not in name and name not in rust_snapshot["counts"]
    )
    return {
        "passed": not changed_tables and not missing_tables,
        "legacy_quick_check": legacy_snapshot["quick_check"],
        "rust_quick_check": rust_snapshot["quick_check"],
        "legacy_tables": legacy_snapshot["tables"],
        "rust_tables": rust_snapshot["tables"],
        "legacy_rows": legacy_snapshot["rows"],
        "rust_rows": rust_snapshot["rows"],
        "changed_tables": changed_tables,
        "missing_tables": missing_tables,
    }


def route_probe(command: str, tool_names: list[str]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="memory-mcp-route-") as directory:
        client = RpcClient(command, Path(directory) / "facts.db")
        unsupported = []
        rpc_errors = []
        invalid_params = 0
        try:
            client.call("initialize", {"protocolVersion": "2024-11-05", "capabilities": {}})
            for name in tool_names:
                response = client.call(
                    "tools/call", {"name": name, "arguments": {}}
                )
                if "error" in response:
                    if response.get("error", {}).get("code") == -32602:
                        invalid_params += 1
                    else:
                        rpc_errors.append(name)
                    continue
                result = response.get("result")
                if not isinstance(result, dict):
                    raise PreflightError(f"server returned an invalid result for {name}")
                content = result.get("content", [{}])
                if not isinstance(content, list) or not content or not isinstance(content[0], dict):
                    raise PreflightError(f"server returned an invalid content for {name}")
                text = content[0].get("text", "")
                if not isinstance(text, str):
                    raise PreflightError(f"server returned an invalid tool payload for {name}")
                if any(
                    marker in text
                    for marker in ("unknown tool", "not implemented", "parity slice")
                ):
                    unsupported.append(name)
        finally:
            client.close()
    return {
        "passed": not unsupported and not rpc_errors,
        "checked": len(tool_names),
        "invalid_params": invalid_params,
        "unsupported": unsupported,
        "rpc_errors": rpc_errors,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--legacy-db", required=True, type=Path)
    parser.add_argument("--rust-db", required=True, type=Path)
    parser.add_argument(
        "--legacy-env",
        action="append",
        default=[],
        help="Environment variable name from the legacy launcher (repeatable)",
    )
    parser.add_argument(
        "--rust-env",
        action="append",
        default=[],
        help="Environment variable name supported by the Rust launcher (repeatable)",
    )
    parser.add_argument(
        "--skip-route-probe",
        action="store_true",
        help="Skip the disposable empty-argument route probe (not recommended)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report: dict[str, Any] = {"status": "blocked", "checks": {}}
    try:
        if args.legacy_db.resolve() == args.rust_db.resolve():
            raise PreflightError("legacy and Rust databases must be different files")
        if not args.legacy_env or not args.rust_env:
            raise PreflightError("both launcher environment name lists are required")

        legacy_info, legacy_tools = disposable_contract(args.legacy_command)
        rust_info, rust_tools = disposable_contract(args.rust_command)
        report["checks"]["contract"] = compare_contracts(
            legacy_info, legacy_tools, rust_info, rust_tools
        )
        report["checks"]["database"] = compare_databases(args.legacy_db, args.rust_db)
        legacy_env = set(args.legacy_env)
        rust_env = set(args.rust_env)
        report["checks"]["environment"] = {
            "passed": legacy_env == rust_env,
            "legacy_names": sorted(legacy_env),
            "rust_names": sorted(rust_env),
            "missing_in_rust": sorted(legacy_env - rust_env),
            "extra_in_rust": sorted(rust_env - legacy_env),
        }
        if args.skip_route_probe:
            report["checks"]["route"] = {"passed": False, "status": "not_run"}
        else:
            names = sorted({tool["name"] for tool in legacy_tools})
            report["checks"]["route"] = {
                "legacy": route_probe(args.legacy_command, names),
                "rust": route_probe(args.rust_command, names),
            }
        checks = report["checks"]
        report["status"] = (
            "pass"
            if all(
                check.get("passed") is True
                if name != "route"
                else check.get("legacy", {}).get("passed") is True
                and check.get("rust", {}).get("passed") is True
                for name, check in checks.items()
            )
            else "blocked"
        )
    except (OSError, PreflightError, sqlite3.Error, ValueError) as error:
        report["error"] = type(error).__name__
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
