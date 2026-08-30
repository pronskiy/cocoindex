"""Transport for the Omnigraph connector — the only module that does I/O."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import tempfile
from typing import Protocol

from cocoindex.connectors.omnigraph._gq import Mutation


@dataclasses.dataclass(frozen=True)
class ConnectionFactory:
    """Identifies an Omnigraph store and the branch to write to.

    Provided once in the app lifespan and resolved at action time via the
    ContextProvider — never captured at declare time, since delete actions run
    in a process where the declaring code never executes.

    ``store`` is a URI (``file:///abs/path/g.omni``), not a filesystem path.
    """

    store: str
    branch: str = "main"
    cli: str = "omnigraph"


class _Client(Protocol):
    async def init_graph(self, schema_pg: str) -> None: ...
    async def apply_schema(self, schema_pg: str) -> None: ...
    async def read_schema(self) -> str | None: ...
    async def mutate(self, mutation: Mutation, *, branch: str) -> None: ...
    async def branch_create(self, name: str, *, frm: str) -> None: ...
    async def branch_merge(self, name: str, *, into: str) -> None: ...
    async def branch_delete(self, name: str) -> None: ...


class OmnigraphCliError(RuntimeError):
    """Non-zero exit from the omnigraph CLI, carrying its stderr."""


class _CliClient:
    def __init__(self, conn: ConnectionFactory) -> None:
        self._conn = conn

    # --- argv builders (pure, unit-tested) ---

    def _mutate_argv(
        self, query_path: str, params_path: str, *, branch: str
    ) -> list[str]:
        """Both the GQ source and the bound params go by FILE, never inline.

        The inline forms (`-e <gq>`, `--params <json>`) put the whole commit
        into argv, and a commit is a whole component's writes: at the 8,192
        entity cap that is ~2.3 MB across the two arguments, well past
        darwin's 1 MB `ARG_MAX` — `create_subprocess_exec` raises `OSError:
        [Errno 7] Argument list too long`, which isn't an `OmnigraphCliError`
        and so isn't caught anywhere. Linux binds tighter still: its
        128 KiB-per-argument `MAX_ARG_STRLEN` caps the expression alone at
        roughly 800 entities whatever `ARG_MAX` allows.

        `--query <path>` and `--params-file <path>` take exactly the same
        input with no size limit at all (verified against the binary, both
        with and without the positional query name, and at the full 8,192
        cap). So the transport simply doesn't put payloads in argv.
        """
        return [
            self._conn.cli,
            "mutate",
            "--store",
            self._conn.store,
            "--branch",
            branch,
            "--query",
            query_path,
            "--params-file",
            params_path,
            "--json",
            "--quiet",
        ]

    def _merge_argv(self, name: str, *, into: str) -> list[str]:
        # No --if-commit: 0.9.0 has no CAS. A conflict is a non-zero exit.
        return [
            self._conn.cli,
            "branch",
            "merge",
            name,
            "--into",
            into,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _init_argv(self, schema_path: str) -> list[str]:
        # `init` takes the graph URI POSITIONALLY, not via --store, and has
        # no --json flag at all — it prints a plain "initialized <uri>" line
        # to stdout instead of JSON.
        return [
            self._conn.cli,
            "init",
            "--schema",
            schema_path,
            "--quiet",
            self._conn.store,
        ]

    def _apply_schema_argv(self, schema_path: str) -> list[str]:
        return [
            self._conn.cli,
            "schema",
            "apply",
            "--schema",
            schema_path,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _schema_show_argv(self) -> list[str]:
        return [
            self._conn.cli,
            "schema",
            "show",
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _branch_create_argv(self, name: str, *, frm: str) -> list[str]:
        return [
            self._conn.cli,
            "branch",
            "create",
            name,
            "--from",
            frm,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    def _branch_delete_argv(self, name: str) -> list[str]:
        return [
            self._conn.cli,
            "branch",
            "delete",
            name,
            "--store",
            self._conn.store,
            "--json",
            "--quiet",
        ]

    # --- execution ---

    async def _run(self, argv: list[str]) -> dict[str, object]:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            raise OmnigraphCliError(
                f"{' '.join(argv[:3])} exited {proc.returncode}: "
                f"{err.decode(errors='replace').strip()}"
            )
        if "--json" not in argv:
            # `init` has no --json flag; its stdout is a plain diagnostic
            # line, not JSON, so there is nothing to parse.
            return {}
        text = out.decode(errors="replace").strip()
        return json.loads(text) if text else {}

    async def init_graph(self, schema_pg: str) -> None:
        with tempfile.NamedTemporaryFile("w", suffix=".pg", encoding="utf-8") as f:
            f.write(schema_pg)
            f.flush()
            await self._run(self._init_argv(f.name))

    async def apply_schema(self, schema_pg: str) -> None:
        with tempfile.NamedTemporaryFile("w", suffix=".pg", encoding="utf-8") as f:
            f.write(schema_pg)
            f.flush()
            await self._run(self._apply_schema_argv(f.name))

    async def read_schema(self) -> str | None:
        """Return the graph's current, complete `.pg` schema source, or
        `None` if the graph hasn't been `init`'d yet.

        Omnigraph's schema is applied whole-graph, not per type: `schema
        apply`/`init` treat their input as the complete desired schema, so
        callers that reconcile one type at a time must read this back and
        merge before writing, rather than applying a single type's
        fragment directly (see `_gq.merge_type_into_schema`).

        Not-yet-initialized is detected on the CLI's stderr text — the raw
        Lance "Dataset ... not found" (verified against the binary) — since
        the exit code alone doesn't distinguish it from any other failure.
        """
        try:
            result = await self._run(self._schema_show_argv())
        except OmnigraphCliError as e:
            msg = str(e).lower()
            # Match the engine's actual phrasing, not the two words
            # separately: the store URI is echoed into every error, so a store
            # under `~/datasets/` would otherwise turn any unrelated
            # "not found" failure into a bogus "graph not initialized".
            if "dataset at path" in msg and "was not found" in msg:
                return None
            raise
        source = result["schema_source"]
        assert isinstance(source, str)
        return source

    async def mutate(self, mutation: Mutation, *, branch: str) -> None:
        with (
            tempfile.NamedTemporaryFile("w", suffix=".gq", encoding="utf-8") as q,
            tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8") as p,
        ):
            q.write(mutation.expr)
            q.flush()
            json.dump(mutation.params, p)
            p.flush()
            await self._run(self._mutate_argv(q.name, p.name, branch=branch))

    async def branch_create(self, name: str, *, frm: str) -> None:
        await self._run(self._branch_create_argv(name, frm=frm))

    async def branch_merge(self, name: str, *, into: str) -> None:
        await self._run(self._merge_argv(name, into=into))

    async def branch_delete(self, name: str) -> None:
        await self._run(self._branch_delete_argv(name))
