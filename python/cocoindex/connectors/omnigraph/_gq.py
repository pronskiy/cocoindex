"""Pure builders for Omnigraph `.pg` schema fragments and `.gq` mutations.

Every function here is a pure function over strings and primitives — no I/O,
no connector types — so the whole module is unit-testable without a running
Omnigraph. Mirrors the role `neo4j/_cypher.py` plays for the Neo4j connector.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import NamedTuple

#: Synthetic property carrying the connector's derived key, declared on every
#: type we manage. Omnigraph gives edges no settable id and its auto-assigned
#: ULID is not filterable, while `where` accepts exactly one equality
#: predicate — so a declared property is the only way to address one specific
#: entity for deletion. Non-nullable, which is legal at init time.
COCO_KEY = "coco_key"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Scalar type keywords `.pg`/`.gq` accept. `DateTime` must precede `Date` in
#: the alternation below so a caller can't rely on `Date` matching first and
#: leaving `Time...` as unconsumed trailing text (Python's `re` backtracks
#: across alternatives, so match order doesn't actually matter for
#: correctness — but keeping the longer name first avoids relying on that).
_PG_BASE_TYPES = (
    "String",
    "Bool",
    "I32",
    "I64",
    "U32",
    "U64",
    "F32",
    "F64",
    "DateTime",
    "Date",
    "Blob",
)
_PG_BASE_ALT = "|".join(_PG_BASE_TYPES)
_PG_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"

#: `Vector(N)`, `[T]` for scalar `T`, and `enum(a, b, ...)`, plus an optional
#: trailing `?` for nullability — the full `.pg`/`.gq` type-expression
#: grammar. Anything else is rejected.
_PG_TYPE_RE = re.compile(
    rf"^(?:{_PG_BASE_ALT}"
    rf"|Vector\([1-9][0-9]*\)"
    rf"|\[(?:{_PG_BASE_ALT})\]"
    rf"|enum\({_PG_IDENT}(?:,\s*{_PG_IDENT})*\)"
    rf")\??$"
)


def validate_identifier(name: str, what: str) -> None:
    """Reject anything that could break out of a `.pg` or `.gq` fragment.

    Omnigraph identifiers have no quoting form we can rely on, so the only
    safe policy is to refuse non-conforming names outright.
    """
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid Omnigraph {what}: {name!r}")


def validate_pg_type(pg_type: str) -> None:
    """Reject anything that isn't a legal Omnigraph type expression.

    A mutation's `pg_type` is never bound as a parameter — it is spliced
    directly into the query *signature* (`$p_email: <pg_type>`), so this is
    as much an injection boundary as `validate_identifier` and needs the
    same strictness.
    """
    if not _PG_TYPE_RE.fullmatch(pg_type):
        raise ValueError(f"Invalid Omnigraph property type: {pg_type!r}")


def render_property(name: str, pg_type: str, *, is_key: bool) -> str:
    validate_identifier(name, "property name")
    validate_pg_type(pg_type)
    return f"{name}: {pg_type} @key" if is_key else f"{name}: {pg_type}"


#: Property names Omnigraph itself reserves on a node type. `id` is the
#: engine's own identity column, materialized from the `@key` property:
#: declaring one alongside it fails at `init`/`schema apply` with "physical
#: schema for 'node:X' must contain exactly one top-level `id` field; found
#: 2" (verified against the binary), and using `id` *as* the key fails even
#: more confusingly with "@key must reference declared properties".
_RESERVED_NODE_PROPERTIES = ("id",)

#: The same for an edge type, plus the two endpoint columns. `src`/`dst`
#: collide with the engine's stored endpoint references ("Duplicate field
#: name \"src\" in schema"); `from`/`to` parse fine in `.pg` but are the
#: literal field names an edge `insert` assigns its endpoints to, so a
#: property by either name could never be written (`build_edge_insert`
#: refuses it) — rejecting it here means the failure lands at schema
#: declaration rather than at the first write.
_RESERVED_EDGE_PROPERTIES = ("id", "src", "dst", "from", "to")


def _check_not_reserved(
    properties: Sequence[tuple[str, str]], type_name: str, reserved: Sequence[str]
) -> None:
    if any(name == COCO_KEY for name, _ in properties):
        raise ValueError(
            f"{COCO_KEY!r} is reserved by the CocoIndex connector and cannot be "
            f"declared on {type_name!r}"
        )
    clashes = sorted({name for name, _ in properties} & set(reserved))
    if clashes:
        raise ValueError(
            f"{clashes!r} {'is' if len(clashes) == 1 else 'are'} reserved by "
            f"Omnigraph and cannot be declared on {type_name!r}; rename the "
            f"field (e.g. {clashes[0]!r} -> {type_name.lower()}_{clashes[0]})"
        )


def render_node_type(
    type_name: str, properties: Sequence[tuple[str, str]], key: tuple[str, ...]
) -> str:
    validate_identifier(type_name, "node type")
    _check_not_reserved(properties, type_name, _RESERVED_NODE_PROPERTIES)
    if len(key) > 1:
        raise ValueError(
            f"Omnigraph node types support exactly one @key property, but "
            f"{type_name} declares {key!r}. The engine rejects the schema "
            f'outright ("node type {type_name} has multiple @key constraints; '
            f'only one is supported"), so a composite key cannot be expressed '
            f"at all — derive a single key field instead."
        )
    by_name = dict(properties)
    for k in key:
        if k not in by_name:
            raise ValueError(f"key property {k!r} is not declared on {type_name}")
        if by_name[k].endswith("?"):
            raise ValueError(f"key property {k!r} must not be nullable on {type_name}")
    lines = [
        f"  {render_property(name, pg_type, is_key=name in key)}"
        for name, pg_type in properties
    ]
    lines.append(f"  {render_property(COCO_KEY, 'String', is_key=False)}")
    body = "\n".join(lines)
    return f"node {type_name} {{\n{body}\n}}"


def render_edge_type(
    type_name: str,
    from_type: str,
    to_type: str,
    properties: Sequence[tuple[str, str]],
) -> str:
    validate_identifier(type_name, "edge type")
    validate_identifier(from_type, "node type")
    validate_identifier(to_type, "node type")
    _check_not_reserved(properties, type_name, _RESERVED_EDGE_PROPERTIES)
    lines = [
        f"  {render_property(name, pg_type, is_key=False)}"
        for name, pg_type in properties
    ]
    lines.append(f"  {render_property(COCO_KEY, 'String', is_key=False)}")
    body = "\n".join(lines)
    return f"edge {type_name}: {from_type} -> {to_type} {{\n{body}\n}}"


#: The start of a type block: `node NAME {` or `edge NAME:` at the
#: beginning of a line. Both forms this connector's own builders emit,
#: braced or not, share this prefix. The KIND is captured, not just the
#: name — a `.pg` may legally hold `node Link` and `edge Link` side by side
#: (the engine accepts it), and matching on the name alone made an edge's
#: fragment overwrite the node's block, destroying the node type and
#: leaving two `edge Link` blocks behind.
_TYPE_BLOCK_START_RE = re.compile(r"^(node|edge)\s+(\w+)\b", re.MULTILINE)

#: An edge block's declaration line: `edge NAME: FROM -> TO`. Matches what
#: `render_edge_type` emits and what the engine accepts.
_EDGE_ENDPOINTS_RE = re.compile(r"^edge\s+(\w+)\s*:\s*(\w+)\s*->\s*(\w+)", re.MULTILINE)


def edge_types_referencing(existing_pg: str, node_type_name: str) -> list[str]:
    """Names of edge types in `existing_pg` that use `node_type_name` as an
    endpoint. Used to refuse removing a node type out from under one."""
    return [
        m.group(1)
        for m in _EDGE_ENDPOINTS_RE.finditer(existing_pg)
        if node_type_name in (m.group(2), m.group(3))
    ]


def _find_type_block(
    existing_pg: str, kind: str, type_name: str
) -> tuple[int, int] | None:
    """Locate the `(start, end)` span of the `kind` type named `type_name`
    in `existing_pg`, or `None` if it isn't present.

    `kind` is `"node"` or `"edge"` and is part of the match, not a hint: a
    node and an edge may share a name in a valid `.pg`, and editing the
    wrong one silently destroys a type.

    Block boundaries: a block starts at `node NAME {` or `edge NAME:` at
    the beginning of a line, and ends at the matching `}` for a braced
    block, or at the end of that line for the brace-less `edge NAME: FROM
    -> TO` form — a property-less edge, which the engine both accepts and
    reproduces via `schema show` in exactly that form (verified against
    the binary; a node, by contrast, can never be brace-less — `node NAME`
    alone is a parse error). This connector's own builders always add
    `coco_key`, so they never emit the brace-less form themselves, but the
    *existing* schema being searched can legitimately contain it, from
    another tool or a `managed_by=user` type.

    Raises rather than guessing on a schema it can't edit safely: two
    blocks with the same kind and name (only the first would ever be
    rewritten, leaving the second as a stale duplicate), or unbalanced
    braces (where the splice point would land ON the opening brace and
    produce corrupt `.pg` that goes straight to `schema apply`).
    """
    if kind not in ("node", "edge"):
        raise ValueError(f"type kind must be 'node' or 'edge', got {kind!r}")
    starts = list(_TYPE_BLOCK_START_RE.finditer(existing_pg))
    matching = [m for m in starts if (m.group(1), m.group(2)) == (kind, type_name)]
    if not matching:
        return None
    if len(matching) > 1:
        raise ValueError(
            f"Omnigraph schema declares {kind} {type_name!r} "
            f"{len(matching)} times; refusing to edit an ambiguous schema"
        )
    m = matching[0]
    start = m.start()
    next_start = next(
        (later.start() for later in starts if later.start() > m.start()), None
    )
    brace_pos = existing_pg.find("{", m.end())
    if brace_pos == -1 or (next_start is not None and brace_pos > next_start):
        # Brace-less: this block is just the one line.
        newline = existing_pg.find("\n", m.end())
        return start, (newline if newline != -1 else len(existing_pg))
    depth = 0
    for i in range(brace_pos, len(existing_pg)):
        if existing_pg[i] == "{":
            depth += 1
        elif existing_pg[i] == "}":
            depth -= 1
            if depth == 0:
                return start, i + 1
    raise ValueError(
        f"Omnigraph schema has an unterminated {kind} {type_name!r} block "
        f"(unbalanced braces); refusing to edit it"
    )


def merge_type_into_schema(
    existing_pg: str, kind: str, type_name: str, fragment: str
) -> str:
    """Merge one type's rendered `.pg` fragment into the full schema source
    read back from `schema show`. `kind` is `"node"` or `"edge"` — see
    `_find_type_block` for why matching on the name alone is unsafe.

    Omnigraph's schema is applied whole-graph, not per type: `schema apply`
    and `init` both treat their input as the *complete* desired schema and
    silently drop any type omitted from it — verified against the engine:
    applying a single new type's fragment alone dropped every other
    existing type from the graph. Since each type reconciles independently
    and only ever renders its own single-type fragment, applying that
    directly would wipe the rest of the graph's schema on every type-level
    sync. This does the merge textually instead: replace the existing
    block for `type_name` if the current schema already has one, otherwise
    append it.

    Pure and unit-tested directly — deliberately not exercised only via a
    live store, since this is fiddly text handling.

    Read-merge-write is not atomic: two components applying schema
    concurrently can race this, and a lost update would silently drop
    whichever type the loser was adding or changing. Not addressed here.
    Schema actions are rare (only on type creation or change), so the
    window is small; closing it fully would need a much larger change —
    one target owning the whole graph's schema — which is out of scope.
    """
    span = _find_type_block(existing_pg, kind, type_name)
    if span is not None:
        start, end = span
        return existing_pg[:start] + fragment + existing_pg[end:]
    # Not present in the current schema -- append it.
    sep = "\n\n" if existing_pg.strip() else ""
    return existing_pg.rstrip("\n") + sep + fragment + "\n"


def remove_type_from_schema(existing_pg: str, kind: str, type_name: str) -> str:
    """Remove the `kind` type named `type_name` from the schema source
    entirely, leaving every other type untouched. A no-op if it isn't
    present.

    This is the first half of the two-`schema apply` rebuild a `@key` (or
    edge endpoint) change needs: the engine flatly rejects an in-place key
    change in one step ("removing property constraints ... not supported
    in schema migration v1", verified against the binary, regardless of
    `--allow-data-loss` — this isn't a soft/hard-drop distinction, it's an
    unsupported migration step outright) but accepts dropping the type and
    then re-adding it with its new definition as two separate calls
    (verified live, and a plain soft drop is enough — re-adding a type of
    the same name right after succeeds cleanly, no `--allow-data-loss`
    needed).
    """
    span = _find_type_block(existing_pg, kind, type_name)
    if span is None:
        return existing_pg
    start, end = span
    before, after = existing_pg[:start], existing_pg[end:]
    # Consume one adjoining blank-line separator so removing a middle block
    # doesn't leave the two neighbors glued together by a doubled one.
    if after.startswith("\n\n"):
        after = after[2:]
    elif before.endswith("\n\n"):
        before = before[:-2]
    return before + after


class PropertyValue(NamedTuple):
    """One bound value: its property name, its GQ type, and the value itself.

    The type travels with the value because a parameterized query must declare
    it in the signature (`query m($p_email: String)`).
    """

    name: str
    pg_type: str
    value: object


class Mutation(NamedTuple):
    """A complete `query <name>(...) { ... }` source plus its bound params.

    Values are never interpolated into `expr`, so a property value containing
    quotes or braces cannot alter the statement.

    `binds` (the `(name, gq_type)` pairs) and `body` (the single statement,
    unwrapped) are the structured pieces `expr` was rendered from. They exist
    so `combine_mutations` can merge several single-statement mutations into
    one query without re-parsing `expr`; `expr` itself remains the standalone
    rendering and is what every builder's own tests assert against.
    """

    expr: str
    params: dict[str, object]
    binds: tuple[tuple[str, str], ...] = ()
    body: str = ""


def _wrap(binds: Sequence[tuple[str, str]], body: str) -> str:
    sig = ", ".join(f"${n}: {t}" for n, t in binds)
    return f"query m({sig}) {{ {body} }}"


def combine_mutations(muts: Sequence[Mutation]) -> Mutation:
    """Merge single-statement mutations into ONE query — i.e. one commit.

    One CLI invocation is one commit, so N separate `mutate` calls give N
    commits and no atomicity. Every builder reuses the same parameter names
    (`p_coco_key`, `p_email`, ...), so each statement's params are
    re-prefixed `s{i}_` before merging into one query signature and body.
    """
    if not muts:
        raise ValueError("combine_mutations requires at least one mutation")
    all_binds: list[tuple[str, str]] = []
    bodies: list[str] = []
    params: dict[str, object] = {}
    for i, m in enumerate(muts):
        body = m.body
        for name, gq_type in m.binds:
            new = f"s{i}_{name}"
            # A `$` sentinel plus a trailing word boundary is enough to avoid
            # one param name being a prefix of another (e.g. `$p_a` inside
            # `$p_ab`) — `\b` after the name stops the match at a non-word
            # character, so `$p_a` never matches inside `$p_ab`.
            body = re.sub(rf"\${re.escape(name)}\b", f"${new}", body)
            all_binds.append((new, gq_type))
            params[new] = m.params[name]
        bodies.append(body)
    combined_body = " ".join(bodies)
    return Mutation(
        _wrap(all_binds, combined_body), params, tuple(all_binds), combined_body
    )


def _bind(
    props: Sequence[PropertyValue], prefix: str
) -> tuple[list[tuple[str, str]], list[str], dict[str, object]]:
    binds: list[tuple[str, str]] = []
    assigns: list[str] = []
    params: dict[str, object] = {}
    seen: set[str] = set()
    for prop in props:
        validate_identifier(prop.name, "property name")
        validate_pg_type(prop.pg_type)
        if prop.name == COCO_KEY:
            raise ValueError(
                f"{COCO_KEY!r} is reserved by the CocoIndex connector and cannot be "
                f"supplied as a property value"
            )
        if prop.name in seen:
            raise ValueError(f"Duplicate property name: {prop.name!r}")
        seen.add(prop.name)
        param = f"{prefix}_{prop.name}"
        binds.append((param, prop.pg_type))
        assigns.append(f"{prop.name}: ${param}")
        params[param] = prop.value
    return binds, assigns, params


def _coco_key_bind(coco_key: str) -> tuple[tuple[str, str], str, dict[str, object]]:
    param = f"p_{COCO_KEY}"
    return (param, "String"), f"{COCO_KEY}: ${param}", {param: coco_key}


def build_node_upsert(
    type_name: str, props: Sequence[PropertyValue], coco_key: str
) -> Mutation:
    """Keyed node `insert` is an upsert by the derived key tuple."""
    validate_identifier(type_name, "node type")
    binds, assigns, params = _bind(props, "p")
    ck_bind, ck_assign, ck_params = _coco_key_bind(coco_key)
    binds.append(ck_bind)
    assigns.append(ck_assign)
    params.update(ck_params)
    body = f"insert {type_name} {{ {', '.join(assigns)} }}"
    return Mutation(_wrap(binds, body), params, tuple(binds), body)


def build_endpoint_stub(
    type_name: str, key_props: Sequence[PropertyValue], coco_key: str
) -> Mutation:
    """Key-only upsert, so an edge can reference a node its owning component
    has not written yet. The owner's own upsert later fills in the rest."""
    return build_node_upsert(type_name, key_props, coco_key)


def _delete_by_coco_key(type_name: str, coco_key: str) -> Mutation:
    ck_bind, _, ck_params = _coco_key_bind(coco_key)
    body = f"delete {type_name} where {COCO_KEY} = ${ck_bind[0]}"
    return Mutation(_wrap([ck_bind], body), ck_params, (ck_bind,), body)


def build_node_delete(type_name: str, coco_key: str) -> Mutation:
    validate_identifier(type_name, "node type")
    return _delete_by_coco_key(type_name, coco_key)


def build_edge_delete(type_name: str, coco_key: str) -> Mutation:
    validate_identifier(type_name, "edge type")
    return _delete_by_coco_key(type_name, coco_key)


def build_edge_insert(
    type_name: str,
    from_ref: PropertyValue,
    to_ref: PropertyValue,
    props: Sequence[PropertyValue],
    coco_key: str,
) -> Mutation:
    """Edge `insert` always creates a new edge — Omnigraph never deduplicates
    and provides no settable id. Idempotence comes entirely from the connector
    refusing to re-insert an edge whose `coco_key` it already tracks, and from
    deleting by `coco_key` before re-inserting a changed one.

    `from_ref.name`/`to_ref.name` are ignored — the assignment is always
    literally `from`/`to`; only `.pg_type` and `.value` address the endpoint.
    """
    validate_identifier(type_name, "edge type")
    validate_pg_type(from_ref.pg_type)
    validate_pg_type(to_ref.pg_type)
    for prop in props:
        if prop.name in ("from", "to"):
            raise ValueError(
                f"{prop.name!r} is reserved for the edge endpoint and cannot be "
                f"supplied as a property value"
            )
    binds = [("e_from", from_ref.pg_type), ("e_to", to_ref.pg_type)]
    assigns = ["from: $e_from", "to: $e_to"]
    params: dict[str, object] = {"e_from": from_ref.value, "e_to": to_ref.value}
    pbinds, passigns, pparams = _bind(props, "p")
    binds += pbinds
    assigns += passigns
    params.update(pparams)
    ck_bind, ck_assign, ck_params = _coco_key_bind(coco_key)
    binds.append(ck_bind)
    assigns.append(ck_assign)
    params.update(ck_params)
    body = f"insert {type_name} {{ {', '.join(assigns)} }}"
    return Mutation(_wrap(binds, body), params, tuple(binds), body)
