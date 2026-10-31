"""
Google Docs Wiki Graph (v1) — CocoIndex pipeline example.

Turn a Google Drive folder of Docs that link to each other with [[wiki
links]] into a graph in Omnigraph:

  Note     nodes — one per Doc, keyed by the slug of the Doc's title
  LinksTo  edges — Note -> Note, one per [[link]] in the Doc

Index (one-shot catch-up; the google_drive source has no live mode):
    cocoindex update main

A link to a Doc that doesn't exist yet still lands: the connector writes a
key-only stub for the target, and the stub fills in once the Doc is written.
"""

from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import cocoindex as coco
from cocoindex.connectors import google_drive, omnigraph

KG_DB = coco.ContextKey[omnigraph.ConnectionFactory]("wiki_graph_db")

# Drive exports a Doc as Markdown and escapes the brackets: `\[\[beta\]\]`.
_LINK_RE = re.compile(r"\\?\[\\?\[([A-Za-z0-9 _-]+?)\\?\]\\?\]")


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    kg = omnigraph.ConnectionFactory(
        os.environ.get("OMNIGRAPH_URL", "http://127.0.0.1:8080"),
        graph_id=os.environ.get("OMNIGRAPH_GRAPH", "wiki"),
        branch=os.environ.get("OMNIGRAPH_BRANCH", "main"),
        cluster=omnigraph.ClusterConfig(
            config_dir=os.environ.get("OMNIGRAPH_CLUSTER_DIR", "./cluster"),
            target=os.environ.get("OMNIGRAPH_CLUSTER_TARGET", "local"),
            schema_file=os.environ.get("OMNIGRAPH_SCHEMA_FILE", "wiki.pg"),
        ),
    )
    builder.provide(KG_DB, kg)
    yield
    await kg.close()


@dataclass
class Note:
    slug: str
    # Optional so a linked-but-unwritten Doc can exist as a key-only stub.
    title: str | None
    words: int | None


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


@coco.fn(memo=True)
async def process_doc(
    file: google_drive.DriveFile,
    notes: omnigraph.NodeTarget[Note],
    links: omnigraph.EdgeTarget[Any],
) -> None:
    text = await file.read_text()
    slug = _slug(file.file_path.path.name)
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    notes.declare_node(
        node=Note(
            slug=slug,
            title=first_line.lstrip("# ").strip() or None,
            words=len(text.split()),
        )
    )
    for target in sorted({_slug(name) for name in _LINK_RE.findall(text)}):
        links.declare_edge(from_id=slug, to_id=target)


@coco.fn
async def app_main() -> None:
    notes = await omnigraph.mount_node_target(
        KG_DB, "Note", await omnigraph.NodeSchema.from_class(Note, key="slug")
    )
    links = await omnigraph.mount_edge_target(KG_DB, "LinksTo", notes, notes)

    source = google_drive.GoogleDriveSource(
        service_account_credential_path=os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"],
        root_folder_ids=[
            folder.strip()
            for folder in os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")
            if folder.strip()
        ],
        mime_types=["application/vnd.google-apps.document"],
    )
    await coco.mount_each(process_doc, source.items(), notes, links)


app = coco.App(coco.AppConfig(name="GDriveWikiGraphOmnigraph"), app_main)
