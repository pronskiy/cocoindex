<h1 align="center">Turn Google Docs that <code>[[link]]</code> to each other into a <em>self-updating</em> graph in Omnigraph.</h1>

<p align="center">
  <b>Every Doc becomes a node, every <code>[[wiki link]]</code> becomes an edge — no LLM, no hand-written schema, about 100 lines of async Python.</b><br/>
  Edit, add or trash a Doc, re-run, and only that Doc is re-read; the graph follows.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/connectors/omnigraph/" title="Read the Omnigraph connector docs"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A team wiki in Google Docs is already a graph: "see [[onboarding]]", "details in [[billing-v2]]". Drive just can't tell you what links where, which pages nothing points to, or which links lead nowhere. This example syncs a Drive folder into [Omnigraph](https://github.com/ModernRelay/omnigraph) — a lakehouse-native property-graph database served over HTTP by `omnigraph-server` — so you can ask the graph instead. It's the smallest end-to-end Omnigraph pipeline in this repo: no LLM, one node type, one edge type. For LLM extraction plus entity resolution, see [`meeting_notes_graph_omnigraph`](../meeting_notes_graph_omnigraph).

## How it works

- **`Note`** nodes — one per Google Doc, keyed by the slug of the Doc's title (`Billing V2` → `billing-v2`), carrying the first line as `title` and a word count.
- **`LinksTo`** edges — `Note -> Note`, one per distinct `[[link]]` in the Doc. The link text is slugged the same way, so `[[Billing V2]]` and `[[billing-v2]]` point at the same node.

The whole pipeline is one function per Doc — read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_doc(file, notes, links) -> None:
    text = await file.read_text()
    slug = _slug(file.file_path.path.name)
    notes.declare_node(node=Note(slug=slug, title=..., words=len(text.split())))
    for target in sorted({_slug(name) for name in _LINK_RE.findall(text)}):
        links.declare_edge(from_id=slug, to_id=target)
```

What happens on each kind of change:

| You do this in Drive | The graph does this |
|---|---|
| Edit a Doc | Only that Doc is re-read; its `title`, `words` and outgoing edges are updated |
| Link to a Doc that doesn't exist yet | The target appears as a key-only stub node, so the edge still lands |
| Write that Doc | The stub fills in |
| Trash a Doc | Its node and outgoing edges go; if other Docs still link to it, it stays as a stub |

## Run it

**1. Start Omnigraph 0.14** with a graph named `wiki` (its `wiki.pg` may start empty), registered as the CLI target `local`. The [connector docs](https://cocoindex.io/docs/connectors/omnigraph/#local-development) have the full recipe; it's the same setup as [`meeting_notes_graph_omnigraph`](../meeting_notes_graph_omnigraph#run-it) with the graph id changed.

**2. Share a Drive folder** of Google Docs with a service account (see [Setting up a service account](https://cocoindex.io/docs/connectors/google_drive/#setting-up-a-service-account)). Only native Google Docs are read; link between them with `[[Doc title]]`.

**3. Configure & install:**

```sh
cp .env.example .env     # set GOOGLE_SERVICE_ACCOUNT_CREDENTIAL, GOOGLE_DRIVE_ROOT_FOLDER_IDS
pip install -e .
```

The `OMNIGRAPH_*` variables point at the server, the graph and the cluster directory the connector deploys `Note` and `LinksTo` through — the server picks the new types up live, no restart.

**4. Build the graph:**

```sh
cocoindex update main
```

The Google Drive source has no live mode, so re-run it after you edit the Docs, or poll:

```sh
while true; do cocoindex update main; sleep 30; done
```

**5. Ask the graph:**

```sh
# Every link
omnigraph query --target local --graph wiki --json -e \
  'query q() { match { $a: Note $a linksTo $b } return { $a.slug, $b.slug } }'

# Backlinks: which Docs point at "alpha"
omnigraph query --target local --graph wiki --json -e \
  'query q() { match { $a: Note $b: Note { slug: "alpha" } $a linksTo $b } return { $a.slug, $a.title } }'

# Every note; stubs (linked, never written) come back without title and words
omnigraph query --target local --graph wiki --json -e \
  'query q() { match { $n: Note } return { $n.slug, $n.title, $n.words } }'
```

To remove everything this example put in the graph, types included, run `cocoindex drop main`.

---

<p align="center">
  If this turned your Drive into a graph, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/connectors/omnigraph/">Omnigraph connector</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/gdrive_wiki_graph_omnigraph" alt="" width="1" height="1" />
