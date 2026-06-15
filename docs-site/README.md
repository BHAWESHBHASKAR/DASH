# DASH documentation site

This directory contains the [mkdocs-material](https://squidfunk.github.io/mkdocs-material/) site that powers the public DASH documentation.

## Layout

```
docs-site/
├── mkdocs.yml            # site configuration (nav, theme, extensions)
├── requirements.txt      # pinned Python deps for the site build
├── README.md             # this file
└── docs/                 # markdown source for every page
    ├── index.md
    ├── quickstart.md
    ├── concepts/
    ├── guides/
    ├── operations/
    ├── reference/
    └── about/
```

## Run locally

```bash
cd docs-site
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdocs serve
```

The site will be available at <http://127.0.0.1:8000/> with live reload.

## Build

```bash
mkdocs build
```

Static output goes to `docs-site/site/`. The output is fully self-contained
(CSS, JS, search index) and can be served from any static host or served
behind a CDN.

## Deploy

Deployment is handled by the GitHub Actions workflow at
`.github/workflows/docs.yml`. Every push to `main` that changes anything
under `docs-site/` rebuilds and publishes the site via the
`peaceiris/actions-gh-pages` action. The published site is available at
<https://BHAWESHBHASKAR.github.io/DASH/>.

To cut a manual deploy without pushing to `main`:

```bash
mkdocs gh-deploy --force
```

## Conventions

- One file per nav entry. File names match the nav slug.
- Use admonitions (`!!! note`, `!!! warning`) for asides.
- Use `=== "Tab title"` (pymdownx.tabbed) for parallel code samples in
  different languages — keeps Python / Go / TypeScript / curl side by
  side.
- Code blocks must be runnable. Every snippet that is shown as `python`,
  `bash`, etc. has been exercised against the Docker compose stack.
- Internal links use relative paths (`../concepts/architecture.md`).
