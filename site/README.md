# Janus site

Static landing page for [Janus](https://github.com/samuelfabel/janus).

## Develop

```bash
cd site
npm install
npm run dev
```

## Build

```bash
cd site
npm run build
```

Output is written to `site/dist`. The production base path is `/janus/` for GitHub Pages at `https://samuelfabel.github.io/janus/`.

## Deploy

GitHub Actions workflow `.github/workflows/pages.yml` builds this folder and publishes to GitHub Pages on pushes to `main` that touch `site/**`.

Enable **Settings → Pages → Source: GitHub Actions** once in the repository settings.
