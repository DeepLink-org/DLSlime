# Versions

DLSlime documentation uses [mike](https://github.com/jimporter/mike) for
versioned publishing with the MkDocs Material version selector.

## Published Channels

| Channel  | Meaning                                     | How it is published                            |
| -------- | ------------------------------------------- | ---------------------------------------------- |
| `dev`    | Documentation from `main` or `master`       | GitHub Actions deploys branch builds as `dev`  |
| `latest` | Alias pointing at the newest published docs | Updated on branch and tag builds               |
| `x.y.z`  | Release documentation                       | Git tags like `v0.1.0` publish version `0.1.0` |

## Local Preview

For normal authoring:

```bash
cd docs
make serve
```

For a versioned preview that exercises the same selector used on GitHub Pages:

```bash
cd docs
make deploy-dev
make serve-versioned
```

`make deploy-dev` writes a local `gh-pages` branch entry for `dev/latest`.
It does not push unless you run the GitHub Actions workflow or call `mike` with
`--push`.

## Release Flow

Tag releases with a `v` prefix:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The docs workflow strips the leading `v` and publishes version `0.1.0` with the
`latest` alias.
