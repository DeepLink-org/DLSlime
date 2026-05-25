# Releasing

This repo publishes three artifacts on every release:

| Artifact                               | Where                                                                                | Auth                    |
| -------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------- |
| `dlslime` Python wheels (cp310..cp313) | [PyPI](https://pypi.org/project/dlslime/)                                            | OIDC, no secrets        |
| `dlslime-ctrl` Python wheel (Rust bin) | [PyPI](https://pypi.org/project/dlslime-ctrl/)                                       | OIDC, no secrets        |
| `dlslime-ctrl` docker image            | [GHCR](https://github.com/orgs/DeepLink-org/packages/container/package/dlslime-ctrl) | built-in `GITHUB_TOKEN` |

All three are triggered automatically by pushing a `vX.Y.Z` tag (which [`scripts/release.sh`](scripts/release.sh) does for you).

## TL;DR for an existing release setup

```bash
git checkout main && git pull
./scripts/release.sh 0.1.2
#   y  to confirm
# Wait ~3 min, then:
#   - https://github.com/DeepLink-org/DLSlime/actions
#   - https://pypi.org/project/dlslime/0.1.2/
#   - docker pull ghcr.io/deeplink-org/dlslime-ctrl:0.1.2
```

The rest of this doc is the **one-time setup** required before the very first release.

______________________________________________________________________

## One-time setup (per registry)

### 1. GHCR (docker image)

**Already done.** The `docker-publish.yml` workflow uses the built-in
`GITHUB_TOKEN` — no configuration needed. After the first push, go to
<https://github.com/orgs/DeepLink-org/packages/container/dlslime-ctrl/settings>
and change visibility to **Public**.

### 2. PyPI Trusted Publishing — `dlslime`

PyPI's [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) lets
GitHub Actions sign in via OIDC without storing any API token.

**Catch-22:** A Trusted Publisher can only be added for an existing PyPI
project. So the very first release has to be uploaded with a normal API token.

#### Bootstrap (one-time per project)

```bash
# Build a sdist locally
python -m build --sdist dlslime
# Upload with a one-shot token
twine upload dlslime/dist/dlslime-*.tar.gz
# (token comes from https://pypi.org/manage/account/token/ — scope "Entire account",
#  delete the token immediately after.)
```

After this, `https://pypi.org/project/dlslime/` exists.

#### Configure Trusted Publisher

1. Open <https://pypi.org/manage/project/dlslime/settings/publishing/>
2. Click **Add a new pending publisher** → fill in:
   - **PyPI Project Name**: `dlslime`
   - **Owner**: `DeepLink-org`
   - **Repository name**: `DLSlime`
   - **Workflow name**: `pypi-publish.yml`
   - **Environment name**: `pypi`
3. Save.

#### Configure GitHub environment (manual-approval gate)

1. Open <https://github.com/DeepLink-org/DLSlime/settings/environments>
2. **New environment** → name it `pypi`
3. Under **Deployment protection rules**, add **Required reviewers** = yourself
   (and any other maintainer you trust). Every release will pause and wait for
   one click of "Approve" before uploading. Lets you abort if a tag was pushed
   by mistake.

### 3. PyPI Trusted Publishing — `dlslime-ctrl`

Same as #2 but for the `dlslime-ctrl` project. Bootstrap once:

```bash
cd dlslime-ctrl
pip install "maturin>=1.0,<2.0"
maturin build --release --out dist
twine upload dist/*.whl
```

Then add the pending publisher at
<https://pypi.org/manage/project/dlslime-ctrl/settings/publishing/> with the
same owner / repo / workflow / environment as `dlslime`.

______________________________________________________________________

## Normal release flow

After the one-time setup above, every release is two commands:

```bash
git checkout main && git pull
./scripts/release.sh 0.1.2
```

`release.sh` will:

1. Bump version in all manifests (`pyproject.toml` × 4 + `Cargo.toml`).
2. Run `cargo update -p dlslime-ctrl` to refresh `Cargo.lock`.
3. Replace old version literals in user-facing docs (`docker/README.md`, env example, compose file, workflow comment).
4. Show the diff, ask for `y/N` confirmation.
5. `git commit -am "release: v0.1.2"`
6. `git tag -a v0.1.2`
7. `git push origin HEAD && git push origin v0.1.2`

GitHub Actions then:

| Workflow             | Trigger  | Output                                                        |
| -------------------- | -------- | ------------------------------------------------------------- |
| `docker-publish.yml` | tag push | `ghcr.io/deeplink-org/dlslime-ctrl:0.1.2` (+ `0.1`, `latest`) |
| `pypi-publish.yml`   | tag push | `dlslime==0.1.2`, `dlslime-ctrl==0.1.2` on PyPI               |

The PyPI workflow pauses on the `pypi` environment until you approve in the
Actions UI. Saves you from a bad release if you fat-finger a tag.

## Dry-run / local-only testing

```bash
./scripts/release.sh --dry-run 0.1.2     # show all changes, no git ops
./scripts/release.sh --no-push 0.1.2     # commit + tag locally, skip push
```

To test the PyPI workflow without uploading: trigger it manually with `dry_run = true` from the Actions UI. Wheels will be built and uploaded as workflow artifacts, but no PyPI upload.

## Re-running a failed release

If the workflow fails mid-way (e.g. one wheel failed to build):

```bash
# In the Actions UI, click "Re-run failed jobs" on the failed workflow run.
# Successful jobs (already-uploaded wheels) will be skipped — PyPI rejects
# duplicate version uploads, so there's no risk of corruption.
```

If you need to **fully restart** with the same version (very rare; means
something was published wrong):

1. **Yank** the bad version from PyPI: <https://pypi.org/manage/project/dlslime/release/0.1.2/>
2. Bump to `0.1.3` and re-release. PyPI does not allow re-uploading a yanked or deleted version under the same number.

## Versioning policy

- `X.Y.Z` — semver. Use `MAJOR.MINOR.PATCH`.
- Pre-releases: append `.rc1`, `.rc2` (PyPI / pip accept these; cargo doesn't,
  but we don't publish `dlslime-ctrl` to crates.io).
- Dev iterations between releases: don't tag. CI auto-publishes `:edge` and
  `:sha-<7-char>` docker tags from every push to `main`.
