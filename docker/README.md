# Docker deployment of `dlslime-ctrl`

Two compose recipes for the DLSlime control plane:

| File                                | Redis                       | Use case                          |
| ----------------------------------- | --------------------------- | --------------------------------- |
| `docker-compose.yml`                | Bundled (host port `16379`) | Self-contained dev / test / CI    |
| `docker-compose.external-redis.yml` | External                    | Production sharing existing Redis |

Both build the same image from [`ctrl.Dockerfile`](./ctrl.Dockerfile) (multi-stage Rust → `debian:bookworm-slim`, ~80 MB).

## Default host ports

| Service                      | Host port | Override env                   |
| ---------------------------- | --------- | ------------------------------ |
| dlslime-ctrl HTTP            | **4479**  | `DLSLIME_CTRL_PORT`            |
| Redis (bundled compose only) | **16379** | `DLSLIME_CTRL_REDIS_HOST_PORT` |

Both are intentionally non-standard so they don't collide with a local Redis on `6379` or an existing HTTP service on `3000` / `8080`. Override them in `docker/.env` if you want.

## Bundled Redis (recommended quick start)

```bash
# From the DLSlime repo root.
docker compose -f docker/docker-compose.yml up -d
```

What happens:

- `dlslime-ctrl` listens on `http://localhost:4479`.
- Redis listens on `localhost:16379`.
- Inside the compose network: ctrl talks to Redis via the service-name DNS (`redis://redis:6379`) — fast, no NAT hop.
- For PeerAgents outside the compose network: ctrl's `/get_redis_address` returns `redis://${DLSLIME_CTRL_ADVERTISE_HOST}:16379` (default `127.0.0.1`) thanks to the `DLSLIME_CTRL_REDIS_ADVERTISE` env var. PeerAgent then connects directly to Redis on the host port.

### Cross-host PeerAgents

If PeerAgents run on a different machine, the default `127.0.0.1` won't be reachable. Tell ctrl what hostname to advertise:

```bash
cp docker/.env.example docker/.env
echo "DLSLIME_CTRL_ADVERTISE_HOST=10.0.0.42" >> docker/.env   # your LAN IP or FQDN
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

## External Redis

```bash
cp docker/.env.example docker/.env
$EDITOR docker/.env                   # set DLSLIME_CTRL_REDIS_URL=redis://your-host:6379
docker compose -f docker/docker-compose.external-redis.yml --env-file docker/.env up -d
```

Common external Redis URLs:

- `redis://my-redis.internal:6379` — LAN / cluster Redis
- `redis://host.docker.internal:6379` — Redis running on the Docker host
  (Linux only: also uncomment `extra_hosts: host.docker.internal:host-gateway` in `docker-compose.external-redis.yml`)

## Configuration

| Variable                       | Default                        | Description                                                                                             |
| ------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------- |
| `DLSLIME_CTRL_PORT`            | `4479`                         | Host port mapped to dlslime-ctrl                                                                        |
| `DLSLIME_CTRL_REDIS_HOST_PORT` | `16379`                        | Host port mapped to bundled Redis                                                                       |
| `DLSLIME_CTRL_ADVERTISE_HOST`  | `127.0.0.1`                    | Hostname/IP that ctrl tells PeerAgents to use for Redis                                                 |
| `DLSLIME_CTRL_REDIS_URL`       | `redis://redis:6379` (bundled) | What ctrl uses to connect to Redis itself                                                               |
| `DLSLIME_CTRL_REDIS_ADVERTISE` | derived in compose             | Overrides what ctrl returns from `/get_redis_address`; used to decouple internal vs external Redis URLs |
| `DLSLIME_CTRL_RUST_LOG`        | `info`                         | Rust log level (`error` / `warn` / `info` / `debug` / `trace`)                                          |

## Smoke test

```bash
curl http://localhost:4479/                                   # health
curl -X POST http://localhost:4479/get_redis_address \
     -H 'Content-Type: application/json' -d '{}'              # should return redis://<advertise-host>:16379
curl -X POST http://localhost:4479/list_entities \
     -H 'Content-Type: application/json' -d '{}'
redis-cli -h 127.0.0.1 -p 16379 ping                          # should print "PONG"
```

## Building only (no run)

```bash
docker compose -f docker/docker-compose.yml build ctrl
```

## Cleanup

```bash
docker compose -f docker/docker-compose.yml down -v
```

## Publishing to GitHub Container Registry (GHCR)

Published image: [`ghcr.io/deeplink-org/dlslime-ctrl`](https://github.com/orgs/DeepLink-org/packages/container/package/dlslime-ctrl).

Why GHCR rather than Docker Hub:

- 100% free for public images, no anonymous pull rate limit.
- CI auth uses the built-in `GITHUB_TOKEN` — no secrets to configure.
- Tied to the GitHub org, so access control follows the repo.

### Just want to use it (no build)

```bash
cat >> docker/.env <<'EOF'
DLSLIME_CTRL_IMAGE=ghcr.io/deeplink-org/dlslime-ctrl:0.1.20
DLSLIME_CTRL_PULL_POLICY=missing
EOF

docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

`pull_policy` values:

| Value                          | Behavior                                    |
| ------------------------------ | ------------------------------------------- |
| `build` *(default in compose)* | Always build locally, never pull            |
| `missing`                      | Pull only if the tag is not present locally |
| `always`                       | Always pull on `up`                         |
| `never`                        | Use local tag, error if missing             |

### Automated publish via GitHub Actions

The workflow [`.github/workflows/docker-publish.yml`](../.github/workflows/docker-publish.yml) builds & pushes automatically — no secrets needed.

| Trigger                    | Tags published                           |
| -------------------------- | ---------------------------------------- |
| Push to `main` / `master`  | `edge`, `sha-<short>`                    |
| Push tag `v0.1.20`         | `0.1.20`, `0.1`, `latest`, `sha-<short>` |
| Manual `workflow_dispatch` | optional extra tag from the input        |

One-time setup after the **first** successful workflow run, in the GitHub UI:

1. Go to <https://github.com/orgs/DeepLink-org/packages/container/dlslime-ctrl/settings>
2. **Danger Zone → Change visibility → Public** — otherwise anonymous `docker pull` will 404.
3. (Optional) Link the package to the repo so it shows up on the repo sidebar.

Releasing a new version:

```bash
# bump versions in:
#   Cargo.toml, dlslime-ctrl/pyproject.toml, dlslime/pyproject.toml, pyproject.toml
git commit -am "release: v0.1.20"
git tag v0.1.20
git push origin main --tags
```

The workflow will build `linux/amd64` + `linux/arm64` and push `0.1.20`, `0.1`, `latest`.

### Manual push (without CI)

```bash
# 1. Create a classic PAT at https://github.com/settings/tokens
#    Scopes: write:packages, read:packages
#    (and `delete:packages` if you want to clean up tags later)
export GHCR_PAT=ghp_xxx...

# 2. Log in to GHCR.
echo "$GHCR_PAT" | docker login ghcr.io -u <your-github-username> --password-stdin

# 3. Build multi-arch and push.
docker buildx create --use --name dlslime-builder 2>/dev/null || docker buildx use dlslime-builder

VERSION=0.1.20
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -f docker/ctrl.Dockerfile \
    -t ghcr.io/deeplink-org/dlslime-ctrl:${VERSION} \
    -t ghcr.io/deeplink-org/dlslime-ctrl:0.1 \
    -t ghcr.io/deeplink-org/dlslime-ctrl:latest \
    --push .
```

> Behind a corporate / China proxy? The `dlslime-builder` container can't reach a proxy on the host's `127.0.0.1`. Make the proxy listen on `0.0.0.0` (e.g. `allow-lan: true` in Clash) and pass the docker-bridge gateway in via build-args:
>
> ```bash
> docker buildx build \
>     --build-arg HTTP_PROXY=http://172.17.0.1:7897 \
>     --build-arg HTTPS_PROXY=http://172.17.0.1:7897 \
>     ...
> ```
