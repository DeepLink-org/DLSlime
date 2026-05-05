from __future__ import annotations

import argparse
import json

from nanoctrl import NanoCtrlClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Minimal NanoCtrl client example")
    parser.add_argument(
        "--address",
        default="127.0.0.1:3000",
        help="NanoCtrl address, for example 10.0.0.12:3000",
    )
    parser.add_argument("--scope", default=None, help="Optional NanoCtrl scope")
    args = parser.parse_args()

    client = NanoCtrlClient(address=args.address, scope=args.scope)
    client.check_connection()

    redis_url = client.get_redis_url()
    engines = client.list_engines()

    print("connected:", args.address)
    print("redis_url:", redis_url)
    print("engines:")
    print(json.dumps(engines, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
