from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import sys

from .config import settings_from_args, validate_settings
from .metrics import Metrics
from .server import serve
from .storage import Storage


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings, args = settings_from_args()
        inspect = (
            args.list_datasets
            or args.list_keys is not None
            or args.get is not None
            or args.get_base64 is not None
        )
        if args.limit < 1 or args.limit > 1000:
            raise ValueError("--limit must be between 1 and 1000")
        if args.offset < 0:
            raise ValueError("--offset must not be negative")
        if args.backup or args.restore or args.check or inspect:
            metrics = Metrics()
            storage = Storage(settings, metrics)
            try:
                if args.backup:
                    from pathlib import Path

                    storage.backup(Path(args.backup))
                    print(f"Backup created: {args.backup}")
                elif args.restore:
                    from pathlib import Path

                    storage.restore(Path(args.restore))
                    print("Restore completed and integrity check passed")
                elif args.check:
                    result = {
                        "integrity": storage.check_integrity(),
                        "storage": storage.stats(),
                    }
                    print(json.dumps(result, indent=2))
                elif args.list_datasets:
                    print(json.dumps({"datasets": storage.list_datasets()}, indent=2))
                elif args.list_keys is not None:
                    keys = storage.list_keys(
                        args.list_keys,
                        limit=args.limit,
                        offset=args.offset,
                    )
                    print(
                        json.dumps(
                            {
                                "dataset": args.list_keys,
                                "offset": args.offset,
                                "limit": args.limit,
                                "keys": [
                                    {
                                        "text": _utf8(key),
                                        "base64": base64.b64encode(key).decode("ascii"),
                                    }
                                    for key in keys
                                ],
                            },
                            indent=2,
                        )
                    )
                elif args.get is not None or args.get_base64 is not None:
                    if args.get is not None:
                        dataset, key_text = args.get
                        key = key_text.encode("utf-8")
                    else:
                        dataset, key_encoded = args.get_base64
                        try:
                            key = base64.b64decode(key_encoded, validate=True)
                        except binascii.Error as error:
                            raise ValueError("KEY_BASE64 must be valid standard base64") from error
                        key_text = _utf8(key)
                    value = storage.get(dataset, key)
                    result = {"dataset": dataset, "key": key_text, "found": value is not None}
                    result["key_base64"] = base64.b64encode(key).decode("ascii")
                    if value is not None:
                        result["value_base64"] = base64.b64encode(value).decode("ascii")
                        result["value_text"] = _utf8(value)
                    print(json.dumps(result, indent=2, ensure_ascii=True))
            finally:
                storage.close()
            return 0

        validate_settings(settings)
        asyncio.run(serve(settings))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        logging.getLogger("blinkdb").error("%s", error)
        return 1


def _utf8(value: bytes) -> str | None:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return None


if __name__ == "__main__":
    sys.exit(main())
