from __future__ import annotations

import asyncio
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
        if args.backup or args.restore or args.check:
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
                else:
                    result = {
                        "integrity": storage.check_integrity(),
                        "storage": storage.stats(),
                    }
                    print(json.dumps(result, indent=2))
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


if __name__ == "__main__":
    sys.exit(main())
