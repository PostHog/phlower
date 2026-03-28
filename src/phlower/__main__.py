"""python -m phlower"""

from __future__ import annotations

import logging


def main() -> None:
    import uvicorn

    from .app import create_app

    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    # Silence noisy kombu reconnect logs
    logging.getLogger("kombu").setLevel(logging.WARNING)

    app = create_app()

    port = int(os.environ.get("PORT", "8100"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
