from __future__ import annotations

import os

from .api import ProConfig, create_app

# Read root from env for reload subprocess support.
_ROOT = os.environ.get("BLACKBOX_ROOT", ".blackbox_store")

app = create_app(ProConfig(root=_ROOT))

