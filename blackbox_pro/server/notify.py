from __future__ import annotations

import os
import logging
from typing import Any

import httpx

_logger = logging.getLogger("blackbox-pro")

def _post_json(url: str, payload: dict[str, Any]) -> None:
    try:
        httpx.post(url, json=payload, timeout=5.0)
    except Exception as e:
        _logger.debug("Notification failed for %s: %s", url, e)


def notify(event: str, payload: dict[str, Any]) -> None:
    """
    Send notifications to configured webhooks.
    Supported env vars:
      BLACKBOX_PRO_SLACK_WEBHOOK
      BLACKBOX_PRO_TEAMS_WEBHOOK
      BLACKBOX_PRO_PAGERDUTY_WEBHOOK
    """
    data = {"event": event, **payload}

    slack = os.environ.get("BLACKBOX_PRO_SLACK_WEBHOOK")
    if slack:
        _post_json(slack, {"text": f"[Blackbox] {event}", "data": data})

    teams = os.environ.get("BLACKBOX_PRO_TEAMS_WEBHOOK")
    if teams:
        _post_json(teams, {"text": f"[Blackbox] {event}", "data": data})

    pagerduty = os.environ.get("BLACKBOX_PRO_PAGERDUTY_WEBHOOK")
    if pagerduty:
        _post_json(pagerduty, {"event": event, "payload": data})
