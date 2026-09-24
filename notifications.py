"""Persistent notification center. Outbound delivery is opt-in and secrets stay in env vars."""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from typing import Any

from storage import Store


class NotificationCenter:
    def __init__(self, config: dict[str, Any], store: Store) -> None:
        self.config = config
        self.store = store

    def emit(self, event: str, message: str, payload: dict[str, Any] | None = None, level: str = "INFO") -> dict[str, Any]:
        notification_id = self.store.notify(level, event, message, payload, False)
        delivered: list[str] = ["IN_APP"] if self.config.get("notifications", {}).get("in_app", True) else []
        errors: list[str] = []
        if self.config.get("notifications", {}).get("outbound_enabled", False):
            for channel, sender in (("TELEGRAM", self._telegram), ("DISCORD", self._discord)):
                try:
                    if sender(message):
                        delivered.append(channel)
                except Exception as exc:
                    errors.append(f"{channel}: {exc}")
        return {"id": notification_id, "delivered": delivered, "errors": errors}

    def _telegram(self, message: str) -> bool:
        settings = self.config["notifications"]
        token = os.environ.get(settings.get("telegram_token_env", ""), "")
        chat_id = os.environ.get(settings.get("telegram_chat_id_env", ""), "")
        if not token or not chat_id:
            return False
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
        with urllib.request.urlopen(urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data), timeout=10):
            return True

    def _discord(self, message: str) -> bool:
        env_name = self.config["notifications"].get("discord_webhook_env", "")
        url = os.environ.get(env_name, "")
        if not url:
            return False
        request = urllib.request.Request(url, data=json.dumps({"content": message}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10):
            return True
