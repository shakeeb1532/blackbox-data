import time
from pathlib import Path
from playwright.sync_api import sync_playwright
import httpx
import os

BASE = os.environ.get("BLACKBOX_PRO_BASE", "http://127.0.0.1:8088")
TOKEN = os.environ.get("BLACKBOX_PRO_TOKEN", "dev-secret-token")
OUT = Path("docs/screens")
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        page.goto(f"{BASE}/ui/home?token={TOKEN}")
        page.set_viewport_size({"width": 1280, "height": 720})
        time.sleep(1)
        page.screenshot(path=str(OUT / "ui_home.png"), full_page=True)

        # Try to discover latest run
        run_id = ""
        try:
            resp = httpx.get(
                f"{BASE}/runs",
                params={"project": "acme-data", "dataset": "demo"},
                headers={"Authorization": f"Bearer {TOKEN}"},
                timeout=5.0,
            )
            if resp.status_code == 200:
                runs = resp.json().get("runs") or []
                if runs:
                    run_id = runs[-1]
        except Exception:
            run_id = ""

        if run_id:
            page.goto(f"{BASE}/ui?project=acme-data&dataset=demo&run_id={run_id}&token={TOKEN}")
            time.sleep(1)
            page.screenshot(path=str(OUT / "ui_run.png"), full_page=True)

            page.goto(f"{BASE}/ui/metrics?token={TOKEN}")
            time.sleep(1)
            page.screenshot(path=str(OUT / "ui_metrics.png"), full_page=True)

        browser.close()


if __name__ == "__main__":
    main()
