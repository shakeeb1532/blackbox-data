import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8088"
TOKEN = "dev-secret-token"
OUT = Path("screens")
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        page.goto(f"{BASE}/ui/home?token={TOKEN}")
        page.set_viewport_size({"width": 1280, "height": 720})
        time.sleep(1)
        page.screenshot(path=str(OUT / "ui_home.png"), full_page=True)

        # Update run_id manually or via list_runs in a real run
        # Example:
        # page.goto(f"{BASE}/ui?project=acme-data&dataset=demo&run_id=<RUN_ID>&token={TOKEN}")
        # page.screenshot(path=str(OUT / "ui_run.png"), full_page=True)

        browser.close()


if __name__ == "__main__":
    main()
