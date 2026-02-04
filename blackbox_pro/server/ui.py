from __future__ import annotations

import html
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from blackbox_pro.server.api import get_store
from blackbox.seal import verify_chain_with_payloads
from blackbox.util import utc_now_iso
from blackbox_pro.server.auth import require_role, require_project_access

router = APIRouter()


# -----------------------------
# helpers
# -----------------------------
def _h(x: Any) -> str:
    return html.escape("" if x is None else str(x))


def _json_pre(obj: Any) -> str:
    s = json.dumps(obj, indent=2, ensure_ascii=False)
    return f"<pre class='code'>{_h(s)}</pre>"


def _get_json_or_none(store, key: str) -> Optional[dict]:
    try:
        return store.get_json(key)
    except FileNotFoundError:
        return None


def _fmt_dt(s: Optional[str]) -> str:
    if not s:
        return ""
    # keep ISO but shorten visual noise
    return s.replace("T", " ").replace("Z", " UTC")


def _badge(text: str, kind: str) -> str:
    # kind: ok|warn|bad|muted|info
    return f"<span class='badge badge-{kind}'>{_h(text)}</span>"


def _kv(k: str, v: Any) -> str:
    return f"<div class='kv'><div class='k'>{_h(k)}</div><div class='v'>{_h(v)}</div></div>"


def _segment_keys(keys: List[str], prefix: str) -> List[str]:
    """
    Return the next segment after prefix (like directories).
    Example: keys contain demo/simple/run_x/chain.json, prefix=demo/
    returns ["simple", ...]
    """
    out: set[str] = set()
    p = prefix.strip("/")
    pfx = p + "/" if p else ""
    for k in keys:
        k = str(k).lstrip("/")
        if not k.startswith(pfx):
            continue
        rest = k[len(pfx) :]
        if not rest:
            continue
        seg = rest.split("/", 1)[0]
        if seg:
            out.add(seg)
    return sorted(out)


def _list_keys_safe(store, prefix: str) -> List[str]:
    try:
        return list(store.list(prefix.rstrip("/")))
    except Exception:
        return list(store.list(prefix.rstrip("/") + "/"))


def _load_verbose_steps(store, prefix: str, run_obj: dict) -> List[Dict[str, Any]]:
    steps = run_obj.get("steps", []) or []
    out: List[Dict[str, Any]] = []
    for s in steps:
        path = s.get("path")
        if not path:
            continue
        step_obj = _get_json_or_none(store, f"{prefix}/{path}")
        if not step_obj:
            continue
        out.append(step_obj)
    return out


def _step_summary(step_obj: dict) -> Tuple[dict, dict, dict]:
    inp = step_obj.get("input") or {}
    outp = step_obj.get("output") or {}
    schema_diff = step_obj.get("schema_diff") or {}
    diff = step_obj.get("diff") or {}
    summary = (diff.get("summary") or {}) if isinstance(diff, dict) else {}
    hint = diff.get("ui_hint") if isinstance(diff, dict) else None
    return inp, outp, {"schema_diff": schema_diff, "diff_summary": summary, "ui_hint": hint}


# -----------------------------
# styling (pure HTML/CSS)
# -----------------------------
def _page(title: str, body: str) -> HTMLResponse:
    css = """
    :root{
      --bg: #05060a;
      --panel: #0b0f1a;
      --panel2: #0f1422;
      --text: #f8fafc;
      --muted: #b7c1d6;
      --line: rgba(248,250,252,0.14);
      --ok: #22c55e;
      --warn: #f59e0b;
      --bad: #ef4444;
      --info: #38bdf8;
      --chip: rgba(248,250,252,0.08);
      --shadow: 0 16px 32px rgba(0,0,0,0.45);
      --mono: "JetBrains Mono", "SFMono-Regular", Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      --sans: "IBM Plex Sans", "Avenir Next", "Helvetica Neue", "Segoe UI", sans-serif;
      --accent: #38bdf8;
      --accent2: #22c55e;
    }
    @media (prefers-color-scheme: light){
      :root{
        --bg:#f5f7fb;
        --panel:#ffffff;
        --panel2:#f1f4fb;
        --text:#0c1424;
        --muted:#5b6b85;
        --line: rgba(12,20,36,0.12);
        --chip: rgba(12,20,36,0.06);
        --shadow: 0 18px 40px rgba(12,20,36,0.10);
        --accent:#3758ff;
        --accent2:#10b981;
      }
    }
    *{ box-sizing:border-box; }
    body{
      margin:0;
      font-family: var(--sans);
      background:
        radial-gradient(1200px 700px at 15% -10%, rgba(56,189,248,0.20), transparent 40%),
        radial-gradient(900px 700px at 85% -5%, rgba(34,197,94,0.18), transparent 45%),
        linear-gradient(180deg, rgba(2,6,23,0.65), rgba(2,6,23,0.00)),
        var(--bg);
      color: var(--text);
      letter-spacing: 0.2px;
    }
    a{ color: inherit; text-decoration:none; }
    a:hover{ text-decoration: underline; }
    .topbar{
      position: sticky;
      top:0;
      z-index: 100;
      backdrop-filter: blur(16px);
      background: rgba(5,6,10,0.85);
      border-bottom: 1px solid var(--line);
    }
    @media (prefers-color-scheme: light){
      .topbar{ background: rgba(255,255,255,0.88); }
    }
    .wrap{ max-width: 1200px; margin: 0 auto; padding: 18px; }
    .brand{
      display:flex; align-items:center; justify-content:space-between; gap:12px;
    }
    .brand h1{
      margin:0;
      font-size: 15px;
      letter-spacing: 0.6px;
      font-weight: 800;
      text-transform: uppercase;
    }
    .brand small{ color: var(--muted); font-weight:600; }
    .logo-dot{
      width:10px;height:10px;border-radius:999px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      display:inline-block; margin-right:10px;
      box-shadow: 0 0 0 4px rgba(122,162,255,0.15);
    }
    .actions{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .btn{
      display:inline-flex; align-items:center; gap:8px;
      padding:9px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(248,250,252,0.08);
      box-shadow: 0 8px 18px rgba(0,0,0,0.35);
      font-weight: 700;
      font-size: 12.5px;
      text-transform: uppercase;
      letter-spacing: 0.4px;
    }
    .btn:hover{ background: rgba(248,250,252,0.18); text-decoration:none; }
    .content{ padding: 18px; }
    .grid{
      display:grid; grid-template-columns: 1.3fr 0.7fr; gap: 14px;
    }
    @media (max-width: 980px){
      .grid{ grid-template-columns: 1fr; }
    }
    .card{
      background: rgba(11,15,26,0.92);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      box-shadow: var(--shadow);
    }
    @media (prefers-color-scheme: light){
      .card{ background: rgba(255,255,255,0.92); }
    }
    .card h2{ margin: 0 0 10px 0; font-size: 14px; letter-spacing: 0.2px; }
    .muted{ color: var(--muted); }
    .chips{ display:flex; gap:8px; flex-wrap:wrap; margin-top:10px; }
    .chip{
      background: var(--chip);
      border: 1px solid var(--line);
      padding: 6px 10px;
      border-radius: 999px;
      font-weight: 650;
      font-size: 12px;
      color: var(--muted);
    }
    .badge{
      font-weight: 750;
      font-size: 12px;
      padding: 5px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(248,250,252,0.12);
      display:inline-flex; align-items:center; gap:6px;
    }
    .badge-ok{ color: var(--ok); }
    .badge-warn{ color: var(--warn); }
    .badge-bad{ color: var(--bad); }
    .badge-info{ color: var(--info); }
    .badge-muted{ color: var(--muted); }
    .kv{
      display:flex; justify-content:space-between; gap:12px;
      padding: 8px 0;
      border-bottom: 1px dashed var(--line);
    }
    .kv:last-child{ border-bottom: none; }
    .k{ color: var(--muted); font-weight: 650; font-size: 12px; }
    .v{ font-weight: 700; font-size: 12.5px; text-align:right; overflow-wrap:anywhere; }
    .steps{
      display:flex; flex-direction:column; gap:12px;
    }
    details{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(231,238,252,0.03);
      overflow: hidden;
    }
    details[open]{ background: rgba(231,238,252,0.045); }
    summary{
      list-style:none;
      cursor:pointer;
      padding: 12px 12px;
      display:flex; align-items:center; justify-content:space-between; gap:10px;
      user-select:none;
    }
    summary::-webkit-details-marker{ display:none; }
    .step-left{ display:flex; gap:10px; align-items:center; min-width: 0; }
    .num{
      width:28px; height:28px; border-radius:10px;
      display:flex; align-items:center; justify-content:center;
      background: rgba(56,189,248,0.22);
      border: 1px solid var(--line);
      font-weight: 900;
      font-size: 12px;
      flex: 0 0 auto;
    }
    .step-title{ font-weight: 850; font-size: 13px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .step-meta{ display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
    .step-body{ padding: 0 12px 12px 12px; }
    .cols{
      display:grid;
      grid-template-columns: 1fr 1fr;
      gap:12px;
    }
    @media (max-width: 820px){
      .cols{ grid-template-columns: 1fr; }
    }
    .code{
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.45;
      padding: 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(2,6,23,0.78);
      overflow:auto;
      max-height: 520px;
    }
    @media (prefers-color-scheme: light){
      .code{ background: rgba(15,23,42,0.03); }
    }
    .footer{
      padding: 18px 0 30px;
      color: var(--muted);
      font-size: 12px;
      text-align:center;
    }
    .formrow{
      display:flex; gap:10px; flex-wrap:wrap; align-items:end;
    }
    select, input{
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(231,238,252,0.04);
      color: var(--text);
      padding: 10px 12px;
      font-weight: 650;
      min-width: 220px;
    }
    @media (prefers-color-scheme: light){
      select, input{ background: rgba(16,24,40,0.03); }
    }
    label{ font-size: 12px; font-weight: 750; color: var(--muted); display:block; margin-bottom:6px; }
    .field{ display:flex; flex-direction:column; }
    """
    html_doc = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>{_h(title)}</title>
    <style>{css}</style>
  </head>
  <body>
    {body}
    <script>
      (function(){
        const TOKEN_KEY = "bb_token";
        function getToken(){
          return (localStorage.getItem(TOKEN_KEY) || "").trim();
        }
        function setToken(v){
          localStorage.setItem(TOKEN_KEY, (v || "").trim());
        }
        function applyToken(){
          const token = getToken();
          document.querySelectorAll("[data-token-link]").forEach((el) => {
            try{
              const url = new URL(el.getAttribute("href"), window.location.origin);
              if(token){
                url.searchParams.set("token", token);
              }else{
                url.searchParams.delete("token");
              }
              el.setAttribute("href", url.pathname + url.search);
            }catch(_e){}
          });
          document.querySelectorAll("form[data-token-form]").forEach((form) => {
            let input = form.querySelector("input[name='token']");
            if(!input){
              input = document.createElement("input");
              input.type = "hidden";
              input.name = "token";
              form.appendChild(input);
            }
            input.value = token;
          });
          const status = document.getElementById("auth-status");
          if(status){
            status.textContent = token ? "auth: token set" : "auth: no token";
            status.className = token ? "badge badge-ok" : "badge badge-muted";
          }
        }
        document.addEventListener("DOMContentLoaded", () => {
          document.querySelectorAll("[data-token-input]").forEach((el) => {
            el.value = getToken();
            el.addEventListener("input", (e) => setToken(e.target.value));
          });
          document.querySelectorAll("[data-token-apply]").forEach((el) => {
            el.addEventListener("click", (e) => {
              e.preventDefault();
              applyToken();
            });
          });
          applyToken();
        });
      })();
    </script>
  </body>
</html>
"""
    return HTMLResponse(html_doc)


# -----------------------------
# UI: home (pick run)
# -----------------------------
@router.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/ui/home", status_code=302)


@router.get("/ui/home", response_class=HTMLResponse, include_in_schema=False)
def ui_home(
    project: Optional[str] = None,
    dataset: Optional[str] = None,
    run_id: Optional[str] = None,
) -> HTMLResponse:
    store = get_store()

    # Discover projects/datasets/runs by listing keys.
    keys = _list_keys_safe(store, "")
    projects = _segment_keys(keys, "")
    sel_project = project or (projects[0] if projects else "")

    ds_keys = _list_keys_safe(store, sel_project) if sel_project else []
    datasets = _segment_keys(ds_keys, sel_project)
    sel_dataset = dataset or (datasets[0] if datasets else "")

    runs: List[str] = []
    if sel_project and sel_dataset:
        base = f"{sel_project}/{sel_dataset}"
        base_keys = _list_keys_safe(store, base)
        runs = _segment_keys(base_keys, base)

    sel_run = run_id or (runs[-1] if runs else "")

    body = f"""
    <div class="topbar">
      <div class="wrap brand">
        <div>
          <h1><span class="logo-dot"></span>Blackbox Data Pro <small>local</small></h1>
          <div class="muted" style="font-size:12px;margin-top:2px;">Forensic audit trail & diff intelligence for Pandas pipelines</div>
        </div>
        <div class="actions">
          <span id="auth-status" class="badge badge-muted">auth: no token</span>
          <a class="btn" data-token-link href="/docs">Docs</a>
          <a class="btn" data-token-link href="/openapi.json">OpenAPI</a>
          <a class="btn" data-token-link href="/ui/docs">Report Guide</a>
        </div>
      </div>
    </div>

    <div class="wrap content">
      <div class="card">
        <h2>Open a run</h2>

        <form method="get" action="/ui" data-token-form>
          <div class="formrow">
            <div class="field">
              <label>Project</label>
              <select name="project">
                {''.join([f"<option value='{_h(p)}' {'selected' if p==sel_project else ''}>{_h(p)}</option>" for p in projects])}
              </select>
            </div>

            <div class="field">
              <label>Dataset</label>
              <select name="dataset">
                {''.join([f"<option value='{_h(d)}' {'selected' if d==sel_dataset else ''}>{_h(d)}</option>" for d in datasets])}
              </select>
            </div>

            <div class="field">
              <label>Run</label>
              <select name="run_id">
                {''.join([f"<option value='{_h(r)}' {'selected' if r==sel_run else ''}>{_h(r)}</option>" for r in runs])}
              </select>
            </div>

            <div class="field" style="min-width:140px;">
              <label>Mode</label>
              <select name="view">
                <option value="summary" selected>Summary</option>
                <option value="verbose">Verbose</option>
              </select>
            </div>

            <div class="field" style="min-width:140px;">
              <button class="btn" type="submit" style="height:42px;cursor:pointer;">Open</button>
            </div>
          </div>
        </form>

        <div class="formrow" style="margin-top:12px;">
          <div class="field" style="flex:1;">
            <label>UI Token</label>
            <input data-token-input placeholder="paste token here" />
          </div>
          <div class="field" style="min-width:160px;">
            <button class="btn" data-token-apply type="button" style="height:42px;cursor:pointer;">Apply Token</button>
          </div>
        </div>

        <div class="chips">
          <span class="chip">Tip: bookmark /ui/home</span>
          <span class="chip">Next: auth + export</span>
        </div>
      </div>

      <div class="footer">Blackbox Data Pro – UI home</div>
    </div>
    """
    return _page("Blackbox Data Pro – Home", body)


# -----------------------------
# UI: run view (pro-looking)
# -----------------------------
@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui(
    project: str = Query(...),
    dataset: str = Query(...),
    run_id: str = Query(...),
    view: str = Query("summary", pattern="^(summary|verbose)$"),
) -> HTMLResponse:
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"

    run_obj = _get_json_or_none(store, f"{prefix}/run.json")
    chain_obj = _get_json_or_none(store, f"{prefix}/chain.json")

    if not run_obj or not chain_obj:
        body = f"""
        <div class="topbar">
          <div class="wrap brand">
            <div><h1><span class="logo-dot"></span>Blackbox Data Pro <small>local</small></h1></div>
            <div class="actions">
              <a class="btn" href="/ui/home">Home</a>
              <a class="btn" href="/docs">Docs</a>
            </div>
          </div>
        </div>
        <div class="wrap content">
          <div class="card">
            <h2>Run not found</h2>
            <div class="muted">Prefix: <span style="font-family:var(--mono);">{_h(prefix)}</span></div>
          </div>
        </div>
        """
        return _page("Blackbox Data Pro – Not found", body)

    ok, verify_msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    verify_ok = bool(ok)

    verbose_steps = _load_verbose_steps(store, prefix, run_obj)
    status = run_obj.get("status", "unknown")

    title = f"Blackbox Data Pro – {project}/{dataset}/{run_id}"

    status_badge = _badge(status, "ok" if status == "ok" else "warn")
    verify_badge = _badge("verified" if verify_ok else "broken", "ok" if verify_ok else "bad")

    # Left: steps
    step_cards: List[str] = []
    for i, st in enumerate(verbose_steps, start=1):
        inp, outp, meta = _step_summary(st)
        schema_diff = meta["schema_diff"] or {}
        diff_summary = meta["diff_summary"] or {}
        hint = meta.get("ui_hint")

        added = diff_summary.get("added", 0)
        removed = diff_summary.get("removed", 0)
        changed = diff_summary.get("changed", 0)

        schema_added = len(schema_diff.get("added_cols") or [])
        schema_removed = len(schema_diff.get("removed_cols") or [])
        dtype_changed = len(schema_diff.get("dtype_changed") or [])

        # Badge logic for “attention”
        attention = (added or removed or changed or schema_added or schema_removed or dtype_changed) > 0
        attention_badge = _badge("attention" if attention else "clean", "warn" if attention else "ok")

        # Details content
        if view == "verbose":
            body_html = _json_pre(st)
        else:
            diff_obj = st.get("diff") or {}

            def _extract_list(obj: Any) -> tuple[list[str], bool]:
                if isinstance(obj, dict):
                    if "items" in obj and isinstance(obj["items"], list):
                        return [str(x) for x in obj["items"]], bool(obj.get("truncated"))
                    if "head" in obj and isinstance(obj["head"], list):
                        return [str(x) for x in obj["head"]], True
                if isinstance(obj, list):
                    return [str(x) for x in obj], False
                return [], False

            def _render_keys(title: str, keys: list[str], truncated: bool, kind: str) -> str:
                max_show = 10
                head = keys[:max_show]
                items = "".join([f"<li>{_h(k)}</li>" for k in head]) if head else "<li class='muted'>none</li>"
                trunc_badge = _badge("truncated", "warn") if truncated or len(keys) > max_show else ""
                download = f"/ui/diff_keys?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}&ordinal={_h(st.get('ordinal', i))}&kind={_h(kind)}"
                return f"""
                <div class='card' style='margin-top:10px;'>
                  <div style='display:flex;justify-content:space-between;align-items:center;'>
                    <strong>{_h(title)}</strong>
                    <div class='chips'>
                      {trunc_badge}
                      <a class='btn' data-token-link href='{download}'>Download full</a>
                    </div>
                  </div>
                  <ul style='margin:8px 0 0 16px;line-height:1.6;'>{items}</ul>
                </div>
                """

            added_keys, added_trunc = _extract_list(diff_obj.get("added_keys"))
            removed_keys, removed_trunc = _extract_list(diff_obj.get("removed_keys"))
            changed_keys, changed_trunc = _extract_list(diff_obj.get("changed_keys"))
            summary_only = bool(diff_obj.get("summary_only"))
            summary_note = "<div class='muted'>Diff summarized (high churn) — keys may be empty.</div>" if summary_only else ""

            body_html = f"""
            {summary_note}
            {_render_keys('Added keys', added_keys, added_trunc, 'added')}
            {_render_keys('Removed keys', removed_keys, removed_trunc, 'removed')}
            {_render_keys('Changed keys', changed_keys, changed_trunc, 'changed')}
            """

        step_cards.append(
            f"""
            <details>
              <summary>
                <div class="step-left">
                  <div class="num">{_h(st.get("ordinal", i))}</div>
                  <div style="min-width:0;">
                    <div class="step-title">{_h(st.get("name", f"step_{i}"))}</div>
                    <div class="muted" style="font-size:12px;margin-top:2px;">
                      rows: {_h(inp.get("n_rows"))} → {_h(outp.get("n_rows"))} &nbsp; | &nbsp;
                      cols: {_h(inp.get("n_cols"))} → {_h(outp.get("n_cols"))}
                    </div>
                  </div>
                </div>
                <div class="step-meta">
                  {_badge(st.get("status",""), "ok" if st.get("status")=="ok" else "warn")}
                  {attention_badge}
                  {(_badge(hint, "info") if hint else "")}
                  <span class="badge badge-muted">schema +{_h(schema_added)} −{_h(schema_removed)} Δdtype:{_h(dtype_changed)}</span>
                  <span class="badge badge-muted">rows +{_h(added)} −{_h(removed)} Δ{_h(changed)}</span>
                </div>
              </summary>
              <div class="step-body">
                {body_html}
              </div>
            </details>
            """
        )

    steps_html = f"""
    <div class="card">
      <h2>Pipeline timeline</h2>
      <div class="steps">
        {''.join(step_cards) if step_cards else "<div class='muted'>No step JSON found.</div>"}
      </div>
    </div>
    """

    # Right: run summary
    run_meta = run_obj.get("runtime") or {}
    host = run_obj.get("host") or {}
    tags = run_obj.get("tags") or {}
    step_count = len(run_obj.get("steps") or [])

    total_added = 0
    total_removed = 0
    total_changed = 0
    summary_hints: list[str] = []
    for st in verbose_steps:
        diff = st.get("diff") or {}
        summ = diff.get("summary") or {}
        try:
            total_added += int(summ.get("added") or 0)
            total_removed += int(summ.get("removed") or 0)
            total_changed += int(summ.get("changed") or 0)
        except Exception:
            pass
        hint = diff.get("ui_hint")
        if hint:
            summary_hints.append(str(hint))

    hint_set = sorted(set(summary_hints))
    hint_badges = "".join([_badge(h, "info") for h in hint_set]) if hint_set else _badge("none", "muted")

    report_summary = (
        f"Run completed with {step_count} steps. "
        f"Row-level changes: +{total_added} / -{total_removed} / Δ{total_changed}. "
        f"Schema changes are visible in the step badges. "
        f"Review hints for summarized or skipped diffs."
    )

    summary_html = f"""
    <div class="card">
      <h2>Run summary</h2>
      <div class="chips">
        {status_badge}
        {verify_badge}
        <span class="badge badge-muted">steps: {_h(len(run_obj.get("steps") or []))}</span>
      </div>
      <div class="formrow" style="margin-top:10px;">
        <div class="field" style="flex:1;">
          <label>UI Token</label>
          <input data-token-input placeholder="paste token here" />
        </div>
        <div class="field" style="min-width:160px;">
          <button class="btn" data-token-apply type="button" style="height:42px;cursor:pointer;">Apply Token</button>
        </div>
      </div>
      <div style="margin-top:10px;">
        {_kv("Project", project)}
        {_kv("Dataset", dataset)}
        {_kv("Run ID", run_id)}
        {_kv("Created", _fmt_dt(run_obj.get("created_at")))}
        {_kv("Finished", _fmt_dt(run_obj.get("finished_at")))}
        {_kv("Host", host.get("hostname"))}
        {_kv("Arch", host.get("arch"))}
        {_kv("Python", run_meta.get("python"))}
        {_kv("Platform", run_meta.get("platform"))}
      </div>
    </div>

    <div class="card" style="margin-top:14px;">
      <h2>Integrity</h2>
      <div style="margin-top:10px;">
        {_kv("Verify", verify_msg)}
        {_kv("Chain entries", len(chain_obj.get("entries", [])))}
        {_kv("Chain head", chain_obj.get("head"))}
      </div>
      <div class="chips">
        <a class="btn" data-token-link href="/verify?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}">API: /verify</a>
        <a class="btn" data-token-link href="/report?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}">API: /report</a>
        <a class="btn" data-token-link href="/report_verbose?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}&show_keys=head&max_keys=10">API: /report_verbose</a>
      </div>
    </div>

    <div class="card" style="margin-top:14px;">
      <h2>Report Summary</h2>
      <div class="muted" style="font-size:12.5px;line-height:1.6;">
        {report_summary}
      </div>
      <div class="chips" style="margin-top:12px;">
        {hint_badges}
      </div>
    </div>

    <div class="card" style="margin-top:14px;">
      <h2>How to Read This</h2>
      <div class="muted" style="font-size:12px;line-height:1.6;">
        <strong>Schema:</strong> column additions/removals and dtype changes. <br/>
        <strong>Rows:</strong> added/removed/changed primary keys. <br/>
        <strong>Hints:</strong> badges explain why a diff is summarized or skipped.
      </div>
      <div class="chips" style="margin-top:12px;">
        <span class="badge badge-info">summary_only_high_churn</span>
        <span class="badge badge-info">diff_skipped_fingerprint_match</span>
      </div>
    </div>

    <div class="card" style="margin-top:14px;">
      <h2>Tags</h2>
      {_json_pre(tags)}
    </div>

    <div class="card" style="margin-top:14px;">
      <h2>Exports</h2>
      <div class="chips">
        <a class="btn" data-token-link href="/ui/export_json?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}">Export JSON</a>
        <a class="btn" data-token-link href="/ui/export_html?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}">Export HTML</a>
        <a class="btn" data-token-link href="/ui/export_evidence?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}">Evidence ZIP</a>
        <a class="btn" data-token-link href="/ui/export_evidence_json?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}">Evidence JSON</a>
      </div>
    </div>
    """

    body = f"""
    <div class="topbar">
      <div class="wrap brand">
        <div>
          <h1><span class="logo-dot"></span>Blackbox Data Pro <small>local</small></h1>
          <div class="muted" style="font-size:12px;margin-top:2px;">
            {_h(project)}/{_h(dataset)} · {_h(run_id)}
          </div>
        </div>
        <div class="actions">
          <span id="auth-status" class="badge badge-muted">auth: no token</span>
          <a class="btn" data-token-link href="/ui/home">Home</a>
          <a class="btn" data-token-link href="/docs">Docs</a>
          <a class="btn" data-token-link href="/openapi.json">OpenAPI</a>
          <a class="btn" data-token-link href="/ui/docs">Report Guide</a>
          <a class="btn" data-token-link href="/ui?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}&view=summary">Summary</a>
          <a class="btn" data-token-link href="/ui?project={_h(project)}&dataset={_h(dataset)}&run_id={_h(run_id)}&view=verbose">Verbose</a>
        </div>
      </div>
    </div>

    <div class="wrap content">
      <div class="grid">
        <div>
          {steps_html}
        </div>
        <div>
          {summary_html}
        </div>
      </div>

      <div class="card" style="margin-top:14px;">
        <h2>Raw run.json</h2>
        {_json_pre(run_obj)}
      </div>

      <div class="footer">Blackbox Data Pro – run viewer</div>
    </div>
    """
    return _page(title, body)


@router.get("/ui/diff_keys", include_in_schema=False)
def ui_diff_keys(
    request,
    project: str,
    dataset: str,
    run_id: str,
    ordinal: int,
    kind: str = "added",
    fmt: str = "json",
) -> Response:
    require_project_access(request, project)
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"
    run_obj = _get_json_or_none(store, f"{prefix}/run.json")
    if not run_obj:
        return Response(content="run not found", media_type="text/plain", status_code=404)
    steps = run_obj.get("steps") or []
    step_path = None
    for s in steps:
        if int(s.get("ordinal", -1)) == int(ordinal):
            step_path = s.get("path")
            break
    if not step_path:
        return Response(content="step not found", media_type="text/plain", status_code=404)
    step_obj = _get_json_or_none(store, f"{prefix}/{step_path}")
    if not step_obj:
        return Response(content="step not found", media_type="text/plain", status_code=404)
    diff = step_obj.get("diff") or {}
    key_map = {"added": "added_keys", "removed": "removed_keys", "changed": "changed_keys"}
    key_name = key_map.get(kind, "added_keys")
    keys = diff.get(key_name) or []
    if isinstance(keys, dict):
        keys = keys.get("items") or keys.get("head") or []
    keys = [str(k) for k in keys]
    if fmt == "csv":
        content = "key\n" + "\n".join(keys) + "\n"
        return Response(content=content, media_type="text/csv")
    return Response(content=json.dumps({"keys": keys}, ensure_ascii=False, indent=2), media_type="application/json")


@router.get("/ui/export_json", include_in_schema=False)
def ui_export_json(request, project: str, dataset: str, run_id: str) -> Response:
    require_project_access(request, project)
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"
    run_obj = _get_json_or_none(store, f"{prefix}/run.json")
    chain_obj = _get_json_or_none(store, f"{prefix}/chain.json")
    if not run_obj or not chain_obj:
        return Response(content="run not found", media_type="text/plain", status_code=404)
    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    payload = {"run": run_obj, "chain": chain_obj, "verify": {"ok": ok, "message": msg}}
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=report_{run_id}.json"},
    )


@router.get("/ui/export_html", include_in_schema=False)
def ui_export_html(request, project: str, dataset: str, run_id: str) -> Response:
    require_project_access(request, project)
    page = ui(project=project, dataset=dataset, run_id=run_id, view="summary")
    return Response(
        content=page.body,
        media_type="text/html",
        headers={"Content-Disposition": f"attachment; filename=report_{run_id}.html"},
    )


@router.get("/ui/export_evidence", include_in_schema=False)
def ui_export_evidence(request, project: str, dataset: str, run_id: str) -> Response:
    require_role(request, {"admin"})
    require_project_access(request, project)
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"
    run_obj = _get_json_or_none(store, f"{prefix}/run.json")
    chain_obj = _get_json_or_none(store, f"{prefix}/chain.json")
    if not run_obj or not chain_obj:
        return Response(content="run not found", media_type="text/plain", status_code=404)
    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    verification = {
        "verified_at": utc_now_iso(),
        "ok": bool(ok),
        "message": msg,
        "chain_entries": len(chain_obj.get("entries", [])),
        "chain_head": chain_obj.get("head"),
    }
    run_bytes = json.dumps(run_obj, ensure_ascii=False, indent=2).encode("utf-8")
    chain_bytes = json.dumps(chain_obj, ensure_ascii=False, indent=2).encode("utf-8")
    verification_bytes = json.dumps(verification, ensure_ascii=False, indent=2).encode("utf-8")
    meta_bytes = json.dumps({"project": project, "dataset": dataset, "run_id": run_id}, ensure_ascii=False, indent=2).encode("utf-8")
    manifest = {
        "run.json": hashlib.sha256(run_bytes).hexdigest(),
        "chain.json": hashlib.sha256(chain_bytes).hexdigest(),
        "verification.json": hashlib.sha256(verification_bytes).hexdigest(),
        "meta.json": hashlib.sha256(meta_bytes).hexdigest(),
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    import io as _io
    import zipfile as _zip
    buf = _io.BytesIO()
    with _zip.ZipFile(buf, "w", compression=_zip.ZIP_DEFLATED) as zf:
        zf.writestr("run.json", run_bytes)
        zf.writestr("chain.json", chain_bytes)
        zf.writestr("verification.json", verification_bytes)
        zf.writestr("meta.json", meta_bytes)
        zf.writestr("manifest.json", manifest_bytes)
    buf.seek(0)
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=evidence_{run_id}.zip"},
    )


@router.get("/ui/export_evidence_json", include_in_schema=False)
def ui_export_evidence_json(request, project: str, dataset: str, run_id: str) -> Response:
    require_role(request, {"admin"})
    require_project_access(request, project)
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"
    run_obj = _get_json_or_none(store, f"{prefix}/run.json")
    chain_obj = _get_json_or_none(store, f"{prefix}/chain.json")
    if not run_obj or not chain_obj:
        return Response(content="run not found", media_type="text/plain", status_code=404)
    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    verification = {
        "verified_at": utc_now_iso(),
        "ok": bool(ok),
        "message": msg,
        "chain_entries": len(chain_obj.get("entries", [])),
        "chain_head": chain_obj.get("head"),
    }
    run_bytes = json.dumps(run_obj, ensure_ascii=False, indent=2).encode("utf-8")
    chain_bytes = json.dumps(chain_obj, ensure_ascii=False, indent=2).encode("utf-8")
    verification_bytes = json.dumps(verification, ensure_ascii=False, indent=2).encode("utf-8")
    meta_bytes = json.dumps({"project": project, "dataset": dataset, "run_id": run_id}, ensure_ascii=False, indent=2).encode("utf-8")
    manifest = {
        "run.json": hashlib.sha256(run_bytes).hexdigest(),
        "chain.json": hashlib.sha256(chain_bytes).hexdigest(),
        "verification.json": hashlib.sha256(verification_bytes).hexdigest(),
        "meta.json": hashlib.sha256(meta_bytes).hexdigest(),
    }
    payload = {
        "run": run_obj,
        "chain": chain_obj,
        "verification": verification,
        "meta": {"project": project, "dataset": dataset, "run_id": run_id},
        "manifest": manifest,
    }
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=evidence_{run_id}.json"},
    )


@router.get("/ui/docs", response_class=HTMLResponse, include_in_schema=False)
def ui_docs() -> HTMLResponse:
    body = f"""
    <div class="topbar">
      <div class="wrap brand">
        <div>
          <h1><span class="logo-dot"></span>Blackbox Data Pro <small>docs</small></h1>
          <div class="muted" style="font-size:12px;margin-top:2px;">How to read reports and diffs</div>
        </div>
        <div class="actions">
          <span id="auth-status" class="badge badge-muted">auth: no token</span>
          <a class="btn" data-token-link href="/ui/home">Home</a>
          <a class="btn" data-token-link href="/docs">API Docs</a>
        </div>
      </div>
    </div>
    <div class="wrap content">
      <div class="card">
        <h2>Report Guide</h2>
        <div class="muted" style="font-size:12.5px;line-height:1.7;">
          <strong>Schema diff</strong>: added/removed columns and dtype changes.<br/>
          <strong>Row diff</strong>: added/removed/changed primary keys.<br/>
          <strong>Hints</strong>: badges indicate summarized or skipped diffs.<br/>
        </div>
      </div>
      <div class="card" style="margin-top:14px;">
        <h2>Common Scenarios</h2>
        <div class="muted" style="font-size:12.5px;line-height:1.7;">
          - Schema changed, rows stable → projection or rename step.<br/>
          - Rows changed, schema stable → filter, join, update, or refresh.<br/>
          - High churn → full refresh or large upstream change.
        </div>
      </div>
      <div class="footer">Blackbox Data Pro – docs</div>
    </div>
    """
    return _page("Blackbox Data Pro – Docs", body)
