# mustamir_cme_extractor.py
# Adds optional S3 uploads and SHARDING (stride across pages).
# Shard args: --shard-count N --shard-index I (0-based). When N>1, each shard processes
# pages: start_page + I, then advances by +N pages each loop.

from playwright.sync_api import sync_playwright
from urllib.parse import urlsplit
import pandas as pd
import os, re, time, argparse, sys
from typing import Optional

# ---------- Optional S3 ----------
_S3 = None
def get_s3():
    global _S3
    if _S3 is None:
        try:
            import boto3
            _S3 = boto3.client("s3")
        except Exception as e:
            raise RuntimeError(f"boto3 not available: {e}")
    return _S3

def s3_upload_file(local_path: str, bucket: str, key: str, retries: int = 5, backoff: float = 1.5):
    s3 = get_s3()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            s3.upload_file(local_path, bucket, key)
            return True
        except Exception as e:
            last_err = e
            time.sleep(backoff ** attempt)
    raise RuntimeError(f"S3 upload failed for s3://{bucket}/{key}: {last_err}")

# ---------- Paths & constants ----------
ROOT_URL = "https://mustamir.scfhs.org.sa/account/external-activities"
OUT_DIR = "out"
ACTIVITY_DIR = os.path.join(OUT_DIR, "activities")
# We'll set MASTER_XLSX dynamically in main (to avoid cross-shard clashes)
MASTER_XLSX = None  # set later

# ---- Selectors ----
LIST_COMPONENT = "app-list-external-activities"
TBODY_SELECTOR = "div.primeng-datatable-container table tbody, div.p-datatable table tbody"
ROW_SELECTOR = f"{TBODY_SELECTOR} tr"
SPINNER_SELECTOR = "td.emptyTable .p-progress-spinner"

PAGINATOR_ROOT = ".p-paginator"
PAGINATOR_PAGES = ".p-paginator-pages"
PAGINATOR_PAGE_BTN = f"{PAGINATOR_PAGES} .p-paginator-page.p-paginator-element.p-link"
ACTIVE_PAGE_BTN = f"{PAGINATOR_PAGES} .p-paginator-page.p-highlight"
NEXT_BTN = f"{PAGINATOR_ROOT} .p-paginator-next.p-paginator-element"

ENGLISH_SWITCH = "a.p-2.text-white.hover1:has-text('English')"

VIEW_CLICKS = [
    "td:last-of-type .action.mx-2",
    "td .action.mx-2",
    'td:last-of-type svg[viewBox="0 0 511.626 511.626"]',
    'svg[viewBox="0 0 511.626 511.626"]',
]

H4_ACTIVITY = "h4:has-text('Activity details'), h4:has-text('Activity Details')"
DETAILS_BLOCK_UNDER_H4 = f"{H4_ACTIVITY} + div"
FORM_GROUPS = f"{DETAILS_BLOCK_UNDER_H4} .form-group"
LABEL_IN_GROUP = "label"
P_IN_GROUP = "p"

H5_SELECTOR = "h5"
H5_NEXT_DIV_XPATH = "xpath=following-sibling::div[1]"
SCIPRO_COMPONENT = "external-activity-agenda-list"

ACCRED_LABEL = "label:has-text('Accredited CME Hours')"
ACCRED_VALUE = f"{ACCRED_LABEL} + p"


# ---------------- Utilities ----------------
def log(msg):
    print(msg, flush=True)

def ensure_out():
    os.makedirs(ACTIVITY_DIR, exist_ok=True)

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def text_or_empty(loc):
    try:
        if loc.count():
            return loc.inner_text().strip()
    except:
        pass
    return ""


# --------------- List helpers ---------------
def get_list_container(page, timeout_ms: int = 120000):
    deadline = time.time() + (timeout_ms / 1000.0)
    last_err = None
    while time.time() < deadline:
        try:
            page.wait_for_selector(LIST_COMPONENT, state="visible", timeout=5000)
            cont = page.locator(LIST_COMPONENT).first
            if cont and cont.count() > 0 and cont.is_visible():
                time.sleep(0.2)
                if cont.locator(TBODY_SELECTOR).count() > 0 or cont.locator(SPINNER_SELECTOR).count() > 0:
                    return cont
        except Exception as e:
            last_err = e
        time.sleep(0.3)
    raise RuntimeError(f"Could not find <app-list-external-activities> within {timeout_ms} ms"
                       + (f" (last error: {last_err})" if last_err else ""))

def tbody_html(container):
    try:
        return container.locator(TBODY_SELECTOR).first.inner_html() or ""
    except:
        return ""

def wait_spinner_gone(container, timeout_s=30):
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            sp = container.locator(SPINNER_SELECTOR)
            if sp.count() == 0 or not sp.first.is_visible():
                return
        except:
            return
        time.sleep(0.15)

def wait_rows_ready(container):
    wait_spinner_gone(container, timeout_s=30)
    try:
        container.wait_for_selector(ROW_SELECTOR, timeout=5000)
    except:
        pass

def active_page_number(container) -> Optional[int]:
    try:
        btn = container.locator(ACTIVE_PAGE_BTN).first
        if not btn.count(): return None
        txt = btn.inner_text().strip()
        return int(txt) if txt.isdigit() else None
    except:
        return None

def wait_tbody_swap(container, prev_html, timeout_s=10):
    start = time.time()
    while time.time() - start < timeout_s:
        cur = tbody_html(container)
        if cur and cur != prev_html:
            return True
        time.sleep(0.1)
    return False

def click_next(container, retries=3):
    for _ in range(retries):
        prev = tbody_html(container)
        btn = container.locator(NEXT_BTN).first
        if btn.count() and btn.is_enabled():
            btn.click()
            wait_rows_ready(container)
            if wait_tbody_swap(container, prev, 10):
                return True
        time.sleep(0.25)
    return False

def click_next_k(container, k: int) -> bool:
    """Advance k pages via the 'next' button. Returns True if all k succeeded."""
    for _ in range(k):
        if not click_next(container):
            return False
    return True

def fast_forward_to_page(container, target_page, hard_cap_steps=4000):
    cur = active_page_number(container)
    if cur is None:
        wait_rows_ready(container)
        cur = active_page_number(container)
    steps = 0
    while cur and cur < target_page and steps < hard_cap_steps:
        if not click_next(container):
            break
        cur = active_page_number(container) or (cur + 1)
        steps += 1
    if cur != target_page:
        log(f"[warn] Fast-forward ended on page {cur}, expected {target_page}")


# ------------- Row helpers -------------
def find_row_eye(row):
    for sel in VIEW_CLICKS:
        loc = row.locator(sel).first
        try:
            if loc.count() and loc.is_visible() and loc.is_enabled():
                return loc
        except:
            pass
    return None

def robust_switch_to_english(page):
    attempts = 0
    while True:
        attempts += 1
        try:
            link = page.locator(ENGLISH_SWITCH).first
            if link.count() and link.is_visible():
                log(f"[info] Switching to English (attempt {attempts})…")
                link.click()
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_selector(LIST_COMPONENT, timeout=60000)
                log("[info] English loaded.")
                return
            else:
                # If not visible, we might already be in English. Verify list exists.
                if page.locator(LIST_COMPONENT).count():
                    log("[info] English toggle not shown; assuming already English.")
                    return
        except Exception as e:
            log(f"[warn] English switch attempt {attempts} failed: {e}")
        time.sleep(min(2 * attempts, 10))


# ---------- Detail page helpers ----------
def wait_detail_ready(page):
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if page.locator(".p-progress-spinner").count() == 0:
                break
        except:
            break
        time.sleep(0.15)
    page.wait_for_selector(H4_ACTIVITY, timeout=30000)
    try:
        page.wait_for_selector(H5_SELECTOR, timeout=30000)
    except:
        pass

def extract_activity_id_from_url(url: str) -> str:
    path = urlsplit(url).path.strip("/")
    last = path.split("/")[-1] if path else ""
    m = re.search(r"(\d+)$", last)
    return m.group(1) if m else last or ""

def extract_detail(page) -> dict:
    wait_detail_ready(page)
    data = {}
    url = page.url
    data["URL"] = url
    data["Activity ID"] = extract_activity_id_from_url(url)

    groups = page.locator(FORM_GROUPS)
    for i in range(groups.count()):
        g = groups.nth(i)
        label = clean_spaces(text_or_empty(g.locator(LABEL_IN_GROUP).first))
        if not label:
            continue
        vals = [clean_spaces(text_or_empty(p)) for p in g.locator(P_IN_GROUP).all()]
        vals = [v for v in vals if v]
        if vals:
            data[label] = " | ".join(vals)

    h5s = page.locator(H5_SELECTOR)
    for i in range(h5s.count()):
        h5 = h5s.nth(i)
        title = clean_spaces(text_or_empty(h5))
        if not title:
            continue
        if title.strip().lower() == "scientific program":
            try:
                next_div = h5.locator(H5_NEXT_DIV_XPATH).first
                next_div.locator(SCIPRO_COMPONENT).wait_for(state="attached", timeout=20000)
            except Exception:
                pass
        next_div = h5.locator(H5_NEXT_DIV_XPATH).first
        section_text = clean_spaces(text_or_empty(next_div))
        if section_text:
            data[title] = section_text

    accred = page.locator(ACCRED_VALUE).first
    val = clean_spaces(text_or_empty(accred))
    if val:
        data["Accredited CME Hours"] = val
    return data


def save_row_to_excels(row_dict: dict):
    ensure_out()
    act_id = row_dict.get("Activity ID", "unknown")
    per_path = os.path.join(ACTIVITY_DIR, f"detail_{act_id}.xlsx")
    pd.DataFrame([row_dict]).to_excel(per_path, index=False)

    # Use shard-specific master (MASTER_XLSX is set in main)
    if os.path.exists(MASTER_XLSX):
        master = pd.read_excel(MASTER_XLSX)
        all_cols = list(dict.fromkeys(list(master.columns) + list(row_dict.keys())))
        master = master.reindex(columns=all_cols)
        new_row = pd.DataFrame([row_dict]).reindex(columns=all_cols)
        master = pd.concat([master, new_row], ignore_index=True)
    else:
        master = pd.DataFrame([row_dict])
    master.to_excel(MASTER_XLSX, index=False)
    return per_path  # so we can upload it immediately


def recover_list(page, expected_page_no=None, list_timeout_ms: int = 120000):
    try:
        page.wait_for_load_state("networkidle", timeout=list_timeout_ms)
    except:
        pass
    cont = get_list_container(page, timeout_ms=list_timeout_ms)
    wait_rows_ready(cont)
    if expected_page_no:
        try:
            cur = active_page_number(cont)
            if cur != expected_page_no:
                fast_forward_to_page(cont, expected_page_no)
        except:
            pass
    return cont, active_page_number(cont)


# ------------- Main -------------
def main(max_pages:int, headless:bool, start_page:int, list_timeout_ms:int,
         s3_bucket: Optional[str], s3_prefix: str, s3_master_every: int,
         shard_count:int, shard_index:int):

    # ---- validate shards ----
    if shard_count < 1:
        raise ValueError("--shard-count must be >= 1")
    if not (0 <= shard_index < shard_count):
        raise ValueError("--shard-index must be in [0, shard-count-1]")

    # shard-aware paths & prefixes
    global MASTER_XLSX
    shard_suffix = "" if shard_count == 1 else f"_shard{shard_index+1}of{shard_count}"
    MASTER_XLSX = os.path.join(OUT_DIR, f"external_activities_master{shard_suffix}.xlsx")
    ensure_out()

    if s3_bucket:
        # nest each shard under its own directory to avoid collisions
        s3_prefix = f"{s3_prefix.rstrip('/')}/shard_{shard_index+1}of{shard_count}"

    uploaded_rows = 0

    def maybe_upload_activity(filepath: str):
        if not s3_bucket:
            return
        rel = os.path.relpath(filepath, start=OUT_DIR).replace("\\", "/")
        key = f"{s3_prefix.rstrip('/')}/{rel}"
        log(f"[s3] upload {rel} -> s3://{s3_bucket}/{key}")
        s3_upload_file(filepath, s3_bucket, key)

    def maybe_upload_master(force=False):
        nonlocal uploaded_rows
        if not s3_bucket:
            return
        if force or uploaded_rows >= s3_master_every:
            uploaded_rows = 0
            rel = os.path.relpath(MASTER_XLSX, start=OUT_DIR).replace("\\", "/")
            key = f"{s3_prefix.rstrip('/')}/{rel}"
            log(f"[s3] upload master -> s3://{s3_bucket}/{key}")
            s3_upload_file(MASTER_XLSX, s3_bucket, key)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context()
        page = ctx.new_page()
        log(f"[step] Shard {shard_index+1}/{shard_count} starting. Go to root list")
        # Robust navigation: keep trying until connected
        while True:
            try:
                page.goto(ROOT_URL, wait_until="networkidle", timeout=90000)
                break
            except Exception as e:
                log(f"[warn] Initial goto failed, retrying in 3s: {e}")
                time.sleep(3)

        robust_switch_to_english(page)

        cont = get_list_container(page, timeout_ms=list_timeout_ms)
        wait_rows_ready(cont)

        # Effective start page for this shard: start_page + shard_index
        eff_start = max(1, start_page) + (shard_index if shard_count > 1 else 0)
        if eff_start > 1:
            log(f"[step] Fast-forwarding to shard start page {eff_start} …")
            try:
                fast_forward_to_page(cont, eff_start)
            except Exception as e:
                log(f"[warn] Could not fast-forward neatly: {e}")

        processed_pages = 0
        current_page = active_page_number(cont) or eff_start

        while True:
            n = current_page
            log(f"[page][shard {shard_index+1}/{shard_count}] Processing page {n}")
            rows = cont.locator(ROW_SELECTOR)
            row_count = rows.count()
            log(f"[info] rows found: {row_count}")

            for r in range(row_count):
                try:
                    rows = cont.locator(ROW_SELECTOR)
                    row = rows.nth(r)
                    eye = find_row_eye(row)
                    if not eye:
                        log(f"[skip] no 'view' action for row {r+1} on page {n}")
                        continue

                    prev_html = tbody_html(cont)
                    with page.expect_navigation():
                        eye.click()
                    try:
                        record = extract_detail(page)
                        log(f"[ok] extracted Activity ID={record.get('Activity ID', '?')}")
                        per_file = save_row_to_excels(record)
                        uploaded_rows += 1
                        maybe_upload_activity(per_file)
                        maybe_upload_master(force=False)
                    except Exception as e:
                        log(f"[warn] extraction failed on page {n}, row {r+1}: {e}")

                    try:
                        page.get_by_role("button", name="Back", exact=True).click()
                    except:
                        page.go_back(wait_until="networkidle", timeout=60000)
                    cont, _ = recover_list(page, expected_page_no=n, list_timeout_ms=list_timeout_ms)
                except Exception as e:
                    log(f"[warn] Row {r+1} failure: {e}")
                    try:
                        cont, _ = recover_list(page, expected_page_no=n, list_timeout_ms=list_timeout_ms)
                    except:
                        continue

            processed_pages += 1
            maybe_upload_master(force=True)

            # Respect --max-pages as "pages for THIS shard"
            if max_pages and processed_pages >= max_pages:
                log("[done] Reached --max-pages cap for this shard.")
                break

            # Move to the *next page for this shard*:
            # stride = shard_count; from current page, advance N pages via 'next' button
            stride = shard_count if shard_count > 1 else 1
            if not click_next_k(cont, stride):
                log("[done] Reached last page (or next disabled) for this shard.")
                break
            current_page = (active_page_number(cont) or (current_page + stride))

        maybe_upload_master(force=True)
        browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=0,
                    help="How many pages THIS SHARD will process (0 = until paginator ends for this shard).")
    ap.add_argument("--start-page", type=int, default=1,
                    help="Global 1-based start page. With sharding, effective start = start-page + shard-index.")
    ap.add_argument("--headless", action="store_true",
                    help="Run headless.")
    ap.add_argument("--list-timeout-ms", type=int, default=120000,
                    help="Timeout in ms to wait for <app-list-external-activities> to appear/settle.")

    # S3 options (optional)
    ap.add_argument("--s3-bucket", type=str, default="",
                    help="If set, upload outputs to this S3 bucket.")
    ap.add_argument("--s3-prefix", type=str, default="runs/current",
                    help="Key prefix under which to place files (e.g., runs/2025-11-05).")
    ap.add_argument("--s3-master-upload-every", type=int, default=25,
                    help="Upload master Excel every N rows (plus end-of-page/end-of-run).")

    # Sharding
    ap.add_argument("--shard-count", type=int, default=1,
                    help="Total number of parallel shards (default 1 = no sharding).")
    ap.add_argument("--shard-index", type=int, default=0,
                    help="Zero-based index of this shard (0..shard-count-1).")

    args = ap.parse_args()
    main(
        max_pages=args.max_pages,
        headless=args.headless,
        start_page=args.start_page,
        list_timeout_ms=args.list_timeout_ms,
        s3_bucket=(args.s3_bucket or None),
        s3_prefix=args.s3_prefix,
        s3_master_every=max(1, args.s3_master_upload_every),
        shard_count=args.shard_count,
        shard_index=args.shard_index,
    )
