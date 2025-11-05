# mustamir_cme_extractor.py
# Final version + resilient infinite retry for initial/recovery navigation
# and infinite retry to enforce English language switch.

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from urllib.parse import urlsplit
import pandas as pd
import os, re, time, argparse, random

ROOT_URL = "https://mustamir.scfhs.org.sa/account/external-activities"
OUT_DIR = "out"
ACTIVITY_DIR = os.path.join(OUT_DIR, "activities")
MASTER_XLSX = os.path.join(OUT_DIR, "external_activities_master.xlsx")

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

# English switch appears only when NOT already in English
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


# --------- Robust list connection (infinite retry) ---------
def goto_list_with_retry(page, list_timeout_ms: int = 120000, wait_until: str = "networkidle"):
    """
    Keep trying to reach the list view until success.
    Backoff: ~5s + small jitter, capped at ~120s between attempts.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            log(f"[nav] Opening list (attempt {attempt}) …")
            page.goto(ROOT_URL, wait_until=wait_until, timeout=max(90000, list_timeout_ms))
            cont = get_list_container(page, timeout_ms=list_timeout_ms)
            wait_rows_ready(cont)
            log("[nav] List loaded.")
            return cont
        except Exception as e:
            # Backoff before retry
            backoff = min(120, 5 + attempt * 3)
            sleep_s = backoff + random.uniform(0, 2.5)
            log(f"[nav] Failed to load list: {e}\n       Retrying in {sleep_s:.1f}s …")
            try:
                page.wait_for_timeout(int(sleep_s * 1000))
            except Exception:
                time.sleep(sleep_s)


# --------------- Force English (infinite retry) ---------------
def enforce_english_with_retry(page, list_timeout_ms: int = 120000):
    """
    Ensure UI is in English. If the English switch is visible, we click it and verify again.
    Success criteria: English switch is NOT visible (i.e., already English) AND list component is present.
    Retries indefinitely with gentle backoff and hard reloads as needed.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            # If switch is not visible, we assume we're already in English.
            link = page.locator(ENGLISH_SWITCH).first
            if not (link.count() and link.is_visible()):
                # Verify list is usable
                cont = get_list_container(page, timeout_ms=list_timeout_ms)
                wait_rows_ready(cont)
                log("[lang] English confirmed (toggle not visible).")
                return cont

            # Switch to English
            log(f"[lang] English toggle visible — switching (attempt {attempt}) …")
            link.click()
            # Wait for SPA/network and the list component to reappear
            try:
                page.wait_for_load_state("networkidle", timeout=max(60000, list_timeout_ms))
            except Exception:
                pass

            # After switching, verify: if the toggle disappears and list is present, we’re done
            cont = get_list_container(page, timeout_ms=list_timeout_ms)
            wait_rows_ready(cont)
            link_after = page.locator(ENGLISH_SWITCH).first
            if not (link_after.count() and link_after.is_visible()):
                log("[lang] English switch succeeded.")
                return cont

            # If still visible, do a hard reload of the route and re-check
            log("[lang] Toggle still visible after click — hard reload and retry …")
            page.goto(ROOT_URL, wait_until="networkidle", timeout=max(90000, list_timeout_ms))

        except Exception as e:
            # Backoff and try again
            backoff = min(120, 5 + attempt * 3)
            sleep_s = backoff + random.uniform(0, 2.5)
            log(f"[lang] Failed to enforce English: {e}\n       Retrying in {sleep_s:.1f}s …")
            try:
                page.wait_for_timeout(int(sleep_s * 1000))
            except Exception:
                time.sleep(sleep_s)


# --------------- List helpers ---------------
def get_list_container(page, timeout_ms: int = 90000):
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
    raise RuntimeError(
        f"Could not find <app-list-external-activities> within {timeout_ms} ms"
        + (f" (last error: {last_err})" if last_err else "")
    )

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

def active_page_number(container):
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
    if os.path.exists(MASTER_XLSX):
        master = pd.read_excel(MASTER_XLSX)
        all_cols = list(dict.fromkeys(list(master.columns) + list(row_dict.keys())))
        master = master.reindex(columns=all_cols)
        new_row = pd.DataFrame([row_dict]).reindex(columns=all_cols)
        master = pd.concat([master, new_row], ignore_index=True)
    else:
        master = pd.DataFrame([row_dict])
    master.to_excel(MASTER_XLSX, index=False)


def recover_list(page, expected_page_no=None, list_timeout_ms: int = 120000):
    """
    Try normal recovery first; if it fails, fall back to an infinite-retry reload of the list
    and re-enforce English before proceeding.
    """
    try:
        try:
            page.wait_for_load_state("networkidle", timeout=list_timeout_ms)
        except Exception:
            pass
        cont = get_list_container(page, timeout_ms=list_timeout_ms)
        wait_rows_ready(cont)
    except Exception as e:
        log(f"[recover] Direct recovery failed: {e}. Retrying via hard reload …")
        cont = goto_list_with_retry(page, list_timeout_ms=list_timeout_ms, wait_until="networkidle")
        # Force English every time we do a hard reload
        cont = enforce_english_with_retry(page, list_timeout_ms=list_timeout_ms)

    if expected_page_no:
        try:
            cur = active_page_number(cont)
            if cur != expected_page_no:
                fast_forward_to_page(cont, expected_page_no)
        except Exception:
            pass
    return cont, active_page_number(cont)


# ------------- Main -------------
def main(max_pages:int, headless:bool, start_page:int, list_timeout_ms:int):
    ensure_out()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context()
        # More forgiving defaults for slow responses
        ctx.set_default_timeout(max(list_timeout_ms, 120000))
        ctx.set_default_navigation_timeout(max(list_timeout_ms, 120000))

        page = ctx.new_page()
        log("[step] Go to root list (with infinite retry)")
        cont = goto_list_with_retry(page, list_timeout_ms=list_timeout_ms, wait_until="networkidle")

        # NEW: enforce English with infinite retry
        cont = enforce_english_with_retry(page, list_timeout_ms=list_timeout_ms)

        # Re-acquire container (belt-and-braces)
        cont = get_list_container(page, timeout_ms=list_timeout_ms)
        wait_rows_ready(cont)

        if start_page and start_page > 1:
            log(f"[step] Fast-forwarding to start page {start_page} …")
            try:
                fast_forward_to_page(cont, start_page)
            except Exception as e:
                log(f"[warn] Could not fast-forward neatly: {e}")

        processed_pages = 0
        current_page = active_page_number(cont) or start_page or 1

        while True:
            n = current_page
            log(f"[page] Processing page {n}")
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
                        save_row_to_excels(record)
                    except Exception as e:
                        log(f"[warn] extraction failed on page {n}, row {r+1}: {e}")

                    try:
                        page.get_by_role("button", name="Back", exact=True).click()
                    except Exception:
                        page.go_back(wait_until="networkidle", timeout=60000)

                    cont, _ = recover_list(page, expected_page_no=n, list_timeout_ms=list_timeout_ms)

                except Exception as e:
                    log(f"[warn] Row {r+1} failure: {e}")
                    try:
                        cont, _ = recover_list(page, expected_page_no=n, list_timeout_ms=list_timeout_ms)
                    except Exception as e2:
                        log(f"[warn] Recovery after row failure also failed: {e2}. Retrying list (infinite) …")
                        cont = goto_list_with_retry(page, list_timeout_ms=list_timeout_ms, wait_until="networkidle")
                        cont = enforce_english_with_retry(page, list_timeout_ms=list_timeout_ms)

            processed_pages += 1
            if max_pages and processed_pages >= max_pages:
                log("[done] Reached --max-pages cap.")
                break

            if not click_next(cont):
                log("[done] Reached last page or next disabled.")
                break

            current_page = active_page_number(cont) or (current_page + 1)

        browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=0,
                    help="How many pages to process (0 = all until paginator ends).")
    ap.add_argument("--start-page", type=int, default=1,
                    help="Page number to start from (1-based).")
    ap.add_argument("--headless", action="store_true",
                    help="Run headless.")
    ap.add_argument("--list-timeout-ms", type=int, default=120000,
                    help="Timeout in ms to wait for <app-list-external-activities> to appear/settle.")
    args = ap.parse_args()
    main(max_pages=args.max_pages, headless=args.headless,
         start_page=args.start_page, list_timeout_ms=args.list_timeout_ms)
