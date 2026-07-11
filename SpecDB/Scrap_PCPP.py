import asyncio
import hashlib
import json
import os
import random

from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions

# ==========================================
# CONFIGURACION
# ==========================================
CACHE_DIR = "SpecDB/ScrapDatabaseCache"
OUTPUT_DIR = "SpecDB/ScrapedDataPCPP"

VISITED_FILE = f"{CACHE_DIR}/pcpp_links.txt"
LINKSTOVISIT_FILE = f"{CACHE_DIR}/pcpp_links_to_visit.txt"


def parse_int_env(env_name, default_value):
    raw = os.environ.get(env_name)
    if not raw:
        return default_value
    try:
        value = int(raw)
    except ValueError:
        print(f"[WARN] {env_name}={raw!r} is not an integer. Using {default_value}.")
        return default_value
    return value if value > 0 else default_value


def parse_bool_env(env_name, default_value=False):
    raw = os.environ.get(env_name)
    if raw is None:
        return default_value
    return raw.strip().lower() not in ("0", "false", "no", "off")


MAX_CONCURRENT_TABS_COLLECTOR = parse_int_env("PCPP_COLLECTOR_CONCURRENCY", 1)
MAX_CONCURRENT_TABS_SCRAPER = parse_int_env("PCPP_SCRAPER_CONCURRENCY", 1)
PCPP_READY_TIMEOUT = parse_int_env("PCPP_READY_TIMEOUT", 20)
PCPP_NAVIGATION_TIMEOUT = parse_int_env("PCPP_NAVIGATION_TIMEOUT", 120)
PCPP_ALLOW_BACKGROUND_TABS = parse_bool_env("PCPP_ALLOW_BACKGROUND_TABS", False)
PCPP_CAPTURE_ENABLED = parse_bool_env("PCPP_CAPTURE_ENABLED", False)
PCPP_PERMISSION_REFERENCE = os.environ.get("PCPP_PERMISSION_REFERENCE", "").strip()

if not PCPP_ALLOW_BACKGROUND_TABS:
    if MAX_CONCURRENT_TABS_COLLECTOR > 1:
        print("[PCPP] Background tabs are disabled; forcing collector concurrency to 1.")
        MAX_CONCURRENT_TABS_COLLECTOR = 1
    if MAX_CONCURRENT_TABS_SCRAPER > 1:
        print("[PCPP] Background tabs are disabled; forcing scraper concurrency to 1.")
        MAX_CONCURRENT_TABS_SCRAPER = 1

CATEGORY_URL_MAP = {
    "Case": "https://pcpartpicker.com/products/case/",
    "Case Fan": "https://pcpartpicker.com/products/case-fan/",
    "CPU": "https://pcpartpicker.com/products/cpu/",
    "CPU Cooler": "https://pcpartpicker.com/products/cpu-cooler/",
    "External Storage": "https://pcpartpicker.com/products/external-hard-drive/",
    "Fan Controller": "https://pcpartpicker.com/products/fan-controller/",
    "GPU": "https://pcpartpicker.com/products/video-card/",
    "Headphones": "https://pcpartpicker.com/products/headphones/",
    "Keyboard": "https://pcpartpicker.com/products/keyboard/",
    "Monitor": "https://pcpartpicker.com/products/monitor/",
    "Motherboard": "https://pcpartpicker.com/products/motherboard/",
    "Mouse": "https://pcpartpicker.com/products/mouse/",
    "Operating System": "https://pcpartpicker.com/products/os/",
    "Optical Drive": "https://pcpartpicker.com/products/optical-drive/",
    "Power Supply": "https://pcpartpicker.com/products/power-supply/",
    "RAM": "https://pcpartpicker.com/products/memory/",
    "Sound Card": "https://pcpartpicker.com/products/sound-card/",
    "Speakers": "https://pcpartpicker.com/products/speakers/",
    "Storage": "https://pcpartpicker.com/products/internal-hard-drive/",
    "Thermal Compound": "https://pcpartpicker.com/products/thermal-paste/",
    "UPS": "https://pcpartpicker.com/products/ups/",
    "Webcam": "https://pcpartpicker.com/products/webcam/",
    "Wired Network Adapter": "https://pcpartpicker.com/products/wired-network-card/",
    "Wireless Network Adapter": "https://pcpartpicker.com/products/wireless-network-card/",
}

HOMEPAGE_READY_SELECTORS = [
    "//a[contains(@href, '/products/cpu/')]",
    "//a[contains(@href, '/products/video-card/')]",
]
CATEGORY_READY_SELECTORS = ["//tbody[@id='category_content']/tr"]
PRODUCT_READY_SELECTORS = [
    "//div[contains(@class, 'group--spec')]",
    "/html/body/div[4]/div[1]/section/h1",
]

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# UTILIDADES
# ==========================================
def load_set_from_file(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def append_to_file(filename, text):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def get_filename_from_url(url, category):
    hash_obj = hashlib.md5(url.encode())
    return f"{category}_{hash_obj.hexdigest()}.json"


async def safe_page_title(page):
    try:
        return await page.title
    except Exception:
        return ""


async def focus_page(page):
    try:
        await page.bring_to_front()
        await asyncio.sleep(0.5)
    except Exception:
        pass


def is_cloudflare_title(title):
    normalized = (title or "").strip().lower()
    return any(
        marker in normalized
        for marker in (
            "just a moment",
            "un momento",
            "attention required",
            "checking your browser",
        )
    )


async def wait_for_any_selector(page, selectors, timeout=PCPP_READY_TIMEOUT):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        for selector in selectors:
            try:
                element = await page.query(selector, timeout=1, raise_exc=False)
                if element:
                    return True
            except Exception:
                continue
        await asyncio.sleep(0.5)
    return False


async def open_pcpp_page(page, url, ready_selectors, label=None):
    label = label or url
    await focus_page(page)
    try:
        await page.go_to(url, timeout=PCPP_NAVIGATION_TIMEOUT)
    except Exception as exc:
        print(f"[PCPP] Navigation error for {label}: {exc}")
        return False

    await asyncio.sleep(random.uniform(2, 4))

    if await wait_for_any_selector(page, ready_selectors, timeout=PCPP_READY_TIMEOUT):
        return True

    title = await safe_page_title(page)
    if is_cloudflare_title(title):
        print(
            f"[PCPP] Challenge detected for {label}. No automated challenge handling is allowed; "
            "keeping it pending."
        )
    else:
        print(f"[PCPP] Expected content did not load for {label}. Keeping it pending.")
    return False


async def get_first_product_href(page):
    try:
        link = await page.query(
            "//tbody[@id='category_content']/tr//a[contains(@href, '/product/')]",
            timeout=1,
            raise_exc=False,
        )
        if link:
            href = link.get_attribute("href")
            return href.strip() if href else ""
    except Exception:
        pass
    return ""


async def wait_for_category_page_update(page, previous_first_href, timeout=PCPP_READY_TIMEOUT):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if not await wait_for_any_selector(page, CATEGORY_READY_SELECTORS, timeout=1):
            await asyncio.sleep(0.5)
            continue

        current_first_href = await get_first_product_href(page)
        if current_first_href and current_first_href != previous_first_href:
            return True

        await asyncio.sleep(0.5)
    return False


async def go_to_category_page(page, category_name, page_number):
    await focus_page(page)
    previous_first_href = await get_first_product_href(page)

    try:
        page_link = await page.query(
            f"//ul[contains(@class, 'pagination')]//a[normalize-space()='{page_number}']",
            timeout=2,
            raise_exc=False,
        )
        if page_link:
            await page_link.click()
        else:
            await page.execute_script(
                f"""
                (() => {{
                    const target = '#page={page_number}';
                    if (window.location.hash === target) {{
                        window.dispatchEvent(new HashChangeEvent('hashchange'));
                    }} else {{
                        window.location.hash = target;
                    }}
                }})();
                """,
                return_by_value=True,
            )
    except Exception as exc:
        print(f"   Error requesting {category_name} page {page_number}: {exc}")
        return False

    await asyncio.sleep(random.uniform(2, 4))
    if await wait_for_category_page_update(page, previous_first_href):
        return True

    title = await safe_page_title(page)
    if is_cloudflare_title(title):
        print(f"   Cloudflare appeared while paginating {category_name} page {page_number}.")
    else:
        print(f"   {category_name} page {page_number} did not update the product table.")
    return False


# ==========================================
# PARTE 1: RECOLECTOR DE LINKS
# ==========================================
async def getPagination(tab):
    try:
        await asyncio.sleep(2)
        pagination = await tab.query("//ul[contains(@class, 'pagination')]//li/a", find_all=True)
        if not pagination:
            return 1

        pages = []
        for p in pagination:
            txt = await p.text
            if txt.isdigit():
                pages.append(int(txt))

        return max(pages) if pages else 1
    except Exception:
        return 1


async def process_category_links(sem, browser, category_name, category_url, visited_links, links_to_visit):
    async with sem:
        print(f"[COLLECTOR] Starting: {category_name}")
        page = await browser.new_tab()
        try:
            ready = await open_pcpp_page(
                page,
                category_url,
                CATEGORY_READY_SELECTORS,
                label=f"{category_name} category",
            )
            if not ready:
                return

            total_pages = await getPagination(page)
            print(f"   {category_name}: {total_pages} pages detected.")

            for i in range(1, total_pages + 1):
                print(f"   {category_name} page {i}")
                if i != 1:
                    try:
                        ready = await go_to_category_page(page, category_name, i)
                        if not ready:
                            break
                        await asyncio.sleep(random.uniform(3, 5))
                    except Exception as e:
                        print(f"   Error paginating {category_name}: {e}")
                        break

                links = await page.query("//tbody[@id='category_content']/tr//a", find_all=True)

                new_count = 0
                for link in links:
                    href = link.get_attribute("href")
                    if not href or "/product/" not in href:
                        continue

                    full_link = "https://pcpartpicker.com" + href.strip()

                    if full_link not in visited_links and full_link not in links_to_visit:
                        links_to_visit.add(full_link)
                        append_to_file(LINKSTOVISIT_FILE, full_link)
                        new_count += 1

                print(f"   {category_name} page {i}: {new_count} new links.")

        except Exception as e:
            print(f"Error in collector {category_name}: {e}")
        finally:
            await page.close()


# ==========================================
# PARTE 2: SCRAPER DE PRODUCTOS
# ==========================================
async def scrape_product_details(sem, browser, url):
    async with sem:
        page = await browser.new_tab()
        try:
            ready = await open_pcpp_page(
                page,
                url,
                PRODUCT_READY_SELECTORS,
                label=f"product {url}",
            )
            if not ready:
                return

            await asyncio.sleep(random.uniform(5, 8))

            category = None
            try:
                category_elem = await page.query("/html/body/div[4]/div[1]/section/section/ol/li/a")
                category = await category_elem.text
            except Exception:
                pass

            product_name = "Unknown Product"
            try:
                product_name_elem = await page.query("/html/body/div[4]/div[1]/section/h1")
                product_name = await product_name_elem.text
            except Exception:
                pass

            found = False
            specs = {}

            try:
                spec_blocks = await page.query("//div[@class='group group--spec']", find_all=True)
                for block in spec_blocks:
                    try:
                        try:
                            label_elem = await block.query("./h3")
                            label_text = (await label_elem.text).replace(":", "").strip()
                        except Exception:
                            print("No label found, skipping block.")
                            continue

                        try:
                            value_elem = await block.query("./div/p")
                            specs[label_text] = (await value_elem.text).strip()
                            found = True
                        except Exception:
                            list_elems = await block.query("./div/ul/li", find_all=True)
                            values_list = []
                            for li in list_elems:
                                txt = await li.text
                                values_list.append(txt.strip())

                            specs[label_text] = f"[{', '.join(values_list)}]"
                            found = True
                    except Exception:
                        continue
            except Exception:
                pass

            final_data = {
                "name": product_name,
            }
            final_data.update(specs)
            final_data["pcpartpicker_url"] = url

            if found and category:
                cat_folder = os.path.join(OUTPUT_DIR, category.replace(" ", ""))
                os.makedirs(cat_folder, exist_ok=True)

                filename = get_filename_from_url(url, category)
                filepath = os.path.join(cat_folder, filename)

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)

                append_to_file(VISITED_FILE, url)
                print(f"Saved: {category} | {product_name[:30]}...")
            else:
                print(f"[PCPP] No specs found or category missing for {url}. Keeping it pending.")
        except Exception as e:
            print(f"Error scraping {url}: {e}")
        finally:
            await page.close()


# ==========================================
# ORQUESTADOR PRINCIPAL
# ==========================================
async def main():
    if not PCPP_CAPTURE_ENABLED:
        print(
            "[PCPP] Capture is suspended. Historical files remain read-only. "
            "Set PCPP_CAPTURE_ENABLED only after obtaining written permission."
        )
        return
    if not PCPP_PERMISSION_REFERENCE:
        print(
            "[PCPP] Capture refused: PCPP_PERMISSION_REFERENCE must identify the written permission "
            "or data agreement that authorizes this source."
        )
        return

    visited_links = load_set_from_file(VISITED_FILE)
    links_to_visit = load_set_from_file(LINKSTOVISIT_FILE)

    links_to_visit = links_to_visit - visited_links

    print(f"Initial state: {len(visited_links)} visited | {len(links_to_visit)} pending.")
    print(
        "PCPP settings: "
        f"collector_concurrency={MAX_CONCURRENT_TABS_COLLECTOR}, "
        f"scraper_concurrency={MAX_CONCURRENT_TABS_SCRAPER}, "
        f"permission_reference={PCPP_PERMISSION_REFERENCE!r}"
    )

    options = ChromiumOptions()
    options.headless = False
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-blink-features=AutomationControlled")
    browser = Chrome(options=options)
    await browser.start()

    page = await browser.new_tab()
    try:
        await open_pcpp_page(
            page,
            "https://pcpartpicker.com",
            HOMEPAGE_READY_SELECTORS,
            label="PCPartPicker homepage",
        )
        await asyncio.sleep(10)
    finally:
        await page.close()

    if len(links_to_visit) < 1000:
        print("\nFASE 1: Searching category links...")
        if MAX_CONCURRENT_TABS_COLLECTOR != 0:
            sem_collector = asyncio.Semaphore(MAX_CONCURRENT_TABS_COLLECTOR)
            tasks = []
            for cat_name, cat_url in CATEGORY_URL_MAP.items():
                tasks.append(
                    process_category_links(
                        sem_collector,
                        browser,
                        cat_name,
                        cat_url,
                        visited_links,
                        links_to_visit,
                    )
                )

            if tasks:
                await asyncio.gather(*tasks)
                links_to_visit = load_set_from_file(LINKSTOVISIT_FILE) - visited_links

    print(f"\nFASE 2: Scraping {len(links_to_visit)} products...")

    sem_scraper = asyncio.Semaphore(MAX_CONCURRENT_TABS_SCRAPER)
    pending_list = list(links_to_visit)

    chunk_size = 100
    for i in range(0, len(pending_list), chunk_size):
        chunk = pending_list[i:i + chunk_size]
        batch_tasks = []
        for url in chunk:
            batch_tasks.append(scrape_product_details(sem_scraper, browser, url))

        await asyncio.gather(*batch_tasks)
        print(f"Preventive rest after block {i}...")
        await asyncio.sleep(2)

    await browser.stop()
    print("\nAll done.")


if __name__ == "__main__":
    asyncio.run(main())
