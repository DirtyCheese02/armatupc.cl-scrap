import asyncio
import os
import json
import random
import hashlib
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions
from pydoll.constants import Key

# ==========================================
# CONFIGURACIÓN
# ==========================================
CACHE_DIR = "SpecDB/ScrapDatabaseCache"
OUTPUT_DIR = "SpecDB/ScrapedDataPCPP"

VISITED_FILE = f"{CACHE_DIR}/pcpp_links.txt"
LINKSTOVISIT_FILE = f"{CACHE_DIR}/pcpp_links_to_visit.txt"

MAX_CONCURRENT_TABS_COLLECTOR = 10  # Pestañas para buscar links
MAX_CONCURRENT_TABS_SCRAPER = 6    # Pestañas para scrapear productos

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

# Asegurar directorios
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# UTILIDADES
# ==========================================
def load_set_from_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def append_to_file(filename, text):
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(text + "\n")

def get_filename_from_url(url, category):
    """Genera un nombre de archivo seguro usando hash para evitar caracteres raros."""
    hash_obj = hashlib.md5(url.encode())
    return f"{category}_{hash_obj.hexdigest()}.json"

# ==========================================
# PARTE 1: RECOLECTOR DE LINKS (Tu código mejorado)
# ==========================================
async def getPagination(tab):
    try:
        await asyncio.sleep(2)
        # Selector actualizado de paginación PCPP
        pagination = await tab.query("//ul[contains(@class, 'pagination')]//li/a", find_all=True)
        if not pagination:
            return 1
        
        # Filtrar solo números
        pages = []
        for p in pagination:
            txt = await p.text
            if txt.isdigit():
                pages.append(int(txt))
        
        return max(pages) if pages else 1
    except:
        return 1

async def process_category_links(sem, browser, category_name, category_url, visited_links, links_to_visit):
    async with sem:
        print(f"🔵 [COLLECTOR] Iniciando: {category_name}")
        page = await browser.new_tab()
        try:
            await page.go_to(category_url)
            await asyncio.sleep(30) 

            total_pages = await getPagination(page)
            print(f"   📄 {category_name}: {total_pages} páginas detectadas.")

            for i in range(1, total_pages + 1):
                print(f"   📄 {category_name} Pág {i}")
                if i != 1:
                    try:
                        # Navegación por URL query params es más segura que clicks en PCPP
                        next_page_url = f"{category_url}#page={i}"
                        await page.go_to(next_page_url)
                        await asyncio.sleep(random.uniform(3, 5))
                    except Exception as e:
                        print(f"   ❌ Error paginando {category_name}: {e}")
                        break
                
                # Extraer links de la tabla
                links = await page.query("//tbody[@id='category_content']/tr//a", find_all=True)
                
                new_count = 0
                for link in links:
                    href = link.get_attribute("href")
                    if not href or "/product/" not in href: continue
                    
                    full_link = "https://pcpartpicker.com" + href.strip()
                    
                    if full_link not in visited_links and full_link not in links_to_visit:
                        links_to_visit.add(full_link)
                        append_to_file(LINKSTOVISIT_FILE, full_link)
                        new_count += 1
                
                print(f"   ➡ {category_name} Pág {i}: {new_count} nuevos links.")

        except Exception as e:
            print(f"🔥 Error en collector {category_name}: {e}")
        finally:
            await page.close()

# ==========================================
# PARTE 2: SCRAPER DE PRODUCTOS (Nueva Lógica)
# ==========================================
async def scrape_product_details(sem, browser, url):
    async with sem:
        page = await browser.new_tab()
        try:
            
            async with page.expect_and_bypass_cloudflare_captcha(
            ):
                await page.go_to(url)
            
            #await page.enable_auto_solve_cloudflare_captcha()
            
            #await page.go_to(url)
            
            await asyncio.sleep(random.uniform(5, 8)) # Espera humana
            
            #await page.disable_auto_solve_cloudflare_captcha()
            
            
            
            try:
                category = await page.query("/html/body/div[4]/div[1]/section/section/ol/li/a")
                category = await category.text
            except:
                pass

            # 2. Nombre del Producto
            product_name = "Unknown Product"
            try:
                product_name = await page.query("/html/body/div[4]/div[1]/section/h1")
                product_name = await product_name.text
            except:
                pass
            
            Found = False

            specs = {}                     
            try:                           
                spec_blocks = await page.query("//div[@class='group group--spec']", find_all=True)
                for block in spec_blocks:
                    try:
                        try:
                            label_elem = await block.query("./h3")
                            label_text = (await label_elem.text).replace(":", "").strip()
                        except:
                            print("No label found, skipping block.")
                            continue
                        # A. Intentar obtener valor simple
                        try:
                            value_elem = await block.query("./div/p")
                            specs[label_text] = (await value_elem.text).strip()
                            Found = True
                        except:
                            list_elems = await block.query("./div/ul/li", find_all=True) 
                            # Recolectamos todos los valores en una lista limpia
                            values_list = []
                            for li in list_elems:
                                txt = await li.text
                                values_list.append(txt.strip())
                            
                            # --- AQUÍ ESTÁ EL FORMATO QUE PEDISTE: "[Val1, Val2]" ---
                            # Unimos con coma y envolvemos en corchetes
                            formatted_list_string = f"[{', '.join(values_list)}]"
                            specs[label_text] = formatted_list_string
                            Found = True
                    except: 
                        continue
            except: 
                pass

# 5. CONSTRUIR JSON PLANO (FORMATO SOLICITADO)
            final_data = {
                "name": product_name,
            }
            
            # Mezclar specs en el nivel raíz
            final_data.update(specs)
            
            # Agregar URL al final (como pediste pc-builder.io_page, pero con pcpp)
            final_data["pcpartpicker_url"] = url

            # 6. Guardar
            if Found:
                cat_folder = os.path.join(OUTPUT_DIR, category.replace(" ", ""))
                os.makedirs(cat_folder, exist_ok=True)

                filename = get_filename_from_url(url, category)
                filepath = os.path.join(cat_folder, filename)

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)

                # 7. Actualizar historial
                append_to_file(VISITED_FILE, url)
                print(f"✅ Guardado: {category} | {product_name[:30]}...")
            else:
                print(final_data)
        except Exception as e:
            print(f"❌ Error scrapeando {url}: {e}")
        finally:
            await page.close()
# ==========================================
# ORQUESTADOR PRINCIPAL
# ==========================================
async def main():
    visited_links = load_set_from_file(VISITED_FILE)
    links_to_visit = load_set_from_file(LINKSTOVISIT_FILE)
    
    # Limpieza: Asegurar que no visitamos lo que ya está visitado
    links_to_visit = links_to_visit - visited_links
    
    print(f"📊 Estado Inicial: {len(visited_links)} visitados | {len(links_to_visit)} pendientes.")

    options = ChromiumOptions()
    options.headless = False
    options.add_argument("--window-size=1280,720")
    browser = Chrome(options=options)
    await browser.start()
    
    page = await browser.new_tab()
    async with page.expect_and_bypass_cloudflare_captcha():
        await page.go_to("https://pcpartpicker.com")
    await asyncio.sleep(10) # Espera humana

    # --- FASE 1: RECOLECTAR LINKS (Si hay pocas pendientes, buscamos más) ---
    # Si tienes muchos pendientes, puedes comentar esta fase para solo procesar
    if len(links_to_visit) < 1000: 
        print("\n🚀 FASE 1: Buscando nuevos links en categorías...")
        sem_collector = asyncio.Semaphore(MAX_CONCURRENT_TABS_COLLECTOR)
        tasks = []
        for cat_name, cat_url in CATEGORY_URL_MAP.items():
            tasks.append(process_category_links(sem_collector, browser, cat_name, cat_url, visited_links, links_to_visit))
        
        if tasks:
            await asyncio.gather(*tasks)
            # Recargar lista tras recolección
            links_to_visit = load_set_from_file(LINKSTOVISIT_FILE) - visited_links

    # --- FASE 2: PROCESAR PRODUCTOS (Scraping profundo) ---
    print(f"\n🚀 FASE 2: Scrapeando {len(links_to_visit)} productos...")
    
    sem_scraper = asyncio.Semaphore(MAX_CONCURRENT_TABS_SCRAPER)
    
    # Convertir set a lista para iterar
    pending_list = list(links_to_visit)
    
    # Procesar en chunks para no saturar la memoria con miles de tareas
    chunk_size = 100 
    for i in range(0, len(pending_list), chunk_size):
        chunk = pending_list[i:i + chunk_size]
        batch_tasks = []
        for url in chunk:
            batch_tasks.append(scrape_product_details(sem_scraper, browser, url))
        
        await asyncio.gather(*batch_tasks)
        print(f"💤 Descanso preventivo tras bloque {i}...")
        await asyncio.sleep(2) 

    await browser.stop()
    print("\n🏁 Todo finalizado.")

if __name__ == "__main__":
    asyncio.run(main())