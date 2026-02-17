import asyncio
from asyncio import tasks
import os
import json
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions
import hashlib


MAX_CONCURRENT_TABS_COLLECTOR = 8  # Pestañas para buscar links
MAX_CONCURRENT_TABS_SCRAPER = 6    # Pestañas para scrapear productos

CATEGORY_URL_MAP = {
    "OperatingSystem": ["https://tecnomas.cl/productos/categorias/Microsoft","https://tecnomas.cl/productos/categorias/Software"],
    "UPS": "https://tecnomas.cl/productos/categorias/UPS",
    "Headphones": "https://tecnomas.cl/productos/categorias/Audio",
    "Mouse_Keyboard": "https://tecnomas.cl/productos/categorias/Teclados y Mouse",
    "Storage_ExternalStorage": ["https://tecnomas.cl/productos/categorias/Almacenamiento", "https://tecnomas.cl/productos/categorias/Almacenamiento Externo"],
    "Monitor": "https://tecnomas.cl/productos/categorias/Monitores",
    "PowerSupply": "https://tecnomas.cl/productos/categorias/Fuentes de Poder",
    "Case": "https://tecnomas.cl/productos/categorias/Gabinetes",
    "Memory": "https://tecnomas.cl/productos/categorias/RAM",
    "CPU": "https://tecnomas.cl/productos/categorias/Procesadores",
    "VideoCard": "https://tecnomas.cl/productos/categorias/Tarjetas de Video",
    "Motherboard": "https://tecnomas.cl/productos/categorias/Placas Madre",
    "Webcam": "https://tecnomas.cl/productos/categorias/Webcam",
    "NetworkAdapter": "https://tecnomas.cl/productos/categorias/Tarjetas de Red",
    "CPUCooler_CaseFan": "https://tecnomas.cl/productos/categorias/Ventiladores y Sistemas de Enfriamiento"
}


async def process_category_links(sem, browser, category_name, category_url, links_to_scrape):
    async with sem:
        print(f"🔵 [COLLECTOR] Iniciando: {category_name}")
        page = await browser.new_tab()
        try:
            await page.go_to(category_url)
            await asyncio.sleep(6) 

            total_pages = await getPagination(page)
            print(f"   📄 {category_name}: {total_pages} páginas detectadas.")

            for i in range(1, total_pages + 1):
                print(f"   📄 {category_name} Pág {i}")
                if i != 1:
                    try:
                        # Navegación por URL query params es más segura que clicks en PCPP
                        next_page_url = f"{category_url}?pagina={i}"
                        await page.go_to(next_page_url)
                        await asyncio.sleep(4)
                    except Exception as e:
                        print(f"   ❌ Error paginando {category_name}: {e}")
                        break
                
                # Extraer links de la tabla
                links = await page.query("//li[contains(@class,'ais-Hits-item')]/a", find_all=True)

                new_count = 0
                for link in links:
                    href = link.get_attribute("href")
                    if not href: continue
                    
                    full_link = "https://tecnomas.cl" + href.strip()
                    item = [category_name, full_link]
                    if item not in links_to_scrape:
                        links_to_scrape.append(item)
                        new_count += 1
                
                print(f"   ➡ {category_name} Pág {i}: {new_count} nuevos links.")

        except Exception as e:
            print(f"🔥 Error en collector {category_name}: {e}")
        finally:
            await page.close()

async def getPagination(Tab):
    try:
        # Busca los botones de paginación
        pagination = await Tab.query("//div[@id='pagination']/div/ul/li[contains(@class,'ais-Pagination-item--page')]", find_all=True)
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


async def scrape_product_details(sem, browser, url, category_name):
    async with sem:
        page = await browser.new_tab()
        try:
            found = False
            await page.go_to(url)
            
            await asyncio.sleep(6)

            # 2. Nombre del Producto
            try:
                product_name = await page.query("//h1[contains(@id,'name-')]")
                product_name = await product_name.text
                    
            except:
                pass

            # Manufacturer
            try:
                manufacturer = await page.query("//a[contains(@id,'brand-')]")
                manufacturer = await manufacturer.text
            except:
                pass
            
            
            
            try:                            
                price = await page.query("//span[contains(@id,'wire-transfer-price-')]")
                price = await price.text
                price = price.replace("$","").replace(".","").strip()
            except:
                pass
            
            try:
                part_element = await page.query("//h2[contains(@id,'sku-')]")
                if part_element:
                    found = True
                    raw_text = await part_element.text
                    if raw_text:
                        partnumber = raw_text.replace("SKU: ","").strip()
                    else:
                        partnumber = "N/A"
                else:
                    partnumber = "N/A"

            except Exception as e:
                print(f"Error extrayendo partnumber: {e}")
                partnumber = "Error"
            
            # Imagen
            try:
                image_element = await page.query("//div[contains(@class,'swiper-zoom-container')]/img")
                image = image_element.get_attribute("src")
            except:
                pass

# 5. CONSTRUIR JSON PLANO (FORMATO SOLICITADO)
            final_data = {
                "store_name": "TecnoMas",
                "scraped_name": product_name,
                "scraped_brand": manufacturer,
                "type": category_name,
                "part #": partnumber,
                "price": price,
                "url": url,
                "image_url": image
            }
            
            # Guardar Json
            if found:
                with open(f"ScrapDB/Outputs/TecnoMas/TM_{hashlib.md5(url.encode()).hexdigest()}.json", "w", encoding="utf-8") as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)
                print(f"✅ Guardado: {url}")
        except Exception as e:
            print(f"❌ Error scrapeando {url}: {e}")
        finally:
            await page.close()
            
async def main():
    options = ChromiumOptions()
    options.headless = os.environ.get("SCRAP_HEADLESS", "1").lower() not in ("0", "false", "no")
    options.start_timeout = int(os.environ.get("SCRAP_BROWSER_START_TIMEOUT", "45"))
    chrome_binary = os.environ.get("CHROME_BINARY_PATH")
    if chrome_binary:
        options.binary_location = chrome_binary
    options.add_argument("--window-size=1280,720")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    print(
        f"[Browser] headless={options.headless} "
        f"binary={'auto' if not chrome_binary else chrome_binary} "
        f"start_timeout={options.start_timeout}s"
    )
    
    browser = Chrome(options=options)
    await browser.start()

    # Limpieza inicial de carpeta
    output_dir = "ScrapDB/Outputs/TecnoMas"
    if os.path.exists(output_dir):
        print("🧹 Limpiando datos anteriores...")
        for file in os.listdir(output_dir):
            if file.endswith(".json"):
                os.remove(os.path.join(output_dir, file))
    else:
        os.makedirs(output_dir, exist_ok=True)
    
    
    print("\n🚀 FASE 1: Buscando nuevos links en categorías...")
    links_to_scrape = []
    sem_collector = asyncio.Semaphore(MAX_CONCURRENT_TABS_COLLECTOR)
    tasks = []
    for cat_name, cat_url in CATEGORY_URL_MAP.items():
        if isinstance(cat_url, list):
            print("1")
            for url in cat_url:
                tasks.append(process_category_links(sem_collector, browser, cat_name, url, links_to_scrape))
        else:
            tasks.append(process_category_links(sem_collector, browser, cat_name, cat_url, links_to_scrape))

    if tasks:
        await asyncio.gather(*tasks)

    print(f"\n🚀 FASE 2: Scrapeando {len(links_to_scrape)} productos...")

    sem_scraper = asyncio.Semaphore(MAX_CONCURRENT_TABS_SCRAPER)
    
    # Convertir set a lista para iterar
    pending_list = links_to_scrape


    # Procesar en chunks para no saturar la memoria con miles de tareas
    chunk_size = 100
    for i in range(0, len(pending_list), chunk_size):
        chunk = pending_list[i:i + chunk_size]
        batch_tasks = []
        for category_name, url in chunk:
            batch_tasks.append(scrape_product_details(sem_scraper, browser, url, category_name))
        
        await asyncio.gather(*batch_tasks)
        print(f"💤 Descanso preventivo tras bloque {i}...")
        await asyncio.sleep(2) 

    await browser.stop()
    print("\n🏁 Todo finalizado.")


if __name__ == "__main__":
    asyncio.run(main())
