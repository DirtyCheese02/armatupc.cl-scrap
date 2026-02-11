import asyncio
from asyncio import tasks
import os
import json
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions
import hashlib

MAX_CONCURRENT_TABS_COLLECTOR = 6  # Pestañas para buscar links
MAX_CONCURRENT_TABS_SCRAPER = 6    # Pestañas para scrapear productos

CATEGORY_URL_MAP = {
    "Case": "https://www.spdigital.cl/categories/componentes-gabinetes/",
    "CaseFan": "https://www.spdigital.cl/categories/componentes-refrigeracion-y-ventilacion-ventilador-gabinete/",
    "CPU": "https://www.spdigital.cl/categories/componentes-procesador/",
    "CPUCooler_Air": "https://www.spdigital.cl/categories/componentes-refrigeracion-y-ventilacion-disipador-cpu/",
    "CPUCooler_Liquid": "https://www.spdigital.cl/categories/componentes-refrigeracion-y-ventilacion-refrigeracion-liquida/",
    "ExternalStorage": "https://www.spdigital.cl/categories/componentes-almacenamiento-externo/",
    "Headphones": "https://www.spdigital.cl/categories/gaming-y-streaming-perifericos-gamer-audifonos-gamer/",
    "Keyboard": "https://www.spdigital.cl/categories/gaming-y-streaming-perifericos-gamer-teclado-gamer/",
    "Memory": "https://www.spdigital.cl/categories/componentes-memorias-ram/",
    "Monitor": "https://www.spdigital.cl/categories/monitor-gamer/",
    "Motherboard": "https://www.spdigital.cl/categories/componentes-placa-madre/",
    "Mouse": "https://www.spdigital.cl/categories/gaming-y-streaming-perifericos-gamer-mouse-gamer/",
    "OperatingSystem": "https://www.spdigital.cl/categories/computacion-software-sistema-operativo-y-aplicaciones/?page=1&sort_by=RELEVANCE%3AASC",
    "PowerSupply": "https://www.spdigital.cl/categories/componentes-fuente-de-poder/",
    "Storage": "https://www.spdigital.cl/categories/componentes-almacenamiento/?page=1&sort_by=RELEVANCE%3AASC&f=category.in%3Ahdd-disco-duro-mecanico&f=category.in%3Assd-unidad-estado-solido&start_cursor=WyIxIiwgIiIsICJVbmlkYWQgU1NEIDUwMEdCIFdEX0JMQUNLIFNONzEwMCBOVk1lIC0gTS4yIDIyODAgUENJZSBHZW40LCBMZWN0dXJhIDY4MDBNQi9zLCBUTEMgM0QgTkFORCJd",
    "ThermalCompound": "https://www.spdigital.cl/categories/componentes-refrigeracion-y-ventilacion-pasta-termica/",
    "UPS": "https://www.spdigital.cl/categories/computacion-ups-y-energia-ups-y-respaldo-energia/",
    "VideoCard": "https://www.spdigital.cl/categories/componentes-tarjeta-de-video/",
    "Webcam": "https://www.spdigital.cl/categories/gaming-y-streaming-streaming-webcam",
    "NetworkAdapter": "https://www.spdigital.cl/categories/tarjeta-de-red/"
}


async def process_category_links(sem, browser, category_name, category_url, links_to_scrape):
    async with sem:
        print(f"🔵 [COLLECTOR] Iniciando: {category_name}")
        page = await browser.new_tab()
        try:
            await page.go_to(category_url)
            await asyncio.sleep(5) 

            total_pages = await getPagination(page)
            print(f"   📄 {category_name}: {total_pages} páginas detectadas.")

            for i in range(1, total_pages + 1):
                print(f"   📄 {category_name} Pág {i}")
                if i != 1:
                    try:
                        # Navegación por URL query params es más segura que clicks en PCPP
                        next_page_url = f"{category_url}{i}"
                        await page.go_to(next_page_url)
                        await asyncio.sleep(4)
                    except Exception as e:
                        print(f"   ❌ Error paginando {category_name}: {e}")
                        break
                
                # Extraer links de la tabla
                links = await page.query("//div/div[contains(@class, 'Fractal-ProductCard__productcard--container')]//a[contains(@class, 'Fractal-ProductCard--image')]", find_all=True)

                new_count = 0
                for link in links:
                    href = link.get_attribute("href")
                    if not href: continue
                    
                    full_link = "https://www.spdigital.cl" + href.strip()
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
        pagination = await Tab.query("//div[contains(@class, 'Fractal-Pagination--pagination')]/span/a/span", find_all=True)
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
            
            await page.go_to(url)
            
            await asyncio.sleep(4)


            # 2. Nombre del Producto
            try:                                 
                product_name = await page.query("/html/body/div/div/div/section/div/div/h1")
                product_name = await product_name.text
            except:
                pass
            

            
            
            try:                                 
                manufacturer = await page.query("/html/body/div/div/div/section/div/div/div/span[contains(@class, 'Fractal-Typography__typography--body')]")
                manufacturer = await manufacturer.text
            except:
                pass
            
            try:
                price = await page.query("/html/body/div/div/div/section/div/div/div/div/span/span[contains(@class, 'Fractal-Price--price')]")
                price = await price.text
                price = price.replace("$","").replace(".","").strip()
            except:
                pass
            
            try:
                part_element = await page.query("/html/body/div/div/div/section/div/div/div[contains(@class, 'Fractal-Typography__typography--soft')]")
                if part_element:
                    raw_text = await part_element.text
                    if "Part number:" in raw_text:
                        partnumber = raw_text.strip().split(" /")[0].replace("Part number: ","").strip()
                    else:
                        partnumber = "N/A"
                else:
                    partnumber = "N/A"

            except Exception as e:
                print(f"Error extrayendo partnumber: {e}")
                partnumber = "Error"
                
            try:
                image = await page.query("/html/body/div/div/div/section/div/div/div/div/div/img")
                image = image.get_attribute("src").strip()
                image = "https:" + image
            except:
                image = "N/A"

# 5. CONSTRUIR JSON PLANO (FORMATO SOLICITADO)
            final_data = {
                "store_name": "SP Digital",
                "scraped_name": product_name,
                "scraped_brand": manufacturer,
                "type": category_name,
                "part #": partnumber,
                "price": price,
                "url": url,
                "image_url": image
            }
            
            # Guardar Json

            with open(f"ScrapDB/Outputs/SPDigital/SP_{hashlib.md5(url.encode()).hexdigest()}.json", "w", encoding="utf-8") as f:
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
    output_dir = "ScrapDB/Outputs/SPDigital"
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
