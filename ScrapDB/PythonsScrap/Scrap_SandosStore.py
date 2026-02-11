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
    "Case": "https://sandos.cl/componentes-gabinetes?filtro_categoria=[%%2229%%22%%2C%%22142%%22%%2C%%2239%%22]",
    "CaseFan": "https://sandos.cl/componentes-gabinetes?filtro_categoria=[$%22143$%22]",
    "CPU": "https://sandos.cl/componentes-procesador",
    "CPUCooler_Air": "https://sandos.cl/componentes-refrigeracion-y-ventilacion-refrigeracion-aire",
    "CPUCooler_Liquid": "https://sandos.cl/componentes-refrigeracion-y-ventilacion-refrigeracion-liquida",
    "ExternalStorage": "https://sandos.cl/almacenamiento-almacenamiento-externo?filtro_categoria=[%%22148%%22%%2C%%22147%%22]",
    "Headphones": "https://sandos.cl/audio-y-video-audifonos",
    "Keyboard": "https://www.sandos.cl/computadores-y-tablets-perifericos-teclados",
    "Memory": "https://sandos.cl/memorias-memorias-ram-memorias-ram-pc",
    "Monitor": "https://sandos.cl/monitores-y-pantallas-monitores?filtro_categoria=[%%22101%%22%%2C%%22169%%22]",
    "Motherboard": "https://sandos.cl/componentes-placa-madre",
    "Mouse": "https://www.sandos.cl/buscar?texto=mouse&filtro_categoria=[%%2276%%22]",
    "OperatingSystem": "https://www.sandos.cl/hogar-y-oficina-software-sistema-operativo-y-aplicaciones",
    "PowerSupply": "https://sandos.cl/componentes-fuente-de-poder",
    "Storage": "https://sandos.cl/almacenamiento-almacenamiento-interno?filtro_categoria=[%%22149%%22%%2C%%22146%%22%%2C%%2220%%22]",
    "ThermalCompound": "https://sandos.cl/componentes-refrigeracion-y-ventilacion-pasta-termica",
    "UPS": "https://www.sandos.cl/hogar-y-oficina-ups-y-energia-ups-y-respaldo-de-energia",
    "VideoCard": "https://sandos.cl/componentes-tarjeta-de-video",
    "Webcam": "https://www.sandos.cl/buscar?texto=webcam",
    "NetworkAdapter": "https://sandos.cl/producto/hpe-broadcom-bcm57416-adaptador-de-red-ocp-30-125510gbase-t-x-2-para-proliant-dl325-gen10-dl345-gen10-dl360-gen10-dl365-gen10-xl220n-gen10-xl290n-gen10-p14917"
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
                        if "?" in category_url:
                            connector = "&"
                        else:
                            connector = "?"
                        next_page_url = f"{category_url}{connector}page={i}"
                        await page.go_to(next_page_url)
                        await asyncio.sleep(4)
                    except Exception as e:
                        print(f"   ❌ Error paginando {category_name}: {e}")
                        break
                
                # Extraer links de la tabla
                links = await page.query("//div[@class='row']/div/div/div/div/a", find_all=True)

                new_count = 0
                for link in links:
                    href = link.get_attribute("href")
                    if not href: continue
                    
                    full_link = "https://www.sandos.cl" + href.strip()
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
        pagination = await Tab.query("//ul[@class='pagination']/li[contains(@class, 'page-item')]", find_all=True)
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
            
            await asyncio.sleep(6)


            # 2. Nombre del Producto
            try:                                 
                product_name = await page.query("/html/body/div/section/div/div/div/div/div/div/div/div/h1")
                product_name = await product_name.text
            except:
                pass
            
            try:                                    
                manufacturer = await page.query("/html/body/div/section/div/div/div/div/div/div/div/div/span[@class='brand-name']")
                manufacturer = await manufacturer.text
            except:
                pass
            
            try:                            
                price = await page.query("//div/div/div/div[@class='price-value-large']")
                price = await price.text
                price = price.replace("$","").replace(".","").strip()
            except:
                pass
            
            try:
                part_element = await page.query("/html/body/div/section/div/div/div/div/div/div/div/div[2]/span[1]")
                if part_element:
                    raw_text = await part_element.text
                    if "Part number:" in raw_text:
                        partnumber = raw_text.split("Part number:")[1].strip()
                    else:
                        partnumber = "N/A"
                else:
                    partnumber = "N/A"

            except Exception as e:
                print(f"Error extrayendo partnumber: {e}")
                partnumber = "Error"
                
            try:
                image = await page.query("//html/body/div/section/div/div/div/div/div/div/div/div/div/div/img")
                image = image.get_attribute("src").strip()
                image = "https://www.sandos.cl" + image
            except:
                image = "N/A"

# 5. CONSTRUIR JSON PLANO (FORMATO SOLICITADO)
            final_data = {
                "store_name": "Sandos",
                "scraped_name": product_name,
                "scraped_brand": manufacturer,
                "type": category_name,
                "part #": partnumber,
                "price": price,
                "url": url,
                "image_url": image
            }
            
            # Guardar Json

            with open(f"ScrapDB/Outputs/Sandos/SS_{hashlib.md5(url.encode()).hexdigest()}.json", "w", encoding="utf-8") as f:
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
    output_dir = "ScrapDB/Outputs/Sandos"
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
