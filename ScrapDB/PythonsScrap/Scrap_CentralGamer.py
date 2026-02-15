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
    "UPS": "https://centralgamer.cl/componentes-pc/energia-y-proteccion/",
    "Headphones": "https://centralgamer.cl/perifericos/audifonos-gamer/",
    "Mouse": "https://centralgamer.cl/perifericos/mouse-gamer/",
    "Keyboard": "https://centralgamer.cl/perifericos/teclado-gamer/",
    "Storage": "https://centralgamer.cl/componentes-pc/almacenamiento/",
    "Monitor": "https://centralgamer.cl/monitores/monitores-gamer/",
    "CPUCooler_ThermalCompound": "https://centralgamer.cl/componentes-pc/refrigeracion-pc/",
    "CaseFan": "https://centralgamer.cl/componentes-pc/ventiladores/",
    "PowerSupply": "https://centralgamer.cl/componentes-pc/fuentes-de-poder/",
    "Case": "https://centralgamer.cl/componentes-pc/gabinetes-gamer/",
    "Memory": "https://centralgamer.cl/componentes-pc/memorias-ram/",
    "CPU": "https://centralgamer.cl/componentes-pc/procesadores/",
    "VideoCard": "https://centralgamer.cl/componentes-pc/tarjetas-de-video/",
    "Motherboard": "https://centralgamer.cl/componentes-pc/placas-madre/",
    "Webcam": "https://centralgamer.cl/perifericos/streaming/webcams/",
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
                        next_page_url = f"{category_url}page/{i}/"
                        await page.go_to(next_page_url)
                        await asyncio.sleep(4)
                    except Exception as e:
                        print(f"   ❌ Error paginando {category_name}: {e}")
                        break
                
                # Extraer links de la tabla
                links = await page.query("//div[contains(@class,'minimog-grid')]/div[contains(@class,'grid-item')]//h3[contains(@class,'product__title')]/a", find_all=True)

                new_count = 0
                for link in links:
                    href = link.get_attribute("href")
                    if not href: continue
                    
                    full_link = href.strip()
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
        list_items = []
        numbers = await Tab.query("/html/body/div/div/div/div/div/div/div/div/div/div/div/div/p", find_all=True)
        for p in numbers:
            text = await p.text
            if "resultados" in text:
                list_items.append(text)
        
        number = list_items[0].replace(" resultados","").split(" ")[-1].strip()
        number = int(number)
        return (number // 12) + 1  # Asumiendo 12 productos

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
                product_name = await page.query("//h2[@class='heading']/span")
                product_name = await product_name.text
                    
            except:
                pass
            
            # Manufacturer
            manufacturer = "N/A"
            
            try:                            
                price = await page.query("//span[@class='precio-efectivo-valor']")
                price = await price.text
                price = price.replace("$","").replace(".","").strip()
            except:
                pass
            
            try:
                part_element = await page.query("//span[@class='part-number']")
                if part_element:
                    found = True
                    raw_text = await part_element.text
                    if raw_text:
                        partnumber = raw_text.strip()
                    else:
                        partnumber = "N/A"
                else:
                    partnumber = "N/A"

            except Exception as e:
                print(f"Error extrayendo partnumber: {e}")
                partnumber = "Error"
            
            # Imagen
            try:
                image_element = await page.query("//img[contains(@class,'wp-post-image')]")
                image = image_element.get_attribute("src")
            except:
                pass

# 5. CONSTRUIR JSON PLANO (FORMATO SOLICITADO)
            final_data = {
                "store_name": "CentralGamer",
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
                with open(f"ScrapDB/Outputs/CentralGamer/CG_{hashlib.md5(url.encode()).hexdigest()}.json", "w", encoding="utf-8") as f:
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
    output_dir = "ScrapDB/Outputs/CentralGamer"
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
