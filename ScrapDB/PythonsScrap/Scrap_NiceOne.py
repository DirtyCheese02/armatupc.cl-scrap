import asyncio
from asyncio import tasks
import os
import json
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions
import hashlib
import math
import re


MAX_CONCURRENT_TABS_COLLECTOR = 8  # Pestañas para buscar links
MAX_CONCURRENT_TABS_SCRAPER = 6    # Pestañas para scrapear productos

CATEGORY_URL_MAP = {
    "OperatingSystem": "https://n1g.cl/Home/38-software",
    "UPS": "https://n1g.cl/Home/94-ups",
    "Headphones": "https://n1g.cl/Home/32-parlantes-audio",
    "Storage": ["https://n1g.cl/Home/55-discos-hdd", "https://n1g.cl/Home/54-discos-ssd","https://n1g.cl/Home/56-discos-25"],
    "Monitor": "https://n1g.cl/Home/28-monitores",
    "CPUCooler_Air": "https://n1g.cl/Home/61-disipador-por-aire",
    "CPUCooler_Liquid": "https://n1g.cl/Home/62-watercooling",
    "CaseFan": "https://n1g.cl/Home/63-ventiladores",
    "ThermalCompound": "https://n1g.cl/Home/64-pasta-disipadora",
    "PowerSupply": ["https://n1g.cl/Home/57-fuentes-certificadas-modular","https://n1g.cl/Home/58-fuentes-certificadas-no-modular"],
    "Case": "https://n1g.cl/Home/24-gabinetes",
    "Memory": ["https://n1g.cl/Home/77-ddr5-pc","https://n1g.cl/Home/33-placas-madre"],
    "CPU": "https://n1g.cl/Home/34-procesadores",
    "VideoCard": ["https://n1g.cl/Home/130-intel","https://n1g.cl/Home/110-nvidia","https://n1g.cl/Home/111-amd"],
    "Motherboard": "https://n1g.cl/Home/33-placas-madre",
    "NetworkAdapter": "https://n1g.cl/Home/76-adaptadores-de-red",
    "Mouse_Keyboard": "https://n1g.cl/Home/29-mouse-teclados"
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
                        next_page_url = f"{category_url}?page={i}"
                        await page.go_to(next_page_url)
                        await asyncio.sleep(4)
                    except Exception as e:
                        print(f"   ❌ Error paginando {category_name}: {e}")
                        break
                
                # Extraer links de la tabla
                links = await page.query("//div[@class='product-description']/h3[@class='h3 product-title']/a", find_all=True)

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
        # Busca los botones de paginación
        number = await Tab.query("//div[contains(@class, 'total-products')]/p", find_all=True)
        if not number:
            return 1
        txt = await number.text
        txt = txt.split(" ")[1]
        if txt.isdigit():
            digit = math.ceil(int(txt)/48)
            return digit
        return 1

    except:
        return 1


async def scrape_product_details(sem, browser, url, category_name):
    async with sem:
        page = await browser.new_tab()
        try:
            found = False
            product_name = ""
            partnumber = "N/A"
            await page.go_to(url)
            
            await asyncio.sleep(6)

            # 2. Nombre del Producto
            try:
                product_name = await page.query("//h1[@class='product_name']")
                product_name = await product_name.text
                    
            except:
                try:
                    product_name = await page.query("//h1[@itemprop='name']")
                    product_name = await product_name.text
                except:
                    pass

            # Manufacturer
            manufacturer = "N/A"
            
            try:                            
                price = await page.query("//span[@class='price']")
                price = await price.text
                price = price.strip().replace("$","").replace(".","").strip()
            except:
                pass
            
            part_element = None
            try:
                name = await page.query("//table[@class='table-horizontal']/tbody/tr/th", find_all=True)
                value = await page.query("//table[@class='table-horizontal']/tbody/tr/td", find_all=True)
                for i in name:
                    txt = await i.text
                    if "número de pieza" in txt.lower() or "part number" in txt.lower():
                        index = name.index(i)
                        part_element = value[index]
                    elif "model" in txt.lower() or "modelo" in txt.lower():
                        index = name.index(i)
                        part_element = value[index]
                if part_element:
                    found = True
                    raw_text = await part_element.text
                    if raw_text:
                        partnumber = raw_text.strip()
                    else:
                        partnumber = "N/A"

            except:
                pass
            
            if not found:
                name_upper = (product_name or "").upper()
                candidates = re.findall(
                    r"\b(?=[A-Z0-9-]{4,20}\b)(?=[A-Z0-9-]*[A-Z])(?=[A-Z0-9-]*\d)[A-Z][A-Z0-9-]*\b",
                    name_upper
                )

                blacklist_prefix = ("SATA", "DDR", "USB", "PCIE", "NVME")
                candidates = [c for c in candidates if not c.startswith(blacklist_prefix)]

                partnumber = max(candidates, key=len) if candidates else "N/A"
                found = True


            # Imagen
            try:
                image_element = await page.query("//div[@class='product-cover']/img")
                image = image_element.get_attribute("src")
            except:
                pass

# 5. CONSTRUIR JSON PLANO (FORMATO SOLICITADO)
            final_data = {
                "store_name": "NiceOne",
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
                with open(f"ScrapDB/Outputs/NiceOne/NO_{hashlib.md5(url.encode()).hexdigest()}.json", "w", encoding="utf-8") as f:
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
    #options.add_argument("--window-size=1280,720")
    #options.add_argument("--no-sandbox")
    #options.add_argument("--disable-dev-shm-usage")
    #options.add_argument("--disable-gpu")
    #print(
    #    f"[Browser] headless={options.headless} "
    #    f"binary={'auto' if not chrome_binary else chrome_binary} "
    #    f"start_timeout={options.start_timeout}s"
    #)
    
    browser = Chrome(options=options)
    await browser.start()

    # Limpieza inicial de carpeta
    output_dir = "ScrapDB/Outputs/NiceOne"
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
