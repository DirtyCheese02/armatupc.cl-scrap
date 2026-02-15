import os
import json
import re
import requests
import uuid as uuid_lib
from io import BytesIO
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from PIL import Image

# ================= CONFIGURACIÓN =================
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Schemas
SPECIFICATIONS_SCHEMA = "specifications"

SCRAP_OUTPUT_DIR = BASE_DIR / "Outputs"
LOG_FILE = BASE_DIR / "unmatched_log.txt"

# Mapeo de categorías a tablas
CATEGORY_TO_TABLE = {
    "CPUCooler_Air": "CpuCoolerSpecifications",
    "CPUCooler_Liquid": "CpuCoolerSpecifications",
    "NetworkAdapter": ["WiredNetworkAdapterSpecifications", "WirelessNetworkAdapterSpecifications"],
    "Case": "CaseSpecifications",
    "CaseFan": "CaseFanSpecifications",
    "CPU": "CPUSpecifications",
    "CPUCooler": "CpuCoolerSpecifications",
    "ExternalStorage": "ExternalStorageSpecifications",
    "FanController": "FanControllerSpecifications",
    "Headphones": "HeadphoneSpecifications",
    "Keyboard": "KeyboardSpecifications",
    "Memory": "RamSpecifications",
    "Monitor": "MonitorSpecifications",
    "Motherboard": "MotherboardSpecifications",
    "Mouse": "MouseSpecifications",
    "OperatingSystem": "OperatingSystemSpecifications",
    "OpticalDrive": "OpticalDriveSpecifications",
    "PowerSupply": "PowerSupplySpecifications",
    "SoundCard": "SoundCardSpecifications",
    "Speakers": "SpeakersSpecifications",
    "Storage": "InternalStorageSpecifications",
    "ThermalCompound": "ThermalPasteSpecifications",
    "UPS": "UpsSpecifications",
    "VideoCard": "GpuSpecifications",
    "Webcam": "WebcamSpecifications",
    "WiredNetworkAdapter": "WiredNetworkAdapterSpecifications",
    "WirelessNetworkAdapter": "WirelessNetworkAdapterSpecifications",
    "CPU_CPUCooler_ThermalCompound": ["CPUSpecifications", "CpuCoolerSpecifications", "ThermalPasteSpecifications"],
    "Mouse_Keyboard": ["MouseSpecifications", "KeyboardSpecifications"],
    "Storage_ExternalStorage": ["InternalStorageSpecifications", "ExternalStorageSpecifications"]
}


# ================= FUNCIONES =================

def parse_part_numbers(raw_val):
    if not raw_val: return []
    if isinstance(raw_val, list):
        return [str(v).strip() for v in raw_val if v]
    s = str(raw_val).strip()
    if s.startswith("[") and s.endswith("]"):
        content = s[1:-1]
        parts = []
        for p in content.split(','):
            clean_p = p.strip().strip("'").strip('"')
            if clean_p:
                parts.append(clean_p)
        return parts
    return [s]

def get_or_create_store(store_name):
    res = supabase.table("Stores").select("Id").eq("Name", store_name).execute()
    if res.data:
        return res.data[0]['Id']
    else:
        res = supabase.table("Stores").insert({"Name": store_name}).execute()
        return res.data[0]['Id']

def find_spec_id(tables, part_number):
    if isinstance(tables, str): target_tables = [tables]
    else: target_tables = tables
    candidates = parse_part_numbers(part_number)
    if not candidates: return None, None

    for table_name in target_tables:
        for candidate in candidates:
            try:
                res = supabase.schema(SPECIFICATIONS_SCHEMA).from_(table_name)\
                    .select("Id")\
                    .ilike("MetaPartNumber", f"%{candidate}%")\
                    .limit(1)\
                    .execute()
                if res.data:
                    return res.data[0]['Id'], table_name
            except Exception as e:
                continue
    return None, None

def download_and_convert_image(image_url):
    """
    Descarga una imagen desde una URL y la convierte a formato WebP.
    Retorna: (bytes_webp, error_message)
    """
    try:
        # Descargar imagen
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Abrir imagen con Pillow
        img = Image.open(BytesIO(response.content))
        
        # Convertir a RGB si es necesario (para PNGs con transparencia)
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Convertir a WebP
        output = BytesIO()
        img.save(output, format='WEBP', quality=85, method=6)
        output.seek(0)
        
        return output.read(), None
    except Exception as e:
        return None, str(e)

def upload_to_supabase_storage(image_bytes, filename):
    """
    Sube una imagen a Supabase Storage en el bucket 'ProductsImages'.
    Retorna: URL pública de la imagen o None si falla.
    """
    try:
        bucket_name = "ProductsImages"
        
        # Subir archivo
        result = supabase.storage.from_(bucket_name).upload(
            path=filename,
            file=image_bytes,
            file_options={"content-type": "image/webp"}
        )
        
        # Obtener URL pública
        public_url = supabase.storage.from_(bucket_name).get_public_url(filename)
        return public_url
    except Exception as e:
        print(f"   ⚠️  Error subiendo imagen: {e}")
        return None

def process_product_image(spec_id, table_name, image_url):
    """
    Procesa la imagen de un producto:
    1. Verifica si ya tiene imagen en la tabla de especificaciones
    2. Si no tiene, descarga, convierte a WebP y sube a Supabase
    3. Actualiza el campo ImageUrl en la tabla de especificaciones
    """
    try:
        # Verificar si ya tiene imagen
        existing = supabase.schema(SPECIFICATIONS_SCHEMA).from_(table_name)\
            .select("ImageUrl")\
            .eq("Id", spec_id)\
            .limit(1)\
            .execute()
        
        if not existing.data:
            return False
        
        current_image_url = existing.data[0].get('ImageUrl')
        
        # Si ya tiene imagen, no hacer nada
        if current_image_url:
            return True
        
        # Descargar y convertir imagen
        webp_bytes, error = download_and_convert_image(image_url)
        if error:
            print(f"   ⚠️  Error descargando imagen: {error}")
            return False
        
        # Generar nombre único para el archivo
        filename = f"{spec_id}.webp"
        
        # Subir a Supabase Storage
        public_url = upload_to_supabase_storage(webp_bytes, filename)
        if not public_url:
            return False
        
        # Actualizar ImageUrl en la tabla de especificaciones
        supabase.schema(SPECIFICATIONS_SCHEMA).from_(table_name).update({
            "ImageUrl": public_url
        }).eq("Id", spec_id).execute()
        
        print(f"   ✅ Imagen procesada y subida para {spec_id}")
        return True
        
    except Exception as e:
        print(f"   ⚠️  Error procesando imagen: {e}")
        return False

# ================= PROCESO PRINCIPAL =================

def process_daily_scraps():
    print("🚀 Iniciando procesamiento (Con Deduplicación y Precio Mínimo)...")
    
    with open(LOG_FILE, 'w', encoding='utf-8') as log:
        log.write(f"--- Reporte de No Match: {datetime.now()} ---\n")

    store_batches = {} 

    if not os.path.exists(SCRAP_OUTPUT_DIR):
        print("❌ Directorio no encontrado.")
        return

    # 1. Lectura de Archivos
    for root, dirs, files in os.walk(SCRAP_OUTPUT_DIR):
        for filename in files:
            if filename.endswith(".json"):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = json.load(f)
                        if isinstance(content, dict): content = [content]
                        
                        for item in content:
                            s_name = item.get("store_name")
                            if s_name:
                                if s_name not in store_batches: store_batches[s_name] = []
                                item["_source_file"] = filename
                                store_batches[s_name].append(item)
                except Exception as e:
                    print(f"❌ Error en {filename}: {e}")

    # 2. Procesamiento por Tienda
    for store_name, items in store_batches.items():
        print(f"\n🔵 Tienda: {store_name} - Items brutos: {len(items)}")
        store_id = get_or_create_store(store_name)
        
        # --- FASE A: Deduplicación en Memoria ---
        # Usaremos un diccionario donde la clave sea el SpecId (el producto único)
        # y el valor sea el item con el MENOR precio encontrado.
        unique_products_today = {} # { "UUID-XXX": {data_del_item_mas_barato} }
        
        # Lista para logs de error que escribiremos después
        unmatched_buffer = []

        print("   🔍 Analizando y deduplicando...")
        for item in items:
            raw_type = item.get("type")
            part_num = item.get("part #")
            price = item.get("price")
            url = item.get("url")
            source_file = item.get("_source_file", "unknown")
            
            if not raw_type or not part_num or not price: continue

            target_tables = CATEGORY_TO_TABLE.get(raw_type)
            if not target_tables: continue
            
            # Buscamos ID
            spec_id, found_table = find_spec_id(target_tables, part_num)
            
            if spec_id and found_table:
                try:
                    price_int = int(price)
                except:
                    continue

                # LÓGICA DE PRECIO MÍNIMO:
                if spec_id in unique_products_today:
                    # Ya vimos este producto hoy. ¿El nuevo es más barato?
                    existing_price = unique_products_today[spec_id]['price_int']
                    if price_int < existing_price:
                        # Reemplazamos con el más barato
                        unique_products_today[spec_id] = {
                            "spec_id": spec_id,
                            "table": found_table,
                            "price_int": price_int,
                            "url": url,
                            "image_url": item.get("image_url"),
                        }
                else:
                    # Primera vez que vemos este producto hoy
                    unique_products_today[spec_id] = {
                        "spec_id": spec_id,
                        "table": found_table,
                        "price_int": price_int,
                        "url": url,
                        "image_url": item.get("image_url"),
                    }
            else:
                unmatched_buffer.append(f"[{source_file}] {url} | TYPE: {raw_type} | PN: {part_num}")

        # Escribir logs de no encontrados
        if unmatched_buffer:
            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                for entry in unmatched_buffer:
                    log.write(entry + "\n")

        print(f"   💾 Insertando {len(unique_products_today)} productos únicos en DB...")

        # --- FASE B: Inserción en Base de Datos ---
        # Ahora recorremos la lista limpia (sin duplicados, precio mínimo garantizado)
        
        found_ids_today = set()

        for spec_id, data in unique_products_today.items():
            found_ids_today.add(spec_id)
            
            # 1. Upsert ProductPricing (Estado Actual)
            supabase.table("ProductPricing").upsert({
                "SpecId": spec_id,
                "SpecTableName": data["table"],
                "StoreId": store_id,
                "Price": data["price_int"],
                "StockStatus": True,
                "Url": data["url"],
                "LastUpdated": datetime.now().isoformat()
            }, on_conflict="SpecId, SpecTableName, StoreId").execute()
            
            # 2. Insert PriceHistory (Nueva entrada siempre)
            # Como ya deduplicamos, esto solo insertará 1 vez por producto por ejecución.
            supabase.table("PriceHistory").insert({
                "SpecId": spec_id,
                "SpecTableName": data["table"],
                "StoreId": store_id,
                "Price": data["price_int"],
                "RecordedAt": datetime.now().isoformat()
            }).execute()
            
            # 3. Procesar imagen del producto si existe y no es N/A
            if "image_url" in data and data["image_url"] != "N/A":
                process_product_image(spec_id, data["table"], data["image_url"])

        # --- FASE C: Stock Agotado ---
        print("   🔄 Verificando stock agotado...")
        active_products = supabase.table("ProductPricing")\
            .select("SpecId")\
            .eq("StoreId", store_id)\
            .eq("StockStatus", True)\
            .execute()
            
        active_ids_db = {row['SpecId'] for row in active_products.data}
        missing_ids = active_ids_db - found_ids_today
        
        if missing_ids:
            print(f"   📉 {len(missing_ids)} productos marcados como NO DISPONIBLES.")
            for missing in missing_ids:
                supabase.table("ProductPricing").update({
                    "StockStatus": False,
                    "LastUpdated": datetime.now().isoformat()
                }).eq("SpecId", missing).eq("StoreId", store_id).execute()

        supabase.table("Stores").update({"LastScrapedAt": datetime.now().isoformat()}).eq("Id", store_id).execute()

    print(f"\n🏁 Listo. Logs en '{LOG_FILE}'.")

if __name__ == "__main__":
    process_daily_scraps()
