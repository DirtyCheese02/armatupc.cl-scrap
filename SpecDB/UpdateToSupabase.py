import os
import json
import re
from dotenv import load_dotenv
from supabase import create_client, Client

# ================= CONFIGURACIÓN =================
load_dotenv()
URL: str = os.environ.get("SUPABASE_URL")
KEY: str = os.environ.get("SUPABASE_KEY")

if not URL or not KEY:
    raise ValueError("❌ Faltan credenciales SUPABASE_URL o SUPABASE_KEY en .env")

supabase: Client = create_client(URL, KEY)

# Schema para especificaciones
SPECIFICATIONS_SCHEMA = "specifications"

DATA_DIR = "SpecDB/ScrapedDataPCPP"

# Mapeo EXACTO basado en tus carpetas y las tablas que creamos
CATEGORY_TO_TABLE = {
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
    "WirelessNetworkAdapter": "WirelessNetworkAdapterSpecifications"
}

# ================= UTILIDADES =================

def normalize_key(key):
    """
    Normaliza las claves del JSON para que coincidan con las columnas de Supabase.
    """
    # 1. FIX CRÍTICO PARA OPTICAL DRIVES:
    # Convertir 'DVD+R' -> 'DVDPlusR', 'DVD-R' -> 'DVDMinusR' antes de borrar símbolos
    key = key.replace("+", "Plus").replace("-", "Minus")
    
    # 2. Eliminar cualquier otro caracter especial (espacios, paréntesis, puntos, slash)
    clean = re.sub(r'[^a-zA-Z0-9]', '', key)
    return clean

def map_json_to_db_row(data):
    row = {}
    
    # 1. Campos Fijos (Meta)
    row["MetaName"] = data.get("name", "Unknown")
    row["ImageUrl"] = data.get("img_url")
    # Soporte legacy para 'pcpartpicker_url' o 'url'
    row["pcpp_link"] = data.get("pcpartpicker_url") or data.get("url")
    
    if "Manufacturer" in data:
        row["MetaManufacturer"] = data["Manufacturer"]
    
    # Manejo flexible de 'Part #' (a veces es lista, a veces string)
    part_num = data.get("Part #")
    if part_num:
        row["MetaPartNumber"] = str(part_num) # Convertimos lista a string si es necesario
        
    # 2. Campos Dinámicos (Specs)
    ignore_keys = {"name", "img_url", "pcpartpicker_url", "url", "Manufacturer", "Part #"}
    
    for key, value in data.items():
        if key not in ignore_keys:
            db_col = normalize_key(key)
            # Todo a string para evitar errores de tipo en la DB
            row[db_col] = str(value)
                
    return row

# ================= MAIN =================

def main():
    if not os.path.exists(DATA_DIR):
        print(f"❌ No existe la carpeta {DATA_DIR}")
        return

    print("🚀 Iniciando carga masiva a Supabase...")

    for category_folder in os.listdir(DATA_DIR):
        # Limpieza nombre carpeta (por si tiene espacios)
        clean_folder = category_folder.replace(" ", "")
        
        # Buscar tabla
        table_name = CATEGORY_TO_TABLE.get(clean_folder)
        
        # Fallback: intentar buscar tal cual viene
        if not table_name:
            table_name = CATEGORY_TO_TABLE.get(category_folder)

        if not table_name:
            # print(f"⚠️ Saltando carpeta desconocida: {category_folder}")
            continue

        folder_path = os.path.join(DATA_DIR, category_folder)
        if not os.path.isdir(folder_path): continue

        print(f"\n📂 {category_folder} -> Tabla: {table_name}")
        
        batch_rows = []
        files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
        
        for filename in files:
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                
                # Validar URL para el constraint UNIQUE
                if not raw_data.get("pcpartpicker_url") and not raw_data.get("url"):
                    continue

                db_row = map_json_to_db_row(raw_data)
                batch_rows.append(db_row)
                
            except Exception as e:
                print(f"   ❌ Error leyendo {filename}: {e}")

        # Insertar / Actualizar (Upsert)
        if batch_rows:
            chunk_size = 100
            total_processed = 0
            
            for i in range(0, len(batch_rows), chunk_size):
                chunk = batch_rows[i:i + chunk_size]
                try:
                    # Upsert usando schema specifications
                    supabase.schema(SPECIFICATIONS_SCHEMA).from_(table_name).upsert(
                        chunk, 
                        on_conflict="pcpp_link", 
                        ignore_duplicates=True
                    ).execute()
                    
                    total_processed += len(chunk)
                    print(f"      ✅ Procesados: {total_processed}/{len(batch_rows)}", end="\r")
                    
                except Exception as e:
                    print(f"\n      🔥 Error DB en lote: {e}")
            print("") # Salto de línea al terminar categoría
        else:
            print("      ℹ️ Sin archivos válidos.")

    print("\n🏁 ¡Carga finalizada con éxito!")

if __name__ == "__main__":
    main()