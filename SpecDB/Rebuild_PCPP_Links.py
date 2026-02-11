import os
import json

# ==========================================
# CONFIGURACIÓN
# ==========================================
# Carpeta donde están los JSONs ya scrapeados
DATA_DIR = "SpecDB/ScrapedDataPCPP"

# Archivo que vamos a reconstruir
OUTPUT_FILE = "SpecDB/ScrapDatabaseCache/pcpp_links.txt"

def main():
    if not os.path.exists(DATA_DIR):
        print(f"❌ Error: No existe la carpeta de datos {DATA_DIR}")
        return

    print(f"📂 Escaneando archivos JSON en: {DATA_DIR}...")

    unique_links = set()
    files_processed = 0
    skipped_unknown = 0
    skipped_no_url = 0

    # Recorrer todas las subcarpetas
    for root, dirs, files in os.walk(DATA_DIR):
        for filename in files:
            if filename.endswith(".json"):
                file_path = os.path.join(root, filename)
                files_processed += 1
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                        # 1. Obtener Nombre y URL
                        name = data.get("name", "Unknown Product")
                        
                        # A veces se guarda como 'pcpartpicker_url' o 'url' dependiendo de la versión del script
                        url = data.get("pcpartpicker_url") or data.get("url")

                        # 2. Validaciones
                        if not url:
                            skipped_no_url += 1
                            continue

                        if name == "Unknown Product" or name == "Unknown":
                            skipped_unknown += 1
                            # print(f"   🔸 Saltando Unknown: {file_path}")
                            continue

                        # 3. Agregar al Set (automáticamente elimina duplicados)
                        unique_links.add(url.strip())

                except Exception as e:
                    print(f"❌ Error leyendo {filename}: {e}")

    # ==========================================
    # GUARDAR RESULTADO
    # ==========================================
    print("\n💾 Guardando archivo reconstruido...")
    
    # Asegurar que el directorio de salida existe
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            # Ordenamos los links alfabéticamente para mantener el orden
            for link in sorted(unique_links):
                f.write(link + "\n")
        
        print(f"✅ ¡Éxito! Archivo generado en: {OUTPUT_FILE}")
        print("-" * 40)
        print(f"📊 Estadísticas:")
        print(f"   - Archivos escaneados: {files_processed}")
        print(f"   - Links Únicos Válidos: {len(unique_links)}")
        print(f"   - Ignorados (Unknown): {skipped_unknown}")
        print(f"   - Ignorados (Sin URL): {skipped_no_url}")
        print("-" * 40)

    except Exception as e:
        print(f"🔥 Error escribiendo el archivo de salida: {e}")

if __name__ == "__main__":
    main()