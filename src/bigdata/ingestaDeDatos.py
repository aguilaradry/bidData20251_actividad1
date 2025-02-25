import requests
import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime

# Fuerza la codificación UTF-8 en Windows
if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8")

# 📌 Ruta de almacenamiento
DB_PATH = 'static/ingestion.db'
EXCEL_PATH = "static/muestra_datos.xlsx"
AUDIT_PATH = "static/auditoria.txt"

# 🔹 Eliminar la base de datos si ya existe (evita errores en pruebas)
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

# 🔹 Función para obtener los datos del API
def obtener_datos_api(url="", params={}):
    """Extrae datos desde el API de Mercado Bitcoin."""
    url = "{}/{}/{}/".format(url, params["coin"], params["method"])
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()  # Retorna los datos en formato JSON
    except requests.exceptions.RequestException as error:
        print(f"❌ Error al obtener datos del API: {error}")
        return {}

# 🔹 Función para crear la base de datos y la tabla en SQLite
def crear_base_datos():
    """Crea la base de datos SQLite y su tabla si no existen."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS datos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            clave TEXT,
            valor TEXT,
            fecha TEXT
        )
    ''')
    conn.commit()
    conn.close()

# 🔹 Función para insertar datos en SQLite
def insertar_datos(datos):
    """Inserta los datos obtenidos del API en SQLite."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ticker = datos.get("ticker", {})

    for clave, valor in ticker.items():
        cursor.execute("INSERT INTO datos (clave, valor, fecha) VALUES (?, ?, ?)", (clave, str(valor), fecha_actual))

    conn.commit()
    conn.close()
    print("✅ Datos insertados en la base de datos.")

# 🔹 Función para generar un archivo Excel con los datos almacenados
def generar_excel():
    """Genera un archivo Excel con los datos almacenados en SQLite."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM datos", conn)
    conn.close()

    df.to_excel(EXCEL_PATH, index=False, sheet_name="Muestra de Datos")
    print(f"✅ Archivo Excel generado: {EXCEL_PATH}")

# 🔹 Función para generar el archivo de auditoría
def generar_auditoria(datos_api):
    """Crea un archivo de auditoría comparando API vs base de datos."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM datos")
    registros_db = cursor.fetchone()[0]
    conn.close()

    registros_api = len(datos_api.get("ticker", {}))  # Cantidad de claves en "ticker"

    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write(f"📅 Auditoría de Ingesta - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 50 + "\n")
        f.write(f"🔹 Registros obtenidos del API: {registros_api}\n")
        f.write(f"🔹 Registros almacenados en BD: {registros_db}\n\n")

        # Comparación de datos
        if registros_api != registros_db:
            f.write("⚠️ Advertencia: Diferencias en el número de registros entre API y BD.\n")
        else:
            f.write("✅ No hay diferencias entre el API y la base de datos.")

    print(f"✅ Archivo de auditoría generado: {AUDIT_PATH}")

# 🔹 Función principal
def main():
    """Ejecuta todo el proceso de ingesta de datos."""
    print("🚀 Iniciando proceso de ingesta de datos...")

    url = "https://www.mercadobitcoin.net/api"
    parametros = {"coin": "BTC", "method": "ticker"}

    datos_api = obtener_datos_api(url, parametros)

    if datos_api:
        crear_base_datos()
        insertar_datos(datos_api)
        generar_excel()
        generar_auditoria(datos_api)
        print("✅ Proceso completado con éxito.")
    else:
        print("❌ No se obtuvieron datos del API.")

if __name__ == "__main__":
    main()
