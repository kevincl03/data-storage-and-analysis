"""Hyperflix Automated Data Discovery and Data Catalog Generator.

Connects dynamically to MongoDB Atlas, reverse-engineers collection schemas,
flattens nested BSON/JSON structures, classifies architectural tiers (OLTP, OLAP, DW),
detects keys and active indexes, and exports a formatted multi-tab Excel workbook.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from bson import ObjectId, Timestamp
from dotenv import load_dotenv
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from pymongo import MongoClient
from pymongo.database import Database

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

LOGGER = logging.getLogger("data_catalog")

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "dashboard_output"

# ==============================================================================
# Hyperflix Streaming Business Semantic Dictionary
# ==============================================================================
DESCRIPCIONES_CAMPOS_HYPERFLIX: dict[str, str] = {
    "_id": "Identificador interno BSON único del documento en MongoDB.",
    "codigo_usuario": "Código de negocio único asignado al suscriptor (ej. USR000001).",
    "nombre": "Nombre completo del suscriptor o denominación oficial del canal/contenido.",
    "email": "Correo electrónico del suscriptor para autenticación y notificaciones de cuenta.",
    "pais": "País de residencia del suscriptor o país de origen de la producción audiovisual.",
    "ciudad": "Ciudad de residencia del suscriptor para análisis regional de audiencia.",
    "fecha_registro": "Fecha y hora UTC en la que el usuario creó su cuenta en Hyperflix.",
    "activo": "Indicador booleano que señala si la cuenta, canal o recurso está habilitado.",
    "preferencias": "Configuraciones personalizadas de reproducción y perfil del suscriptor.",
    "preferencias.idioma": "Idioma preferido del suscriptor para audio y subtítulos en streaming.",
    "preferencias.autoplay": "Preferencia booleana para reproducir automáticamente el siguiente título.",
    "preferencias.clasificacion_maxima": "Restricción de control parental configurada en el perfil (ej. 16+, 18+).",
    "codigo_pelicula": "Identificador de negocio único de la película en el catálogo (ej. MOV000001).",
    "titulo": "Título oficial de la película o serie disponible para streaming.",
    "generos": "Lista de géneros cinematográficos asignados al título (ej. Drama, Ciencia ficción).",
    "anio_estreno": "Año en que fue estrenada originalmente la producción audiovisual.",
    "duracion_minutos": "Duración total en minutos del metraje cinematográfico o episodio.",
    "clasificacion": "Clasificación etaria de audiencia recomendada (ej. Todo público, 13+, 16+, 18+).",
    "pais_origen": "País de origen de la casa productora de la obra audiovisual.",
    "calidad_maxima": "Resolución máxima de transmisión disponible en la plataforma (HD, Full HD, 4K).",
    "disponible": "Indicador booleano que valida si el contenido está publicado y activo.",
    "fecha_publicacion": "Fecha y hora en que la película fue incorporada al catálogo de Hyperflix.",
    "codigo_canal": "Identificador único de señal para canales de televisión en vivo (ej. CHN0001).",
    "categoria": "Categoría temática del canal en vivo (ej. Noticias, Deportes, Infantil).",
    "idioma": "Idioma principal de transmisión de la señal en directo.",
    "fecha_alta": "Fecha de incorporación del canal de televisión a la plataforma.",
    "codigo_suscripcion": "Identificador de negocio del contrato de suscripción (ej. SUB000001).",
    "usuario_id": "Clave foránea que referencia al suscriptor titular de la cuenta o evento.",
    "plan": "Nivel de plan de suscripción contratado (Básico, Estándar o Premium).",
    "precio_mensual": "Tarifa mensual recurrente facturada según el plan contratado.",
    "fecha_inicio": "Fecha y hora de inicio de la vigencia de la suscripción o de la sesión de streaming.",
    "fecha_fin": "Fecha y hora de vencimiento de la suscripción o conclusión del stream.",
    "estado": "Estado operacional de la entidad (ej. Activa, Cancelada, Completada, Abandonada).",
    "renovacion_automatica": "Indicador booleano que define si la suscripción se renueva al expirar.",
    "caracteristicas": "Lista de beneficios y características técnicas contratadas en el plan.",
    "codigo_pago": "Identificador único de la transacción financiera de pago (ej. PAY000001).",
    "suscripcion_id": "Clave foránea que vincula la transacción o reproducción con la suscripción.",
    "fecha_pago": "Marca temporal UTC en que se procesó exitosamente el pago.",
    "monto": "Monto monetario cobrado por la suscripción mensual.",
    "moneda": "Código ISO de la divisa utilizada en el cobro (ej. COP, USD).",
    "metodo_pago": "Medio financiero utilizado por el suscriptor (ej. Tarjeta de crédito, PSE).",
    "codigo_evento": "Identificador de telemetría de la sesión de reproducción (ej. EVT000001).",
    "tipo_contenido": "Discriminador del recurso reproducido ('pelicula' o 'canal_tv').",
    "contenido_id": "Clave foránea que enlaza con el catálogo de películas o canales de Hyperflix.",
    "duracion_vista_minutos": "Minutos efectivos consumidos por el suscriptor en la sesión de streaming.",
    "dispositivo": "Dispositivo o hardware cliente utilizado para el streaming (ej. Smart TV, Móvil, Web).",
    "dispositivo.tipo": "Tipo de hardware utilizado por el suscriptor para ver la transmisión.",
    "dispositivo.os": "Sistema operativo desde donde se originó el stream (ej. Android, tvOS, iOS).",
    "calidad": "Resolución efectiva transmitida durante la sesión de visualización (ej. 4K, Full HD).",
    "minutos_reproducidos": "Métrica fundamental de engagement: total de minutos reproducidos en la sesión.",
    "fecha_id": "Clave foránea de dimensión temporal en formato YYYYMMDD para el modelo estrella.",
    "anio": "Año calendario derivado para análisis temporal y particionamiento.",
    "mes": "Mes calendario (1-12) derivado para análisis de estacionalidad.",
    "dia": "Día del mes derivado de la marca temporal del evento.",
    "hora": "Hora del día (0-23) en que ocurrió la interacción de streaming.",
    "dia_semana": "Día de la semana (1=Domingo..7=Sábado) para análisis de hábitos de audiencia.",
    "nombre_dia": "Nombre textual del día de la semana correspondiente a la fecha analítica.",
    "trimestre": "Trimestre del año (1-4) para análisis financiero y comparativo estacional.",
    "es_fin_de_semana": "Indicador booleano que señala si el evento ocurrió en sábado o domingo.",
    "segmento_usuario": "Perfil analítico del suscriptor según consumo (ej. VIP, Frecuente, Casual).",
    "gasto_total_acumulado": "Métrica financiera acumulada de ingresos generados por el suscriptor.",
    "total_pagos": "Cantidad de pagos completados exitosamente por el suscriptor a lo largo del tiempo.",
    "visualizacion_id": "Identificador de hecho sustituto en la tabla fact_visualizaciones.",
    "content_type": "Discriminador unificado de contenido en la dimensión ('pelicula' o 'canal_tv').",
    "anio_lanzamiento": "Año de estreno consolidado en la dimensión unificada de contenidos.",
    "categoria_contenido": "Categoría o género principal asignado en vistas analíticas.",
    "nombre_contenido": "Denominación unificada del título o señal en vistas analíticas.",
    "pago_completado": "Indicador booleano que certifica que el evento cuenta con pago al día.",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def load_settings() -> tuple[str, str]:
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "").strip()
    db_name = os.getenv("MONGO_DB_NAME", "hyperflix_db").strip()
    if not mongo_uri:
        raise ValueError("Missing required environment variable: MONGO_URI in .env")
    return mongo_uri, db_name


def connect_mongo(mongo_uri: str, db_name: str) -> tuple[MongoClient, Database]:
    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5_000,
        connectTimeoutMS=5_000,
        appname="hyperflix-data-catalog",
    )
    client.admin.command("ping")
    return client, client[db_name]


def classify_collection_architecture(collection_name: str) -> str:
    """Classifies a MongoDB collection into its architectural layer."""
    name_lower = collection_name.lower()
    if name_lower.startswith("dim_") or name_lower.startswith("fact_"):
        return "Data Warehouse (Modelo Estrella)"
    if name_lower.endswith("_analitica") or name_lower.endswith("_analitico") or "analitica" in name_lower:
        return "OLAP (Analítico)"
    return "OLTP (Transaccional)"


def translate_bson_type(value: Any) -> str:
    """Translates a Python/BSON value type into standard catalog terminology."""
    if isinstance(value, ObjectId):
        return "String (UUID)"
    if isinstance(value, (datetime, Timestamp, date)):
        return "Timestamp"
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Double"
    if isinstance(value, list):
        return "Array"
    if isinstance(value, str):
        return "String"
    if isinstance(value, dict):
        return "Document (Subdocumento)"
    if value is None:
        return "Null / Desconocido"
    return type(value).__name__.capitalize()


def flatten_document(document: dict[str, Any] | None, prefix: str = "") -> dict[str, Any]:
    """Recursively flattens nested BSON/JSON dictionaries into dot-separated paths."""
    if not document:
        return {}

    items: dict[str, Any] = {}
    for key, value in document.items():
        field_name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict) and value:
            nested = flatten_document(value, prefix=field_name)
            items.update(nested)
        else:
            items[field_name] = value
    return items


def is_primary_key(field_name: str) -> str:
    return "Sí" if field_name == "_id" else "No"


def is_foreign_key(field_name: str) -> str:
    if field_name == "_id":
        return "No"
    # Fields ending in _id or containing .id in flattened structures
    leaf = field_name.split(".")[-1]
    return "Sí" if leaf.endswith("_id") or leaf == "id" else "No"


def is_indexed_field(field_name: str, index_keys: set[str]) -> str:
    """Determines if a field or its prefix is indexed based on index_information."""
    if field_name in index_keys:
        return "Sí"
    # Also check if top-level field is indexed
    root = field_name.split(".")[0]
    return "Sí" if root in index_keys else "No"


def get_field_business_description(field_name: str) -> str:
    """Retrieves business description from Hyperflix dictionary or returns default."""
    if field_name in DESCRIPCIONES_CAMPOS_HYPERFLIX:
        return DESCRIPCIONES_CAMPOS_HYPERFLIX[field_name]

    leaf = field_name.split(".")[-1]
    if leaf in DESCRIPCIONES_CAMPOS_HYPERFLIX:
        return DESCRIPCIONES_CAMPOS_HYPERFLIX[leaf]

    return "Campo operacional / analítico de la plataforma Hyperflix."


def extract_collection_indexes(collection: Any) -> set[str]:
    """Extracts indexed field names from collection index_information."""
    indexed_fields: set[str] = set()
    try:
        indexes = collection.index_information()
        for info in indexes.values():
            for key_tuple in info.get("key", []):
                indexed_fields.add(key_tuple[0])
    except Exception as error:
        LOGGER.warning("Could not read index information for %s: %s", collection.name, error)
    return indexed_fields


def inspect_database_schema(db: Database) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Inspects all collections in MongoDB and builds architecture and data dictionary tables."""
    collection_names = sorted(db.list_collection_names())
    LOGGER.info("Discovered %d collections in database '%s'", len(collection_names), db.name)

    resumen_rows: list[dict[str, Any]] = []
    diccionario_rows: list[dict[str, Any]] = []

    for name in collection_names:
        collection = db[name]
        architecture_zone = classify_collection_architecture(name)

        # 1. Total records and estimated size
        try:
            total_docs = collection.estimated_document_count()
        except Exception:
            total_docs = collection.count_documents({})

        estimated_size_bytes = 0
        try:
            stats = db.command("collStats", name)
            estimated_size_bytes = stats.get("size", 0)
        except Exception:
            # Fallback estimation: docs * 350 bytes average
            estimated_size_bytes = total_docs * 350

        if estimated_size_bytes >= 1_048_576:
            size_formatted = f"{estimated_size_bytes / 1_048_576:.2f} MB"
        elif estimated_size_bytes >= 1_024:
            size_formatted = f"{estimated_size_bytes / 1_024:.2f} KB"
        else:
            size_formatted = f"{estimated_size_bytes} Bytes"

        resumen_rows.append({
            "Colección": name,
            "Zona Arquitectónica (OLTP/OLAP/DW)": architecture_zone,
            "Total Registros (Docs)": total_docs,
            "Tamaño Estimado": size_formatted,
        })

        # 2. Extract sample document, flatten, and inspect keys/indexes
        sample_doc = collection.find_one()
        indexed_fields = extract_collection_indexes(collection)

        if sample_doc:
            flattened = flatten_document(sample_doc)
            for field_name, value in flattened.items():
                data_type = translate_bson_type(value)
                pk = is_primary_key(field_name)
                fk = is_foreign_key(field_name)
                indexed = is_indexed_field(field_name, indexed_fields)
                description = get_field_business_description(field_name)

                diccionario_rows.append({
                    "Colección": name,
                    "Campo": field_name,
                    "Tipo de Dato": data_type,
                    "Es Primary Key (Sí/No)": pk,
                    "Es Foreign Key (Sí/No)": fk,
                    "Está Indexado (Sí/No)": indexed,
                    "Descripción de Negocio": description,
                })
        else:
            # Collection is empty: include standard _id as minimal schema definition
            diccionario_rows.append({
                "Colección": name,
                "Campo": "_id",
                "Tipo de Dato": "String (UUID)",
                "Es Primary Key (Sí/No)": "Sí",
                "Es Foreign Key (Sí/No)": "No",
                "Está Indexado (Sí/No)": "Sí",
                "Descripción de Negocio": DESCRIPCIONES_CAMPOS_HYPERFLIX["_id"],
            })

    df_resumen = pd.DataFrame(resumen_rows)
    df_diccionario = pd.DataFrame(diccionario_rows)
    return df_resumen, df_diccionario


def style_excel_sheet(worksheet: Any, df: pd.DataFrame, header_bg_color: str = "1F4E79") -> None:
    """Applies professional enterprise styling to an openpyxl worksheet."""
    header_fill = PatternFill(start_color=header_bg_color, end_color=header_bg_color, fill_type="solid")
    header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    cell_font = Font(name="Calibri", size=10)
    thin_border = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"),
    )
    center_align = Alignment(horizontal="center", vertical="center")
    left_align = Alignment(horizontal="left", vertical="center")

    # Format header row
    for col_idx in range(1, len(df.columns) + 1):
        cell = worksheet.cell(row=1, column=col_idx)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center_align

    # Format data rows
    for row_idx in range(2, len(df) + 2):
        for col_idx in range(1, len(df.columns) + 1):
            cell = worksheet.cell(row=row_idx, column=col_idx)
            cell.font = cell_font
            cell.border = thin_border
            # Center indicators and IDs, left-align descriptions and names
            val_str = str(cell.value or "")
            if val_str in ("Sí", "No") or df.columns[col_idx - 1] in ("Zona Arquitectónica (OLTP/OLAP/DW)", "Total Registros (Docs)"):
                cell.alignment = center_align
            else:
                cell.alignment = left_align

    # Auto-adjust column widths
    for col_idx in range(1, len(df.columns) + 1):
        col_letter = get_column_letter(col_idx)
        max_len = max(len(str(worksheet.cell(row=r, column=col_idx).value or "")) for r in range(1, len(df) + 2))
        worksheet.column_dimensions[col_letter].width = min(max(max_len + 4, 14), 75)


def export_catalog_to_excel(
    df_resumen: pd.DataFrame,
    df_diccionario: pd.DataFrame,
    output_path: Path,
) -> Path:
    """Exports both DataFrames to a styled multi-tab Excel spreadsheet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_resumen.to_excel(writer, sheet_name="Resumen Arquitectónico", index=False)
        df_diccionario.to_excel(writer, sheet_name="Diccionario de Datos", index=False)

        workbook = writer.book
        style_excel_sheet(workbook["Resumen Arquitectónico"], df_resumen, header_bg_color="1F4E79")
        style_excel_sheet(workbook["Diccionario de Datos"], df_diccionario, header_bg_color="2E4057")

    LOGGER.info("Excel Catalog successfully written to: %s", output_path)
    return output_path


def safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "ascii"
        clean_text = text.encode(encoding, errors="replace").decode(encoding)
        print(clean_text)


def print_academic_justification(output_path: Path, total_collections: int, total_fields: int) -> None:
    """Prints the completion banner and formal academic justification quote."""
    banner = f"""
================================================================================
[EXITO] DATA DISCOVERY Y CATALOGO DE DATOS GENERADO EXITOSAMENTE
================================================================================
* Archivo generado: {output_path}
* Colecciones auditadas: {total_collections}
* Atributos / Campos catalogados: {total_fields}

JUSTIFICACION ACADEMICA Y DE GOBIERNO DE DATOS:
"En una arquitectura de datos moderna, el 'Data Discovery' es tan critico como
el almacenamiento. Para garantizar el gobierno de los datos de Hyperflix y
facilitar el onboarding de nuevos analistas o cientificos de datos, se implemento
un script de generacion automatizada de catalogos capaz de realizar ingenieria
inversa, aplanamiento de subdocumentos BSON e inspeccion dinamica de indices
directamente desde MongoDB Atlas."
================================================================================
"""
    safe_print(banner)


def generate_data_catalog(output_dir: Path | None = None) -> Path:
    """Main orchestrator function for Data Discovery and Catalog generation."""
    configure_logging()
    target_dir = output_dir or DEFAULT_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    output_file = target_dir / f"Catalogo_Datos_Hyperflix_{today_str}.xlsx"

    mongo_uri, db_name = load_settings()
    LOGGER.info("Connecting to MongoDB Atlas database '%s'...", db_name)
    client, db = connect_mongo(mongo_uri, db_name)

    try:
        df_resumen, df_diccionario = inspect_database_schema(db)
        export_catalog_to_excel(df_resumen, df_diccionario, output_file)
        print_academic_justification(output_file, len(df_resumen), len(df_diccionario))
        return output_file
    finally:
        client.close()
        LOGGER.info("Database connection closed.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hyperflix Automated Data Discovery & Data Catalog Generator"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to save the generated Excel catalog (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    try:
        generate_data_catalog(output_dir=args.output_dir)
    except Exception as error:
        LOGGER.error("Data catalog generation failed: %s", error)
        sys.exit(1)


if __name__ == "__main__":
    main()
