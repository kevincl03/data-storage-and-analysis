"""Pruebas unitarias del pipeline ETL de Hyperflix (MongoDB -> BigQuery).

Las pruebas no contactan ni Atlas ni BigQuery: validan la capa de
transformacion, el tipado de datos y la configuracion del pipeline.
"""

import unittest
from datetime import date, datetime, timezone
from unittest import mock

import pandas as pd
from bson import ObjectId
from google.cloud.bigquery import SchemaField

from scripts import etl_mongo_to_bigquery as etl

USUARIO_ID = ObjectId("6aa1cb0448539af82128d325")
CONTENIDO_ID = ObjectId("6aa1cb0448539af82128d326")
SUSCRIPCION_ID = ObjectId("6aa1cb0448539af82128d327")
VISUALIZACION_ID = ObjectId("6aa1cb0548539af82128d329")

# Documento real de `dim_usuario`: los pagos llegan envueltos en el documento
# literal {"ifNull": [valor, defecto]} por el builder del Data Warehouse.
SAMPLE_USUARIO = {
    "_id": USUARIO_ID,
    "usuario_id": USUARIO_ID,
    "codigo_usuario": "USR000001",
    "nombre": "Juan Perez",
    "email": "juan.perez@hyperflix.example",
    "pais": "Colombia",
    "ciudad": "Bogota",
    "activo": True,
    "fecha_registro": datetime(2026, 9, 9, 16, 9, 24, 92000, tzinfo=timezone.utc),
    "total_pagado": {"ifNull": [135500, 0]},
    "pagos_completados": {"ifNull": [5, 0]},
}

SAMPLE_PELICULA = {
    "_id": CONTENIDO_ID,
    "contenido_id": CONTENIDO_ID,
    "content_type": "pelicula",
    "titulo": "Horizonte Digital",
    "generos": ["Ciencia ficcion", "Drama"],
    "categoria": None,
    "anio_lanzamiento": 2025,
    "duracion_minutos": 118,
    "disponible": True,
}

SAMPLE_CANAL = {
    "_id": ObjectId("6aa1cbc0c5d3aa0fb4960854"),
    "contenido_id": ObjectId("6aa1cbc0c5d3aa0fb4960854"),
    "content_type": "canal_tv",
    "titulo": "Hyper Channel 001",
    "generos": [],
    "categoria": "Musica",
    "anio_lanzamiento": None,
    "duracion_minutos": None,
    "disponible": True,
}

SAMPLE_FECHA = {
    "_id": "20260810",
    "fecha_id": "20260810",
    "fecha": datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc),
    "anio": 2026,
    "mes": 8,
    "dia": 10,
    "dia_semana": 2,
    "nombre_dia": "lunes",
    "trimestre": 3.0,
    "es_fin_de_semana": False,
}

SAMPLE_VISUALIZACION = {
    "_id": VISUALIZACION_ID,
    "visualizacion_id": VISUALIZACION_ID,
    "usuario_id": USUARIO_ID,
    "contenido_id": CONTENIDO_ID,
    "suscripcion_id": SUSCRIPCION_ID,
    "fecha_id": "20260810",
    "minutos_reproducidos": 42,
}


def _frame(documents):
    return pd.DataFrame.from_records(documents)


class FakeCollection:
    def __init__(self, documents):
        self.documents = documents

    def find(self, batch_size=None):
        return iter(self.documents)


class FakeDatabase:
    def __init__(self, collections):
        self.collections = collections

    def list_collection_names(self):
        return list(self.collections)

    def __getitem__(self, name):
        return FakeCollection(self.collections.get(name, []))


class FakeMongoClient:
    def __init__(self, collections):
        self.collections = collections
        self.closed = False

    def __getitem__(self, name):
        return FakeDatabase(self.collections)

    def close(self):
        self.closed = True


class TransformDimUsuarioTests(unittest.TestCase):
    """Reglas de transformacion de la dimension de suscriptores."""

    def setUp(self):
        self.data = etl.transform_dim_usuario(_frame([SAMPLE_USUARIO]))

    def test_unwraps_literal_ifnull_document(self):
        """El builder guarda {"ifNull": [v, 0]}; el ETL debe desempaquetarlo."""
        self.assertEqual(int(self.data.loc[0, "total_pagado"]), 135500)
        self.assertEqual(int(self.data.loc[0, "pagos_completados"]), 5)

    def test_payment_metrics_use_nullable_integer_dtype(self):
        self.assertEqual(str(self.data["total_pagado"].dtype), "Int64")
        self.assertEqual(str(self.data["pagos_completados"].dtype), "Int64")

    def test_objectid_is_converted_to_string(self):
        self.assertEqual(self.data.loc[0, "usuario_id"], str(USUARIO_ID))
        self.assertIsInstance(self.data.loc[0, "usuario_id"], str)

    def test_boolean_and_timestamp_are_typed(self):
        self.assertEqual(str(self.data["activo"].dtype), "boolean")
        self.assertTrue(bool(self.data.loc[0, "activo"]))
        self.assertEqual(str(self.data["fecha_registro"].dtype), "datetime64[ns, UTC]")

    def test_nulls_are_preserved_as_nulls(self):
        data = etl.transform_dim_usuario(
            _frame([{**SAMPLE_USUARIO, "ciudad": None, "total_pagado": None}])
        )
        self.assertIsNone(data.loc[0, "ciudad"])
        self.assertTrue(pd.isna(data.loc[0, "total_pagado"]))


class TransformDimContenidoTests(unittest.TestCase):
    """Catalogo unificado de peliculas y canales de TV."""

    def setUp(self):
        self.data = etl.transform_dim_contenido(_frame([SAMPLE_PELICULA, SAMPLE_CANAL]))

    def test_generos_becomes_list_of_strings(self):
        self.assertEqual(self.data.loc[0, "generos"], ["Ciencia ficcion", "Drama"])
        self.assertEqual(self.data.loc[1, "generos"], [])

    def test_generos_is_never_null(self):
        data = etl.transform_dim_contenido(_frame([{**SAMPLE_PELICULA, "generos": None}]))
        self.assertEqual(data.loc[0, "generos"], [])

    def test_channel_rows_keep_null_numeric_attributes(self):
        self.assertTrue(pd.isna(self.data.loc[1, "anio_lanzamiento"]))
        self.assertTrue(pd.isna(self.data.loc[1, "duracion_minutos"]))
        self.assertEqual(self.data.loc[1, "categoria"], "Musica")

    def test_movie_categoria_stays_null(self):
        self.assertIsNone(self.data.loc[0, "categoria"])
        self.assertEqual(int(self.data.loc[0, "anio_lanzamiento"]), 2025)


class TransformDimTiempoTests(unittest.TestCase):
    """Calendario UTC de fechas de reproduccion."""

    def setUp(self):
        self.data = etl.transform_dim_tiempo(_frame([SAMPLE_FECHA]))

    def test_trimestre_float_is_cast_to_integer(self):
        self.assertEqual(str(self.data["trimestre"].dtype), "Int64")
        self.assertEqual(int(self.data.loc[0, "trimestre"]), 3)

    def test_business_key_is_taken_from_id_when_fecha_id_is_missing(self):
        data = etl.transform_dim_tiempo(
            _frame([{key: value for key, value in SAMPLE_FECHA.items() if key != "fecha_id"}])
        )
        self.assertEqual(data.loc[0, "fecha_id"], "20260810")

    def test_fecha_is_utc_aware(self):
        self.assertEqual(str(self.data["fecha"].dtype), "datetime64[ns, UTC]")


class TransformFactTests(unittest.TestCase):
    """Tabla de hechos de reproducciones."""

    def setUp(self):
        self.lookup = {"20260810": date(2026, 8, 10)}
        self.data = etl.transform_fact_visualizaciones(
            _frame([SAMPLE_VISUALIZACION]), self.lookup
        )

    def test_measure_is_nullable_integer(self):
        self.assertEqual(str(self.data["minutos_reproducidos"].dtype), "Int64")
        self.assertEqual(int(self.data.loc[0, "minutos_reproducidos"]), 42)

    def test_foreign_keys_are_strings(self):
        self.assertEqual(self.data.loc[0, "usuario_id"], str(USUARIO_ID))
        self.assertEqual(self.data.loc[0, "contenido_id"], str(CONTENIDO_ID))
        self.assertEqual(self.data.loc[0, "suscripcion_id"], str(SUSCRIPCION_ID))

    def test_fecha_is_derived_from_dim_tiempo(self):
        self.assertEqual(self.data.loc[0, "fecha"], date(2026, 8, 10))

    def test_fact_keeps_only_keys_and_measure(self):
        self.assertEqual(
            list(self.data.columns),
            [
                "visualizacion_id",
                "usuario_id",
                "contenido_id",
                "suscripcion_id",
                "fecha_id",
                "minutos_reproducidos",
                "fecha",
            ],
        )


class BigQuerySchemaTests(unittest.TestCase):
    """El esquema BigQuery debe coincidir exactamente con el DataFrame."""

    def test_schema_columns_match_transformed_dataframe(self):
        samples = {
            "dim_usuario": _frame([SAMPLE_USUARIO]),
            "dim_contenido": _frame([SAMPLE_PELICULA, SAMPLE_CANAL]),
            "dim_tiempo": _frame([SAMPLE_FECHA]),
            "fact_visualizaciones": _frame([SAMPLE_VISUALIZACION]),
        }
        lookup = etl.build_fecha_lookup(etl.transform_dim_tiempo(samples["dim_tiempo"]))
        for table, frame in samples.items():
            with self.subTest(tabla=table):
                data = etl.transform_table(table, frame, lookup)
                expected = [field.name for field in etl.BQ_SCHEMAS[table]]
                self.assertEqual(list(data.columns), expected)

    def test_primary_key_is_required_in_every_table(self):
        for table, key in etl.PRIMARY_KEYS.items():
            with self.subTest(tabla=table):
                fields = {field.name: field for field in etl.BQ_SCHEMAS[table]}
                self.assertIn(key, fields)
                self.assertEqual(fields[key].mode, "REQUIRED")

    def test_generos_is_declared_as_repeated_string(self):
        fields = {field.name: field for field in etl.BQ_SCHEMAS["dim_contenido"]}
        self.assertEqual(fields["generos"].field_type, "STRING")
        self.assertEqual(fields["generos"].mode, "REPEATED")

    def test_fact_is_partitioned_by_fecha(self):
        self.assertIn("fecha", {field.name for field in etl.BQ_SCHEMAS["fact_visualizaciones"]})

    def test_desired_partitioning_targets_fecha_for_the_fact(self):
        partitioning = etl._desired_partitioning("fact_visualizaciones", enabled=True)
        self.assertIsNotNone(partitioning)
        self.assertEqual(partitioning.field, "fecha")
        self.assertEqual(partitioning.type_, "DAY")

    def test_partitioning_is_disabled_by_default(self):
        """BigQuery sandbox expira particiones a 60 dias: no se particiona por defecto."""
        self.assertIsNone(etl._desired_partitioning("fact_visualizaciones"))

    def test_dimensions_are_not_partitioned(self):
        for table in ("dim_usuario", "dim_contenido", "dim_tiempo"):
            with self.subTest(tabla=table):
                self.assertIsNone(etl._desired_partitioning(table, enabled=True))

    def test_layout_alignment_failure_does_not_break_the_pipeline(self):
        """BigQuery sandbox forbids PATCH: the ETL must warn, not fail."""
        from google.cloud import bigquery

        class ForbiddenClient:
            def update_table(self, table, fields):
                raise etl.GoogleAPIError("403 Billing has not been enabled for this project.")

        table = bigquery.Table(
            bigquery.DatasetReference("proyecto", "dataset").table("fact_visualizaciones"),
            schema=[SchemaField("fecha", "DATE")],
        )
        aligned = etl._align_table_layout(
            ForbiddenClient(), table, "fact_visualizaciones", partitioning_enabled=True
        )
        self.assertIsNotNone(aligned.time_partitioning)
        self.assertEqual(aligned.time_partitioning.field, "fecha")

    def test_pipeline_covers_the_four_star_schema_tables(self):
        self.assertEqual(
            set(etl.WAREHOUSE_TABLES),
            {"dim_usuario", "dim_contenido", "dim_tiempo", "fact_visualizaciones"},
        )


class SettingsTests(unittest.TestCase):
    """Resolucion de la configuracion desde el entorno."""

    def test_missing_credentials_file_raises_clear_error(self):
        with mock.patch.dict(etl.os.environ, {
            "MONGO_URI": "mongodb+srv://user:pass@cluster.example.mongodb.net/",
            "GOOGLE_APPLICATION_CREDENTIALS": "./credenciales_inexistente.json",
        }, clear=True), mock.patch.object(etl, "load_dotenv"):
            with self.assertRaises(etl.EtlError) as context:
                etl.load_settings()
        self.assertIn("credenciales", str(context.exception).lower())

    def test_default_write_disposition_is_idempotent_truncate(self):
        self.assertEqual("WRITE_TRUNCATE", etl.VALID_DISPOSITIONS[0])
        self.assertIn("WRITE_TRUNCATE", etl.VALID_DISPOSITIONS)
        self.assertIn("WRITE_APPEND", etl.VALID_DISPOSITIONS)

    def test_unknown_table_is_rejected_by_the_cli(self):
        settings = etl.Settings(
            mongo_uri="mongodb+srv://user:pass@cluster.example.mongodb.net/",
            mongo_db_name="hyperflix_db",
            credentials_path=etl.BASE_DIR / "credenciales_hyperflix_bigquery.json",
            bigquery_project_id="hyperflix-512315",
            bigquery_dataset_id="hyperflix_dw",
            bigquery_location="US",
            batch_size=100,
            write_disposition="WRITE_TRUNCATE",
        )
        with mock.patch.object(etl, "load_settings", return_value=settings), \
                mock.patch.object(etl, "run_pipeline") as pipeline:
            exit_code = etl.main(["--only", "tabla_inexistente"])
        pipeline.assert_not_called()
        self.assertEqual(exit_code, 1)


class DryRunTests(unittest.TestCase):
    """El modo --dry-run no debe escribir nada en BigQuery."""

    def setUp(self):
        self.collections = {
            "dim_usuario": [SAMPLE_USUARIO],
            "dim_contenido": [SAMPLE_PELICULA, SAMPLE_CANAL],
            "dim_tiempo": [SAMPLE_FECHA],
            "fact_visualizaciones": [SAMPLE_VISUALIZACION],
        }
        self.settings = etl.Settings(
            mongo_uri="mongodb+srv://user:pass@cluster.example.mongodb.net/",
            mongo_db_name="hyperflix_db",
            credentials_path=etl.BASE_DIR / "credenciales_hyperflix_bigquery.json",
            bigquery_project_id="hyperflix-512315",
            bigquery_dataset_id="hyperflix_dw",
            bigquery_location="US",
            batch_size=100,
            write_disposition="WRITE_TRUNCATE",
        )

    def test_dry_run_extracts_and_transforms_without_touching_bigquery(self):
        def fail(*args, **kwargs):
            raise AssertionError("BigQuery no debe contactarse en modo --dry-run.")

        with mock.patch.object(etl, "get_mongo_client", return_value=FakeMongoClient(self.collections)), \
                mock.patch.object(etl, "get_bigquery_client", side_effect=fail):
            summary = etl.run_pipeline(self.settings, dry_run=True)

        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["tables"]["dim_usuario"], 1)
        self.assertEqual(summary["tables"]["dim_contenido"], 2)
        self.assertEqual(summary["tables"]["dim_tiempo"], 1)
        self.assertEqual(summary["tables"]["fact_visualizaciones"], 1)

    def test_missing_warehouse_collections_raise_a_clear_error(self):
        with mock.patch.object(etl, "get_mongo_client", return_value=FakeMongoClient({})):
            with self.assertRaises(etl.EtlError) as context:
                etl.run_pipeline(self.settings, dry_run=True)
        self.assertIn("data_warehouse_builder", str(context.exception))


if __name__ == "__main__":
    unittest.main()
