import unittest

from scripts import data_warehouse_builder


class FakeCollection:
    def __init__(self):
        self.calls = []

    def aggregate(self, pipeline, allowDiskUse=False):
        self.calls.append((pipeline, allowDiskUse))
        return []


class DataWarehouseBuilderTests(unittest.TestCase):
    def test_merge_uses_replace_and_insert_semantics(self):
        stage = data_warehouse_builder._merge("fact_visualizaciones")
        self.assertEqual(stage["$merge"]["into"], "fact_visualizaciones")
        self.assertEqual(stage["$merge"]["on"], "_id")
        self.assertEqual(stage["$merge"]["whenMatched"], "replace")
        self.assertEqual(stage["$merge"]["whenNotMatched"], "insert")

    def test_run_enables_disk_use_for_big_data_aggregations(self):
        collection = FakeCollection()
        data_warehouse_builder._run(collection, [{"$group": {"_id": None}}])
        self.assertEqual(collection.calls[0][1], True)

    def test_warehouse_scope_is_limited_to_four_collections(self):
        self.assertEqual(
            data_warehouse_builder.WAREHOUSE_COLLECTIONS,
            ("dim_tiempo", "dim_usuario", "dim_contenido", "fact_visualizaciones"),
        )


if __name__ == "__main__":
    unittest.main()