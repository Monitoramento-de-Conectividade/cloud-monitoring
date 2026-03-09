import unittest

from backend.cloudv2_dashboard import _normalize_bulk_pivot_ids


class BulkPivotActionHelpersTests(unittest.TestCase):
    def test_normalize_bulk_pivot_ids_strips_and_dedupes(self):
        self.assertEqual(
            _normalize_bulk_pivot_ids([" PivotA ", "", "PivotB", "PivotA", None]),
            ["PivotA", "PivotB"],
        )

    def test_normalize_bulk_pivot_ids_requires_non_empty_list(self):
        with self.assertRaisesRegex(ValueError, "pivot_ids obrigatorio"):
            _normalize_bulk_pivot_ids([])

    def test_normalize_bulk_pivot_ids_enforces_limit(self):
        with self.assertRaisesRegex(ValueError, "maximo de 2 pivot_ids por requisicao"):
            _normalize_bulk_pivot_ids(["PivotA", "PivotB", "PivotC"], limit=2)


if __name__ == "__main__":
    unittest.main()
