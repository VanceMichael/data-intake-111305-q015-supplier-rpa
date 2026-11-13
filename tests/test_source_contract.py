import hashlib
import json
import unittest

from supplier_rpa.client import Client
from supplier_rpa.decoding import parse_page, records as decode_records
from supplier_rpa.origin import load_scenario, serve


class SourceContractTests(unittest.TestCase):
    def test_domain_observations_preserve_source_distinctions(self):
        scenario = load_scenario()
        records = scenario["source_records"]
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            self.assertEqual(records[0]["order"], records[2]["order"])
            self.assertNotEqual(records[0]["row"], records[2]["row"])
            self.assertNotEqual(records[0]["quantity"], records[2]["quantity"])
            forms = parse_page(client.fetch("/feed").body).forms
            self.assertEqual(forms[0]["fields"]["id"], records[0]["order"])

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
