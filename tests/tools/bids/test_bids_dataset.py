import os
import unittest
import pandas as pd
from hed.schema.hed_schema_io import load_schema, load_schema_version
from hed.schema.hed_schema_group import HedSchemaGroup
from hed.tools.bids.bids_dataset import BidsDataset
from hed.tools.bids.bids_file_group import BidsFileGroup


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.root_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../data/bids/eeg_ds003654s_hed')
        cls.library_path = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                        '../../data/bids/eeg_ds003654s_hed_library'))

    def test_constructor(self):
        bids1 = BidsDataset(Test.root_path)
        self.assertIsInstance(bids1, BidsDataset, "BidsDataset should create a valid object from valid dataset")
        self.assertIsInstance(bids1.participants, pd.DataFrame, "BidsDataset participants should be a DataFrame")
        self.assertIsInstance(bids1.dataset_description, dict, "BidsDataset dataset_description should be a dict")
        self.assertIsInstance(bids1.event_files, BidsFileGroup, "BidsDataset event_files should be  BidsFileGroup")
        self.assertIsInstance(bids1.schemas, HedSchemaGroup, "BidsDataset schemas should be HedSchemaGroup")

        bids2 = BidsDataset(self.library_path)
        self.assertIsInstance(bids2, BidsDataset,
                              "BidsDataset with libraries should create a valid object from valid dataset")
        self.assertIsInstance(bids2.participants, pd.DataFrame,
                              "BidsDataset with libraries should have a participants that is a DataFrame")
        self.assertIsInstance(bids2.dataset_description, dict,
                              "BidsDataset with libraries dataset_description should be a dict")
        self.assertIsInstance(bids2.event_files, BidsFileGroup,
                              "BidsDataset with libraries event_files should be  BidsFileGroup")
        self.assertIsInstance(bids2.schemas, HedSchemaGroup,
                              "BidsDataset with libraries should have schemas that is a HedSchemaGroup")

    def test_constructor_with_schema_group(self):

        base_version = '8.0.0'
        library1_url = "https://raw.githubusercontent.com/hed-standard/hed-schema-library/main/" + \
                       "library_schemas/score/hedxml/HED_score_0.0.1.xml"
        library2_url = "https://raw.githubusercontent.com/hed-standard/hed-schema-library/main/" + \
                       "library_schemas/testlib/hedxml/HED_testlib_1.0.2.xml"
        schema_list = [load_schema_version(xml_version=base_version)]
        schema_list.append(load_schema(library1_url, library_prefix="sc"))
        schema_list.append(load_schema(library2_url, library_prefix="test"))
        bids1 = BidsDataset(self.library_path, schema_group=HedSchemaGroup(schema_list))
        self.assertIsInstance(bids1, BidsDataset,
                              "BidsDataset with libraries should create a valid object from valid dataset")
        self.assertIsInstance(bids1.participants, pd.DataFrame,
                              "BidsDataset with libraries should have a participants that is a DataFrame")
        self.assertIsInstance(bids1.dataset_description, dict,
                              "BidsDataset with libraries dataset_description should be a dict")
        self.assertIsInstance(bids1.event_files, BidsFileGroup,
                              "BidsDataset with libraries event_files should be  BidsFileGroup")
        self.assertIsInstance(bids1.schemas, HedSchemaGroup,
                              "BidsDataset with libraries should have schemas that is a HedSchemaGroup")

    def test_validator(self):
        bids1 = BidsDataset(self.root_path)
        self.assertIsInstance(bids1, BidsDataset, "BidsDataset should create a valid object from valid dataset")
        issues = bids1.validate()
        self.assertTrue(issues, "BidsDataset validate should return issues when the default check_for_warnings is used")
        issues = bids1.validate(check_for_warnings=True)
        self.assertTrue(issues,
                        "BidsDataset validate should return issues when check_for_warnings is True")
        issues = bids1.validate(check_for_warnings=False)
        self.assertFalse(issues, "BidsDataset validate should return no issues when check_for_warnings is False")

        bids2 = BidsDataset(self.library_path)
        self.assertIsInstance(bids2, BidsDataset,
                              "BidsDataset with libraries should create a valid object from valid dataset")
        issues = bids2.validate()
        self.assertTrue(issues,
                        "BidsDataset with libraries should return issues when the default check_for_warnings is used")
        issues = bids2.validate(check_for_warnings=True)
        self.assertTrue(issues,
                        "BidsDataset with libraries should return issues when check_for_warnings is True")
        issues = bids2.validate(check_for_warnings=False)
        self.assertFalse(issues,
                         "BidsDataset with libraries should return no issues when check_for_warnings is False")

    def test_get_summary(self):
        bids1 = BidsDataset(self.root_path)
        summary1 = bids1.get_summary()
        self.assertIsInstance(summary1, dict, "BidsDataset summary is a dictionary")
        self.assertTrue("hed_schema_versions" in summary1, "BidsDataset summary has a hed_schema_versions key")
        self.assertIsInstance(summary1["hed_schema_versions"], list,
                              "BidsDataset summary hed_schema_versions is a list")
        self.assertTrue("dataset" in summary1)
        self.assertEqual(len(summary1["hed_schema_versions"]), 1,
                         "BidsDataset summary hed_schema_versions entry has one schema")
        bids2 = BidsDataset(self.library_path)
        summary2 = bids2.get_summary()
        self.assertIsInstance(summary2, dict, "BidsDataset with libraries has a summary that is a dictionary")
        self.assertTrue("hed_schema_versions" in summary2,
                        "BidsDataset with libraries has a summary with a hed_schema_versions key")
        self.assertIsInstance(summary2["hed_schema_versions"], list,
                              "BidsDataset with libraries hed_schema_versions in summary is a list")
        self.assertEqual(len(summary2["hed_schema_versions"]), 3,
                         "BidsDataset with libraries summary hed_schema_versions list has 3 schemas")
        self.assertTrue("dataset" in summary2)


if __name__ == '__main__':
    unittest.main()
