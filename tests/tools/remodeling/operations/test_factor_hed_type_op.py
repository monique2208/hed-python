import os
import json
import unittest
from hed.tools.remodeling.operations.factor_hed_type_op import FactorHedTypeOp
from hed.tools.remodeling.operations.dispatcher import Dispatcher


class Test(unittest.TestCase):
    """

    TODO: Test when no factor names and values are given.

    """
    @classmethod
    def setUpClass(cls):
        path = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                             '../../../data/remodeling/'))
        cls.data_path = os.path.realpath(os.path.join(path, 'sub-002_task-FacePerception_run-1_events.tsv'))
        cls.json_path = os.path.realpath(os.path.join(path, 'task-FacePerception_events.json'))
        base_parameters = {
            "type_tag": "Condition-variable",
            "type_values": [],
            "overwrite_existing": False,
            "factor_encoding": "one-hot"
        }
        cls.json_parms = json.dumps(base_parameters)
        cls.dispatch = Dispatcher([], hed_versions=['8.1.0'])

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid(self):
        # Test correct when all valid and no unwanted information
        parms = json.loads(self.json_parms)
        op = FactorHedTypeOp(parms)
        df_new = op.do_op(self.dispatch, self.data_path, 'subj2_run1', sidecar=self.json_path)
        self.assertEqual(len(df_new), 200, "factor_hed_type_op length is correct")
        self.assertEqual(len(df_new.columns), 20,  "factor_hed_type_op has correct number of columns")

    # def test_valid_specific_column(self):
    #     # Not implemented yet
    #     # Test correct when all valid and no unwanted information
    #     parms = json.loads(self.json_parms)
    #     parms["type_values"] = ["key-assignment"]
    #     op = FactorHedTypeOp(parms)
    #     df_new = op.do_op(self.data_path, hed_schema=self.hed_schema, sidecar=self.json_path)
    #     self.assertEqual(len(df_new), 200, "factor_hed_type_op length is correct when type_values specified")
    #     self.assertEqual(len(df_new.columns), 12,
    #                      "factor_hed_type_op has correct number of columns when type_values specified")

    def test_valid_categorical(self):
        # Test correct when all valid and no unwanted information
        parms = json.loads(self.json_parms)
        parms["factor_encoding"] = "categorical"
        op = FactorHedTypeOp(parms)
        df_new = op.do_op(self.dispatch, self.data_path, 'subj2_run1', sidecar=self.json_path)
        self.assertEqual(len(df_new), 200, "factor_hed_type_op length is correct when categorical encoding")
        self.assertEqual(len(df_new.columns), 13,
                         "factor_hed_type_op has correct number of columns when categorical encoding")


if __name__ == '__main__':
    unittest.main()