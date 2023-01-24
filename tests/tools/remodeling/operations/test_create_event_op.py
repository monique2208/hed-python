import os
import json
import pandas as pd
import numpy as np
import unittest
from hed.tools.remodeling.operations.create_event_op import CreateEventOp


class Test(unittest.TestCase):
    """

    """

    @classmethod
    def setUpClass(cls):
        path = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                             '../../../data/remodel_tests/number_columns'))
        cls.valid_data_path = os.path.realpath(os.path.join(path, 'valid.tsv'))
        cls.invalid_continuity_break_path = os.path.realpath(os.path.join(path, 'invalid_continuity_break.tsv'))
        cls.invalid_inner_start_path = os.path.realpath(os.path.join(path, 'invalid_inner_start.tsv'))
        cls.invalid_inner_start2_path = os.path.realpath(os.path.join(path, 'invalid_inner_start2.tsv'))
        cls.invalid_no_inner_restart_path = os.path.realpath(os.path.join(path, 'invalid_no_inner_restart.tsv'))
        cls.invalid_unanchored_inner_path = os.path.realpath(os.path.join(path, 'invalid_unanchored_inner.tsv'))

        cls.parameters_json_path = os.path.realpath(os.path.join(path, 'parameters.json'))

        cls.dispatcher = None
        cls.file_name = None

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_tsv_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)['valid_inner']
        op = CreateEventOp(parms)
        df = pd.read_csv(self.valid_data_path, sep='\t')
        df_test = pd.read_csv(self.valid_data_path, sep='\t')

        df_new = op.do_op(self.dispatcher, df, self.file_name)

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "create_event should not change the input df columns")
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy(), equal_nan=True),
                        "create_event should not change the input df values")

        self.assertTrue(len(df_new) - len(df_test) == 12)

    def test_invalid_unanchored_inner_tsv_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)['valid_inner']
        op = CreateEventOp(parms)
        df = pd.read_csv(self.invalid_unanchored_inner_path, sep='\t')
        df_test = pd.read_csv(self.invalid_unanchored_inner_path, sep='\t')

        with self.assertRaisesRegex(ValueError, "InvalidRemodelerGroup"):
            df_new = op.do_op(self.dispatcher, df, self.file_name)

    def test_invalid_tsv_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)['valid_inner']
        op = CreateEventOp(parms)
        df = pd.read_csv(self.invalid_inner_start_path, sep='\t')
        df_test = pd.read_csv(self.invalid_inner_start_path, sep='\t')

        with self.assertRaisesRegex(ValueError, "InvalidRemodelerGroup"):
            df_new = op.do_op(self.dispatcher, df, self.file_name)

    def test_invalid_tsv_inner_start2_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)["valid_inner"]
        op = CreateEventOp(parms)
        df = pd.read_csv(self.invalid_inner_start2_path, sep='\t')
        df_test = pd.read_csv(self.invalid_inner_start2_path, sep='\t')

        with self.assertRaisesRegex(ValueError, "InvalidRemodelerGroup"):
            df_new = op.do_op(self.dispatcher, df, self.file_name)

    def test_invalid_tsv_no_inner_restart_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)["valid_inner"]
        op = CreateEventOp(parms)
        df = pd.read_csv(self.invalid_no_inner_restart_path, sep='\t')
        df_test = pd.read_csv(self.invalid_no_inner_restart_path, sep='\t')

        with self.assertRaisesRegex(ValueError, "InvalidRemodelerGroup"):
            df_new = op.do_op(self.dispatcher, df, self.file_name)

    def test_invalid_tsv_continuity_break_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)["valid_inner"]
        op = CreateEventOp(parms)
        df = pd.read_csv(self.invalid_continuity_break_path, sep='\t')
        df_test = pd.read_csv(self.invalid_continuity_break_path, sep='\t')

        with self.assertRaisesRegex(ValueError, "InvalidRemodelerGroup"):
            df_new = op.do_op(self.dispatcher, df, self.file_name)



