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
                                             '../../../data/remodel_tests/'))
        cls.numbered_event_data = os.path.realpath(os.path.join(path, 'events_numbered.tsv'))

        cls.parameters_json_path = os.path.realpath(os.path.join(path, 'create_event_param.json'))

        cls.dispatcher = None
        cls.file_name = None

    @classmethod
    def tearDownClass(cls):
        pass

    def test_valid_tsv_valid_json_double_nested_inner(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)['valid_simple']
        op = CreateEventOp(parms)
        df = pd.read_csv(self.numbered_event_data, sep='\t')
        df_test = pd.read_csv(self.numbered_event_data, sep='\t')

        df_new = op.do_op(self.dispatcher, df, self.file_name)

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "create_event should not change the input df columns")    
        self.assertTrue(df.equals(df_test),
                        "create_event should not change the input df values")
        
        # Test whether correct amount of events have been added
        self.assertTrue(len(df_new) - len(df_test) == 12)

