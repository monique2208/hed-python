import os
import json
import pandas as pd
import numpy as np
import unittest
from hed.tools.remodeling.operations.group_by_count_group import GroupByCountGroup


class Test(unittest.TestCase):
    """

    """

    @classmethod
    def setUpClass(cls):
        path = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                             '../../../data/remodel_tests'))

        cls.sample_columns = ["onset", "duration", "trial"]
        cls.data = [[33.4228, 2.0084, np.nan],
                    [36.9395, 0.5, np.nan],
                    [37.4395, 0.25, 1],
                    [37.6895, 0.4083, 1],
                    [38.0936, 0.0, 1],
                    [38.0979, 0.5, np.nan],
                    [38.5979, 0.25, 2],
                    [38.8479, 0.3, 2],
                    [39.1435, 0.0, 2],
                    [39.1479, 0.5, np.nan],
                    [115.6238, 0.25, 3],
                    [115.8738, 0.3083, 3],
                    [116.1782, 0.0, 3],
                    [116.18220000000001, 0.0167, np.nan],
                    [134.1619, 0.0, np.nan],
                    [134.16570000000002, 2.0084, np.nan],
                    [151.7409, 0.5, np.nan],
                    [152.241, 0.25, 4],
                    [152.491, 0.2, 4],
                    [152.691, 1.05, 4],
                    [347.9184, 0.5, np.nan],
                    [348.4184, 0.25, 5],
                    [348.6684, 0.4667, 5],
                    [349.1281, 0.0, 5],
                    [349.1351, 0.0167, np.nan],
                    [366.5138, 0.0, np.nan],
                    [366.5186, 2.0084, np.nan]
                    ]
        
        cls.numbered_data = [[33.4228, 2.0084, np.nan, np.nan],
                    [36.9395, 0.5, np.nan, np.nan],
                    [37.4395, 0.25, 1, 1],
                    [37.6895, 0.4083, 1, 1],
                    [38.0936, 0.0, 1, 1],
                    [38.0979, 0.5, np.nan, np.nan],
                    [38.5979, 0.25, 2, 1],
                    [38.8479, 0.3, 2, 1],
                    [39.1435, 0.0, 2, 1],
                    [39.1479, 0.5, np.nan, np.nan],
                    [115.6238, 0.25, 3, 2],
                    [115.8738, 0.3083, 3, 2],
                    [116.1782, 0.0, 3, 2],
                    [116.18220000000001, 0.0167, np.nan, np.nan],
                    [134.1619, 0.0, np.nan, np.nan],
                    [134.16570000000002, 2.0084, np.nan, np.nan],
                    [151.7409, 0.5, np.nan, np.nan],
                    [152.241, 0.25, 4, 2],
                    [152.491, 0.2, 4, 2],
                    [152.691, 1.05, 4, 2],
                    [347.9184, 0.5, np.nan, np.nan],
                    [348.4184, 0.25, 5, np.nan],
                    [348.6684, 0.4667, 5, np.nan],
                    [349.1281, 0.0, 5, np.nan],
                    [349.1351, 0.0167, np.nan, np.nan],
                    [366.5138, 0.0, np.nan, np.nan],
                    [366.5186, 2.0084, np.nan, np.nan]
                    ]

        cls.numbered_sample_columns = ["onset", "duration", "trial", "block"]
        cls.parameters_json_path = os.path.realpath(os.path.join(path, 'group_by_count_param.json'))

        cls.dispatcher = None
        cls.file_name = None

    @classmethod
    def tearDownClass(cls):
        pass


    def test_valid_simple(self):
        with open(self.parameters_json_path, "r") as read_parms:
            parms = json.load(read_parms)['valid_simple']

        op = GroupByCountGroup(parms)
        df = pd.DataFrame(self.data, columns=self.sample_columns)
        df_test = pd.DataFrame(self.data, columns=self.sample_columns)

        df_new = op.do_op(self.dispatcher, df, self.file_name)

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "group_by_count_group should not change the input df values")
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy(), equal_nan=True),
                        "group_by_count_group should not change the input df values") 

        self.assertTrue(np.array_equal(df_new.to_numpy(), pd.DataFrame(self.numbered_data, columns=self.numbered_sample_columns).to_numpy(), equal_nan=True),
                        "group_by_count_group result should match template result")
        