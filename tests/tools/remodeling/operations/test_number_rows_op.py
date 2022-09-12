import json
import pandas as pd
import numpy as np
import unittest
from hed.tools.remodeling.operations.number_rows_op import NumberRowsOp


class Test(unittest.TestCase):
    """

    """

    @classmethod
    def setUpClass(cls):
        cls.sample_data = [[33.4228, 2.0084, "80"],
                           [36.9395, 0.5, "40"],
                           [37.4395, 0.25, "30"],
                           [37.6895, 0.4083, "12"],
                           [38.0936, 0.0, "2"],
                           [38.0979, 0.5, "40"],
                           [38.5979, 0.25, "30"],
                           [38.8479, 0.3, "11"],
                           [39.1435, 0.0, "1"],
                           [39.1479, 0.5, "40"],
                           [115.6238, 0.25, "30"],
                           [115.8738, 0.3083, "12"],
                           [116.1782, 0.0, "1"],
                           [116.18220000000001, 0.0167, "70"],
                           [134.1619, 0.0, "3"],
                           [134.16570000000002, 2.0084, "80"],
                           [151.7409, 0.5, "40"],
                           [152.241, 0.25, "30"],
                           [152.491, 0.2, "211"],
                           [152.691, 1.05, "221"],
                           [347.9184, 0.5, "40"],
                           [348.4184, 0.25, "30"],
                           [348.6684, 0.4667, "11"],
                           [349.1281, 0.0, "1"],
                           [349.1351, 0.0167, "70"],
                           [366.5138, 0.0, "3"],
                           [366.5186, 2.0084, "Stress_post"]
                           ]

        cls.sample_columns = ["onset", "duration", "code"]
        cls.existing_sample_columns = ["onset", "duration", "number"]

        cls.numbered_data = [[33.4228, 2.0084, "80", 1],
                             [36.9395, 0.5, "40", 2],
                             [37.4395, 0.25, "30", 3],
                             [37.6895, 0.4083, "12", 4],
                             [38.0936, 0.0, "2", 5],
                             [38.0979, 0.5, "40", 6],
                             [38.5979, 0.25, "30", 7],
                             [38.8479, 0.3, "11", 8],
                             [39.1435, 0.0, "1", 9],
                             [39.1479, 0.5, "40", 10],
                             [115.6238, 0.25, "30", 11],
                             [115.8738, 0.3083, "12", 12],
                             [116.1782, 0.0, "1", 13],
                             [116.18220000000001, 0.0167, "70", 14],
                             [134.1619, 0.0, "3", 15],
                             [134.16570000000002, 2.0084, "80", 16],
                             [151.7409, 0.5, "40", 17],
                             [152.241, 0.25, "30", 18],
                             [152.491, 0.2, "211", 19],
                             [152.691, 1.05, "221", 20],
                             [347.9184, 0.5, "40", 21],
                             [348.4184, 0.25, "30", 22],
                             [348.6684, 0.4667, "11", 23],
                             [349.1281, 0.0, "1", 24],
                             [349.1351, 0.0167, "70", 25],
                             [366.5138, 0.0, "3", 26],
                             [366.5186, 2.0084, "Stress_post", 27]
                             ]

        cls.numbered_columns = ["onset", "duration", "code", "number"]

        cls.overwritten_data = [[33.4228, 2.0084, 1],
                                [36.9395, 0.5, 2],
                                [37.4395, 0.25, 3],
                                [37.6895, 0.4083, 4],
                                [38.0936, 0.0, 5],
                                [38.0979, 0.5, 6],
                                [38.5979, 0.25, 7],
                                [38.8479, 0.3, 8],
                                [39.1435, 0.0, 9],
                                [39.1479, 0.5, 10],
                                [115.6238, 0.25, 11],
                                [115.8738, 0.3083, 12],
                                [116.1782, 0.0, 13],
                                [116.18220000000001, 0.0167, 14],
                                [134.1619, 0.0, 15],
                                [134.16570000000002, 2.0084, 16],
                                [151.7409, 0.5, 17],
                                [152.241, 0.25, 18],
                                [152.491, 0.2, 19],
                                [152.691, 1.05, 20],
                                [347.9184, 0.5, 21],
                                [348.4184, 0.25, 22],
                                [348.6684, 0.4667, 23],
                                [349.1281, 0.0, 24],
                                [349.1351, 0.0167, 25],
                                [366.5138, 0.0, 26],
                                [366.5186, 2.0084, 27]
                                ]

        cls.filter_numbered_data = [[33.4228, 2.0084, "80", "n/a"],
                                    [36.9395, 0.5, "40", 1],
                                    [37.4395, 0.25, "30", "n/a"],
                                    [37.6895, 0.4083, "12", "n/a"],
                                    [38.0936, 0.0, "2", "n/a"],
                                    [38.0979, 0.5, "40", 2],
                                    [38.5979, 0.25, "30", "n/a"],
                                    [38.8479, 0.3, "11", "n/a"],
                                    [39.1435, 0.0, "1", "n/a"],
                                    [39.1479, 0.5, "40", 3],
                                    [115.6238, 0.25, "30", "n/a"],
                                    [115.8738, 0.3083, "12", "n/a"],
                                    [116.1782, 0.0, "1", "n/a"],
                                    [116.18220000000001, 0.0167, "70", "n/a"],
                                    [134.1619, 0.0, "3", "n/a"],
                                    [134.16570000000002, 2.0084, "80", "n/a"],
                                    [151.7409, 0.5, "40", 4],
                                    [152.241, 0.25, "30", "n/a"],
                                    [152.491, 0.2, "211", "n/a"],
                                    [152.691, 1.05, "221", "n/a"],
                                    [347.9184, 0.5, "40", 5],
                                    [348.4184, 0.25, "30", "n/a"],
                                    [348.6684, 0.4667, "11", "n/a"],
                                    [349.1281, 0.0, "1", "n/a"],
                                    [349.1351, 0.0167, "70", "n/a"],
                                    [366.5138, 0.0, "3", "n/a"],
                                    [366.5186, 2.0084, "Stress_post", "n/a"]
                                    ]

        cls.filter_overwritten_numbered_data = [[33.4228, 2.0084, "n/a"],
                                                [36.9395, 0.5, 1],
                                                [37.4395, 0.25, "n/a"],
                                                [37.6895, 0.4083, "n/a"],
                                                [38.0936, 0.0, "n/a"],
                                                [38.0979, 0.5, 2],
                                                [38.5979, 0.25, "n/a"],
                                                [38.8479, 0.3, "n/a"],
                                                [39.1435, 0.0, "n/a"],
                                                [39.1479, 0.5, 3],
                                                [115.6238, 0.25, "n/a"],
                                                [115.8738, 0.3083, "n/a"],
                                                [116.1782, 0.0, "n/a"],
                                                [116.18220000000001, 0.0167, "n/a"],
                                                [134.1619, 0.0, "n/a"],
                                                [134.16570000000002, 2.0084, "n/a"],
                                                [151.7409, 0.5, 4],
                                                [152.241, 0.25, "n/a"],
                                                [152.491, 0.2, "n/a"],
                                                [152.691, 1.05, "n/a"],
                                                [347.9184, 0.5, 5],
                                                [348.4184, 0.25, "n/a"],
                                                [348.6684, 0.4667, "n/a"],
                                                [349.1281, 0.0, "n/a"],
                                                [349.1351, 0.0167, "n/a"],
                                                [366.5138, 0.0, "n/a"],
                                                [366.5186, 2.0084, "n/a"]
                                                ]

        base_parameters = {
            "number_column_name": "number"
        }

        overwrite_false_parameters = {
            "number_column_name": "number",
            "overwrite": False
        }

        overwrite_true_parameters = {
            "number_column_name": "number",
            "overwrite": True
        }

        filter_complete_parameters = {
            "number_column_name": "number",
            "match_value": {"column": "code", "value": "40"}
        }

        filter_incomplete_parameters = {
            "number_column_name": "number",
            "match_value": {"column": "code"}
        }

        filter_invalid_parameters = {
            "number_column_name": "number",
            "match_value": {"column": "code", "value": "40", "label": "code"}
        }

        filter_wrong_type_parameters = {
            "number_column_name": "number",
            "match_value": {"column": 246, "value": []}
        }

        filter_missing_column_parameters = {
            "number_column_name": "number",
            "match_value": {"column": "trial_type", "value": "40"}
        }

        filter_missing_value_parameters = {
            "number_column_name": "number",
            "match_value": {"column": "code", "value": "stop_trial"}
        }

        filter_overwrite_parameters = {
            "number_column_name": "number",
            "match_value": {"column": "number", "value": "40"},
            "overwrite": True
        }

        cls.json_parms = json.dumps(base_parameters)

        cls.json_overwrite_false_parms = json.dumps(overwrite_false_parameters)
        cls.json_overwrite_true_parms = json.dumps(overwrite_true_parameters)

        cls.json_filter_complete_parameters = json.dumps(filter_complete_parameters)
        cls.json_filter_incomplete_parameters = json.dumps(filter_incomplete_parameters)
        cls.json_filter_invalid_parameters = json.dumps(filter_invalid_parameters)
        cls.json_filter_wrong_type_parameters = json.dumps(filter_wrong_type_parameters)
        cls.json_filter_missing_column_parameters = json.dumps(filter_missing_column_parameters)
        cls.json_filter_missing_value_parameters = json.dumps(filter_missing_value_parameters)
        cls.json_filter_overwrite_parameters = json.dumps(filter_overwrite_parameters)

        cls.dispatcher = None
        cls.file_name = None

    @classmethod
    def tearDownClass(cls):
        pass

    def test_number_rows_new_column(self):
        # Test when new column name is given with overwrite unspecified (=False)
        parms = json.loads(self.json_parms)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_check = pd.DataFrame(self.numbered_data, columns=self.numbered_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_new = op.do_op(self.dispatcher, df_test, self.file_name)
        df_new = df_new.fillna('n/a')

        self.assertTrue(list(df_new.columns) == list(df_check.columns),
                        "numbered_events should have the expected columns")
        self.assertTrue(len(df_new) == len(df_test),
                        "numbered_events should have same length as original dataframe")
        self.assertTrue(all([i + 1 == value for (i, value) in enumerate(df_new[parms['number_column_name']])]),
                        "event should be numbered consecutively from 1 to length of the dataframe")
        self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
                        "numbered_events should not differ from check")

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "number_rows should not change the input df columns")
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
                        "number_rows should not change the input df values")

    def test_existing_column_overwrite_false(self):
        # Test when existing column name is given with overwrite specified False
        parms = json.loads(self.json_overwrite_false_parms)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)

        with self.assertRaisesRegex(ValueError, "ExistingNumberColumn"):
            df_new = op.do_op(self.dispatcher, df_test, self.file_name)

    def test_existing_column_overwrite_unspecified(self):
        # Test when existing column name is given with overwrite unspecified (=False)
        parms = json.loads(self.json_parms)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)

        with self.assertRaisesRegex(ValueError, "ExistingNumberColumn"):
            df_new = op.do_op(self.dispatcher, df_test, self.file_name)

    def test_existing_column_overwrite_true(self):
        # Test when existing column name is given with overwrite True
        parms = json.loads(self.json_overwrite_true_parms)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)
        df_check = pd.DataFrame(self.overwritten_data, columns=self.existing_sample_columns)
        df_new = op.do_op(self.dispatcher, df_test, self.file_name)
        df_new = df_new.fillna('n/a')

        self.assertTrue(list(df_new.columns) == list(self.existing_sample_columns),
                        "numbered_events should have the same columns as original dataframe in case of overwrite")
        self.assertTrue(len(df_new) == len(df_test),
                        "numbered_events should have same length as original dataframe")
        self.assertTrue(all([i + 1 == value for (i, value) in enumerate(df_new[parms['number_column_name']])]),
                        "event should be numbered consecutively from 1 to length of the dataframe")
        self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
                        "numbered_events should not differ from check")

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "split_event should not change the input df columns")
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
                        "split_event should not change the input df values")

    def test_filter_complete_parameters(self):
        # Test when valid complete filter/match_value parameters are given
        parms = json.loads(self.json_filter_complete_parameters)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_check = pd.DataFrame(self.filter_numbered_data, columns=self.numbered_columns)
        df_new = op.do_op(self.dispatcher, df_test, self.file_name)
        df_new = df_new.fillna('n/a')

        self.assertTrue(list(df_new.columns) == list(self.numbered_columns),
                        "numbered_events should have expected columns")
        self.assertTrue(len(df_new) == len(df_test),
                        "numbered_events should have same length as original dataframe")
        self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
                        "numbered_events should not differ from check")

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "split_event should not change the input df columns")
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
                        "split_event should not change the input df values")

    def test_filter_incomplete_parameters(self):
        # Test when filter/match_value parameters are not complete
        parms = json.loads(self.json_filter_incomplete_parameters)

        with self.assertRaisesRegex(KeyError, "MissingRequiredParameters"):
            op = NumberRowsOp(parms)

    def test_filter_invalid_parameters(self):
        # Test when invalid filter/match_value parameters are given
        parms = json.loads(self.json_filter_invalid_parameters)

        with self.assertRaisesRegex(KeyError, "BadParameter"):
            op = NumberRowsOp(parms)

    def test_filter_wrong_type_parameters(self):
        # Test when invalid filter/match_value parameters are given
        parms = json.loads(self.json_filter_wrong_type_parameters)
        # TODO: need to fix type
        # with self.assertRaisesRegex(TypeError, "BadType"):
        #     op = NumberRowsOp(parms)

    def test_filter_missing_column_parameters(self):
        # Test when specified filter column is not in event file
        parms = json.loads(self.json_filter_missing_column_parameters)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)

        with self.assertRaisesRegex(ValueError, "MissingMatchColumn"):
            df_new = op.do_op(self.dispatcher, df_test, self.file_name)

    def test_filter_missing_value_parameters(self):
        # Test when specified filter value is not in event file
        parms = json.loads(self.json_filter_missing_value_parameters)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.sample_columns)

        with self.assertRaisesRegex(ValueError, "MissingMatchValue"):
            df_new = op.do_op(self.dispatcher, df_test, self.file_name)

    def test_filter_overwrite(self):
        # Test when specified filter value is not in event file
        parms = json.loads(self.json_filter_overwrite_parameters)
        op = NumberRowsOp(parms)
        df = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)
        df_test = pd.DataFrame(self.sample_data, columns=self.existing_sample_columns)
        df_check = pd.DataFrame(self.filter_overwritten_numbered_data, columns=self.existing_sample_columns)
        df_new = op.do_op(self.dispatcher, df_test, self.file_name)
        df_new = df_new.fillna('n/a')

        self.assertTrue(list(df_new.columns) == list(self.existing_sample_columns),
                        "numbered_events should have expected columns")
        self.assertTrue(len(df_new) == len(df_test),
                        "numbered_events should have same length as original dataframe")
        self.assertTrue(np.array_equal(df_new.to_numpy(), df_check.to_numpy()),
                        "numbered_events should not differ from check")

        # Test that df has not been changed by the op
        self.assertTrue(list(df.columns) == list(df_test.columns),
                        "split_event should not change the input df columns")
        self.assertTrue(np.array_equal(df.to_numpy(), df_test.to_numpy()),
                        "split_event should not change the input df values")


if __name__ == '__main__':
    unittest.main()
