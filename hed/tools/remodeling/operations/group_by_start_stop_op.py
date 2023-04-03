import numpy as np
from hed.tools.remodeling.operations.base_op import BaseOp
from hed.tools.util.data_util import get_indices, tuple_to_range
import itertools


class GroupByStartStopOp(BaseOp):

    PARAMS = {
        "operation": "group_by_start_stop",
        "required_parameters": {
            "group_name": str,
            "source_column": str,
            "start": dict,
            "stop": dict
        },
        "optional_parameters": {"overwrite": bool, "exclude_values": list, "start_at_beginning": bool,
                                "end_at_end": bool}
    }

    def __init__(self, parameters):
        super().__init__(self.PARAMS, parameters)

        self.group_name = parameters['group_name']
        self.source_column = parameters['source_column']
        self.start = parameters['start']
        self.stop = parameters['stop']

        self.required_start_stop_dict = {"values": list, "inclusion": str}
        self.required_inclusion_params = ["include", "exclude"]

        required = set(self.required_start_stop_dict.keys())
        for param_to_test in [self.start, self.stop]:
            required_missing = required.difference(set(param_to_test.keys()))
            if required_missing:
                raise KeyError("MissingRequiredParameters",
                               f"Specified {param_to_test} for number_rows requires parameters {list(required_missing)}")
            for param_name, param_value in param_to_test.items():
                if param_name in required:
                    param_type = self.required_start_stop_dict[param_name]
                    if not isinstance(param_value, param_type):
                        raise TypeError("BadType" f"{param_value} has type {type(param_value)} not {param_type}")
                    if (param_name == 'inclusion') & (param_value not in self.required_inclusion_params):
                        raise ValueError(
                            "BadValue" f" {param_name} must be one of {self.required_inclusion_params} not {param_value}")
                else:
                    raise KeyError("BadParameter",
                                   f"{param_name} not a required or optional parameter for Group by start stop")

        self.overwrite = parameters.get('overwrite', False)
        self.exclude_values = parameters.get('exclude_values', [])
        self.start_at_beginning = parameters.get('start_at_beginning', False)
        self.end_at_end = parameters.get('end_at_end', False)

    def do_op(self, dispatcher, df, name, sidecar=None):
        """ Add numbers to groups of events in dataframe.

        Args:
            dispatcher (Dispatcher) - dispatcher object for context
            df (DataFrame) - The DataFrame to be remodeled.
            name (str) - Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like)   Only needed for HED operations.

        Returns:
            Dataframe - a new dataframe after processing.

        """
        # check if number column exists and if so, check overwrite setting
        group_column = self.group_name

        if group_column in df.columns:
            if self.overwrite is False:
                raise ValueError("ExistingNumberColumn",
                                 f"{self.group_name} remodeler number column already exists in event file.", "")

        # check if source_column exists
        if self.source_column not in df.columns:
            raise ValueError("MissingSourceColumn",
                             f"Column {self.source_column} does not exist in event file {name}.", "")

        # check if all elements in value lists start and stop exist in the source_column
        missing = []
        for element in self.start['values']:
            if element not in df[self.source_column].tolist():
                missing.append(element)
        if len(missing) > 0:
            raise ValueError("MissingValue",
                             f"Start value(s) {missing} does not exist in {self.source_column} of event file {name}")

        missing = []
        for element in self.stop['values']:
            if element not in df[self.source_column].tolist():
                missing.append(element)
        if len(missing) > 0:
            raise ValueError("MissingValue",
                             f"Start value(s) {missing} does not exist in {self.source_column} of event file {name}")

        df_new = df.copy()
        # create number column
        df_new[group_column] = np.nan

        # find group indices
        indices = tuple_to_range(get_indices(df, 
                                             self.source_column, 
                                             self.start['values'], 
                                             self.stop['values']),
                                             [self.start['inclusion'],self.stop['inclusion']])
        
        for i, group in enumerate(indices):
            df_new.loc[group, group_column] = i + 1

        df_new.loc[self.exclude_values, group_column] = np.nan

        return df_new