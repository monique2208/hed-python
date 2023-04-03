import numpy as np
import pandas as pd
from hed.tools.remodeling.operations.base_op import BaseOp


class CreateEventOp(BaseOp):

    PARAMS = {
        "operation": "create_event",
        "required_parameters": {
            "target_group": str,
            "anchor_column": str,
            "label": str
        },
        "optional_parameters": {}
    }

    def __init__(self, parameters):
        super().__init__(self.PARAMS, parameters)
        self.check_parameters(parameters)
        self.target_group = parameters['target_group']
        self.anchor_column = parameters['anchor_column']
        self.label = parameters['label']

    def do_op(self, dispatcher, df, name, sidecar=None, verbose=False):
        """ Find groups based on number column and create new events from groups

        Args:
            dispatcher (Dispatcher) - dispatcher object for context
            df (DataFrame) - The DataFrame to be remodeled.
            name (str) - Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like)   Only needed for HED operations.
            verbose (bool) If True output informative messages during operation.

        Returns:
            Dataframe - a new dataframe after processing.

        """
        
        if self.target_group not in df.columns:
            raise ValueError("MissingInputColumn",
                             f"Column {self.target_group} does not exist in event file {name}")

        df_new = df.copy()

        number_groups = df_new.groupby(self.target_group)
        group_indexes = number_groups.groups.values()
        group_indexes_to_list = [list(x) for x in list(group_indexes)]
        
        

        if self.anchor_column not in df.columns:
            df_new[self.anchor_column] = np.nan

        for ind, sublist in enumerate(group_indexes_to_list):
            onset = df_new.loc[sublist[0], 'onset']
            duration = ((df_new.loc[sublist[-1], 'onset'] - df_new.loc[sublist[0], 'onset'])
                        + df_new.loc[sublist[-1], 'duration'])

            line = pd.DataFrame([np.repeat(np.nan, len(df_new.columns))],
                                columns=df_new.columns, index=[sublist[0] - 0.5])
            line['onset'] = onset
            line['duration'] = duration

            line[self.anchor_column] = self.label

            df_new = pd.concat([line, df_new])

        df_new = df_new.sort_index().reset_index(drop=True)
        return df_new

