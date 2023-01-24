import numpy as np
import pandas as pd
from hed.tools.remodeling.operations.base_op import BaseOp
from hed.tools.remodeling.remodeler_group import RemodelerGroup


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
        if '_rno' not in self.target_group:
            raise ValueError("InvalidSourceColumn",
                             "Create event source column must be a remodeler-number-column.")
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

        number_columns = RemodelerGroup(self.target_group, df)

        df_new = df.copy()

        if self.anchor_column not in df.columns:
            df_new[self.anchor_column] = np.nan

        for ind, sublist in enumerate(number_columns.indexes):
            onset = df_new.loc[sublist[0], 'onset']
            duration = ((df_new.loc[sublist[-1], 'onset'] - df_new.loc[sublist[0], 'onset'])
                        + df_new.loc[sublist[-1], 'duration'])

            copy_columns = [x.name for x in number_columns.layers]

            line = pd.DataFrame([np.repeat(np.nan, len(df_new.columns))],
                                columns=df_new.columns, index=[sublist[0] - 0.5])
            line['onset'] = onset
            line['duration'] = duration

            for column in copy_columns:
                line[column] = df_new.loc[sublist[0], column]

            line[self.anchor_column] = self.label

            df_new = pd.concat([line, df_new])

        df_new = df_new.sort_index().reset_index(drop=True)
        return df_new

