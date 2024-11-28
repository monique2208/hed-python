""" Rename columns in a columnar file. """

from hed.tools.remodeling.operations.base_op import BaseOp
import numpy as np


class SplitByColumnOp (BaseOp):
    """ Rename columns in a tabular file.

    Required remodeling parameters:
        - **column_mapping** (*dict*): The names of the columns to be renamed with values to be remapped to.
        - **ignore_missing** (*bool*): If true, the names in column_mapping that are not columns and should be ignored.

    """
    NAME = "rename_columns"

    PARAMS = {
        "type": "object",
        "properties": {
            "column_to_split": {
                "type": "string"
                },
            "split_by": {
                "type": "string",
            }
        },
        "required": [
            "column_to_split",
            "split_by"
        ],
        "additionalProperties": False
    }

    def __init__(self, parameters):
        """ Constructor for rename columns operation.

        Parameters:
            parameters (dict): Dictionary with the parameter values for required and optional parameters

        """
        super().__init__(parameters)
        self.column_to_split = parameters['column_to_split']
        self.split_by = parameters['split_by']

    def do_op(self, dispatcher, df, name, sidecar=None):
        """ Rename columns as specified in column_mapping dictionary.

        Parameters:
            dispatcher (Dispatcher): Manages the operation I/O.
            df (DataFrame): The DataFrame to be remodeled.
            name (str): Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like):  Not needed for this operation.

        Returns:
            Dataframe: A new dataframe after processing.

        :raises KeyError:
            - When ignore_missing is False and column_mapping has columns not in the data.

        """
        df_new = df.copy()

        for value in df_new[self.split_by].dropna().unique():
            df_new[value] = np.where(df_new[self.split_by] == value, df_new[self.column_to_split], np.nan)
        
        return df_new


    @staticmethod
    def validate_input_data(parameters):
        """ Additional validation required of operation parameters not performed by JSON schema validator. """
        return []
