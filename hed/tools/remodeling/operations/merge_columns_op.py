""" Rename columns in a columnar file. """

from hed.tools.remodeling.operations.base_op import BaseOp
import pandas as pd
import numpy as np


class MergeColumnsOp (BaseOp):
    """ Rename columns in a tabular file.

    Required remodeling parameters:
        - **column_mapping** (*dict*): The names of the columns to be renamed with values to be remapped to.
        - **ignore_missing** (*bool*): If true, the names in column_mapping that are not columns and should be ignored.

    """
    NAME = "merge_columns"

    PARAMS = {
        "type": "object",
        "properties": {
            "column_names": {
                "type": "array",
                "description": "Columns that should be merged together",
                "items": {
                    "type": "string"
                }
            },
            "new_column": {
                "type": "string"
            },
            "separator": {
                "type": "string"
            },
            "remove_source_columns": {
                "type": "boolean",
                "description": "If true delete the columns that were merged together and only keep the new column"
            }
        },
        "required": [
            "column_names",
            "new_column",
            "separator"
        ],
        "additionalProperties": False
    }

    def __init__(self, parameters):
        """ Constructor for extend columns operation.

        Parameters:
            parameters (dict): Dictionary with the parameter values for required and optional parameters

        """
        super().__init__(parameters)
        self.column_names = parameters["column_names"]
        self.new_column = parameters["new_column"]
        self.separator = parameters["separator"]
        self.remove_source_columns = parameters.get("remove_source_columns", False)

    def do_op(self, dispatcher, df, name, sidecar=None):
        """ Replace values as specified in value mapping dictionary

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
        
        df_new[self.new_column] = df_new[self.column_names].apply(lambda x: ' | '.join(x.dropna()), axis=1)

        #restore n/a's 
        df_new = df_new.replace(r'^\s*$', np.nan, regex=True)

        if self.remove_source_columns:
            df_new = df_new.drop(self.column_names, axis=1)

        return df_new

    @staticmethod
    def validate_input_data(parameters):
        """ Additional validation required of operation parameters not performed by JSON schema validator. """
        return []
