""" Rename columns in a columnar file. """

from hed.tools.remodeling.operations.base_op import BaseOp
import pandas as pd


class ExtendColumnsOp (BaseOp):
    """ Rename columns in a tabular file.

    Required remodeling parameters:
        - **column_mapping** (*dict*): The names of the columns to be renamed with values to be remapped to.
        - **ignore_missing** (*bool*): If true, the names in column_mapping that are not columns and should be ignored.

    """
    NAME = "extend_columns"

    PARAMS = {
        "type": "object",
        "properties": {
            "column_name": {
                "type": "string",
                "description": "Name of the column that matches between file that is extending"
            },
            "filepath": {
                        "type": "string",
                        "description": "Name of the column that matches between file that is extending"
                    },
            "ignore_missing": {
                "type": "boolean",
                "description": "If true ignore columns that are not in event file, otherwise error."
            }
        },
        "required": [
            "column_name",
            "filepath"
        ],
        "additionalProperties": False
    }

    def __init__(self, parameters):
        """ Constructor for extend columns operation.

        Parameters:
            parameters (dict): Dictionary with the parameter values for required and optional parameters

        """
        super().__init__(parameters)
        self.column_name = parameters["column_name"]
        self.filepath = parameters['filepath']

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

        df_map = pd.read_csv(self.filepath)

        merged_df = pd.merge(df_new, df_map, on=self.column_name, how='left')

        return merged_df

    @staticmethod
    def validate_input_data(parameters):
        """ Additional validation required of operation parameters not performed by JSON schema validator. """
        return []
