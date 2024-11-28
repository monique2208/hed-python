""" Remove columns from a columnar file. """
from hed.tools.remodeling.operations.base_op import BaseOp
import pandas as pd


class DelimiterSplitOp(BaseOp):
    """ Remove columns from a columnar file.

    Required remodeling parameters:
        - **column_names** (*list*): The names of the columns to be removed.
        - **ignore_missing** (*boolean*): If True, names in column_names that are not columns in df should be ignored.

    """
    NAME = "remove_columns"

    PARAMS = {
        "type": "object",
        "properties": {
            "source_column": {
                "type": "string",
            },
            "destination_column": {
                "type": ["string", "array"]
            },
            "delimiter": {
                "type": "string"
            },
            "axis": {
                "type": "string",
                "enum": ["row", "column"]
            },
            "copy_columns": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "minItems": 1,
                "uniqueItems": True
            }
        },
        "required": [
            "source_column",
            "destination_column",
            "delimiter",
            "axis"
        ],
        "additionalProperties": False
    }

    def __init__(self, parameters):
        """ Constructor for remove columns operation.

        Parameters:
            parameters (dict): Dictionary with the parameter values for required and optional parameters.

        """
        super().__init__(parameters)
        self.source_column = parameters['source_column']
        self.destination_column = parameters['destination_column']
        self.delimiter = parameters["delimiter"]
        self.axis = parameters["axis"]
        self.copy_columns = parameters.get("copy_columns", False)
        

    def do_op(self, dispatcher, df, name, sidecar=None):
        """ Remove indicated columns from a dataframe.

        Parameters:
            dispatcher (Dispatcher): Manages the operation I/O.
            df (DataFrame): The DataFrame to be remodeled.
            name (str): Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like):  Not needed for this operation.

        Returns:
            Dataframe: A new dataframe after processing.

        :raises KeyError:
            - If ignore_missing is False and a column not in the data is to be removed.

        """
        df_new = df.copy()
        
        # ensure no double or trialing white spaces
        df_new = df.map(lambda x: x.strip() if isinstance(x, str) else x)


        if self.axis == "row":
            df_non_nan = df_new[df_new[self.source_column].notna()].copy()
            df_non_nan[self.source_column] = df_non_nan[self.source_column].str.replace('  ', ' ')
            df_non_nan[self.destination_column] = df_non_nan[self.source_column].str.split(self.delimiter)
            df_non_nan['item_index'] = df_non_nan[self.destination_column].apply(lambda x: list(range(1,len(x)+1)))
            df_exploded = df_non_nan.explode([self.destination_column, 'item_index']).reset_index(drop=True)
            df_exploded[self.source_column] = pd.NA
            columns_to_keep = ["onset", "duration", self.destination_column, 'item_index']
            if self.copy_columns:
                columns_to_keep = columns_to_keep + self.copy_columns
            df_exploded = df_exploded[columns_to_keep]
            df_new = pd.concat([df_new, df_exploded], ignore_index=True).sort_values(by=['onset', self.source_column, 'item_index']).reset_index(drop=True)
        
        if self.axis == "column":
            df_new[self.destination_column] = df_new[self.source_column].str.split(self.delimiter, expand=True)

        return df_new

    @staticmethod
    def validate_input_data(parameters):
        """ Additional validation required of operation parameters not performed by JSON schema validator. """
        return []
