""" Rename columns in a columnar file. """

from hed.tools.remodeling.operations.base_op import BaseOp


class RemapValuesOp (BaseOp):
    """ Rename columns in a tabular file.

    Required remodeling parameters:
        - **column_mapping** (*dict*): The names of the columns to be renamed with values to be remapped to.
        - **ignore_missing** (*bool*): If true, the names in column_mapping that are not columns and should be ignored.

    """
    NAME = "remap_values"

    PARAMS = {
        "type": "object",
        "properties": {
            "column_name": {
                "type": "string",
                "description": "Name of the column of which the values should be changes."
            },
            "value_mapping": {
                "type": "object",
                "description": "Mapping between original value and the new value.",
                "patternProperties": {
                    ".*": {
                        "type": "string"
                    }
                },
                "minProperties": 1
            },
            "ignore_missing": {
                "type": "boolean",
                "description": "If true ignore value_mapping keys that don't correspond to columns values, otherwise error."
            }
        },
        "required": [
            "column_name",
            "value_mapping",
            "ignore_missing"
        ],
        "additionalProperties": False
    }

    def __init__(self, parameters):
        """ Constructor for rename columns operation.

        Parameters:
            parameters (dict): Dictionary with the parameter values for required and optional parameters

        """
        super().__init__(parameters)
        self.column_name = parameters["column_name"]
        self.value_mapping = parameters['value_mapping']
        if parameters['ignore_missing']:
            self.error_handling = 'ignore'
        else:
            self.error_handling = 'raise'

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
        df_new[self.column_name] = df_new[self.column_name].apply(lambda x: self.value_mapping.get(str(x), x))

        return df_new

    @staticmethod
    def validate_input_data(parameters):
        """ Additional validation required of operation parameters not performed by JSON schema validator. """
        return []
