""" Implementation in progress. """

from hed.tools.remodeling.operations.base_op import BaseOp
import numpy as np

# TODO: This class is under development


class LabelResponseOp(BaseOp):
    """ Add a column describing a response (as correct or incorrect) """
    NAME = "number_rows"

    PARAMS = {
        "type": "object",
        "properties": {
        "event_map": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "trigger": {
                        "type": "object",
                        "description": "Specify the column names and values of the valid trigger. The content of undefined columns will be ignored",
                        "patternProperties": {
                            ".*": {
                                "type": "string"
                            }
                        },
                        "minProperties": 1
                    },
                    "response": {
                        "type": "object",
                        "description": "Specify the column names and values of the valid response event. The content of undefined columns will be ignored",
                        "patternProperties": {
                            ".*": {
                                "type": "string"
                            }
                        },
                        "minProperties": 1
                    },
                    "label": {
                        "description": "The label to provide to this event combination",
                        "type": "string"
                    }
                },
                "required": [
                    "trigger",
                    "response",
                    "label"
                ]
            },
                "minItems": 1,
                "uniqueItems": "True"
            },
            "column_name": {
                "type": "string"
            },
            "reaction_time": {
                "type": "number",
                "minimum": 0,
                "description": "A maximum time within which the response should happen from the trigger to count"
            }
        },
        "required": [
            "event_map",
            "column_name"
        ],
        "additionalProperties": "False"
    }
    

    def __init__(self, parameters):
        super().__init__(parameters)
        self.event_map = parameters['event_map']
        self.column_name = parameters['column_name']
        self.reaction_time = parameters.get('reaction_time', False)

    def do_op(self, dispatcher, df, name, sidecar=None):
        """
        Adds a new column (column_name) that provides information about the relationship between the trigger event
        and the response event, with the label provided by a user.
        If a trigger event matches multiple responses or receives conflicting labels from the event_map,
        the trigger is labeled as "undetermined". Response events retain their respective labels.
        """
        if self.column_name in df.columns:
            raise ValueError("ExistingColumn",
                            f"Column {self.column_name} already exists in the event file.", "")

        df_new = df.copy()
        df_new[self.column_name] = None  # Initialize the new column with None

        # A dictionary to track labels assigned to trigger indices
        trigger_labels = {}

        for event in self.event_map:
            trigger_conditions = event["trigger"]
            response_conditions = event["response"]
            label = event["label"]
            reaction_time = self.reaction_time

            # Find trigger rows matching the conditions
            trigger_rows = df_new
            for col, val in trigger_conditions.items():
                trigger_rows = trigger_rows[trigger_rows[col] == val]

            # Iterate through matching trigger rows
            for _, trigger_row in trigger_rows.iterrows():
                trigger_index = trigger_row.name
                trigger_onset = trigger_row["onset"]

                # Find matching response rows within the reaction time
                response_rows = df_new
                for col, val in response_conditions.items():
                    response_rows = response_rows[response_rows[col] == val]

                if reaction_time is not None:
                    response_rows = response_rows[
                        (response_rows["onset"] > trigger_onset + 0.100) &
                        (response_rows["onset"] <= trigger_onset + reaction_time)
                    ]

                if response_rows.empty:
                    continue  # No response, leave the trigger label as None

                if len(response_rows) > 1:
                    # Multiple responses: label the trigger as "undetermined"
                    df_new.at[trigger_index, self.column_name] = "undetermined"
                    for response_index in response_rows.index:
                        df_new.at[response_index, self.column_name] = label
                else:
                    # Single response: Assign the label to the trigger and response
                    response_index = response_rows.index[0]
                    if trigger_index in trigger_labels:
                        df_new.at[trigger_index, self.column_name] = "undetermined"
                    else:
                        # First label assignment for this trigger
                        trigger_labels[trigger_index] = label
                        df_new.at[trigger_index, self.column_name] = label

                    df_new.at[response_index, self.column_name] = label

        return df_new

    @staticmethod
    def validate_input_data(parameters):
        """ Additional validation required of operation parameters not performed by JSON schema validator. """
        return []
