import numpy as np
from hed.tools.remodeling.operations.base_op import BaseOp
from hed.tools.util.data_util import get_indices, tuple_to_range
import itertools
from math import ceil, floor


class GroupByCountGroup(BaseOp):

    PARAMS = {
        "operation": "group_by_count",
        "required_parameters": {
            "group_name": str,
            "from_group": str,
            "count": int
        },
        "optional_parameters": {"overwrite": bool, "force_complete": bool}
    }

    def __init__(self, parameters):
        super().__init__(self.PARAMS, parameters)

        self.group_name = parameters["group_name"]
        self.from_group = parameters["from_group"]
        self.count = parameters["count"]

        self.overwrite = parameters.get("overwrite", False)
        self.force_complete = parameters.get("force_complete", False)

    def do_op(self, dispatcher, df, name, sidecar=None):
        """ Add numbers to groups of events in dataframe based on counting events.

        Args:
            dispatcher (Dispatcher) - dispatcher object for context
            df (DataFrame) - The DataFrame to be remodeled.
            name (str) - Unique identifier for the dataframe -- often the original file path.
            sidecar (Sidecar or file-like)   Only needed for HED operations.

        Returns:
            Dataframe - a new dataframe after processing.

        """

        group_column = self.group_name

        if group_column in df.columns and not self.overwrite:
            raise ValueError("ExistingNumberColumn",
                                f"{self.group_name} remodeler number column already exists in event file.", "")

        df_new = df.copy()
        
        group_indexes = self._get_group_by_index_list(df_new)

        number_of_groups = len(group_indexes)

        if number_of_groups % self.count == 0:
            number_of_new_groups = int(number_of_groups / self.count)
        else:
            if self.force_complete:
                number_of_new_groups = ceil(number_of_groups / self.count)
            else:
                number_of_new_groups = floor(number_of_groups / self.count)
        
        group_iterator = iter(group_indexes)

        for group_ind in range(number_of_new_groups):
            for ind in range(self.count):
                df_new.loc[next(group_iterator),self.group_name] = group_ind+1

        return df_new

    def _get_group_by_index_list(self, df):
        groups = df.groupby(self.from_group)
        group_indexes = groups.groups.values()
        return [list(x) for x in list(group_indexes)]
        