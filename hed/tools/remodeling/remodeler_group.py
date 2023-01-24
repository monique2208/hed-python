import numpy as np
import pandas as pd


class RemodelerGroup(object):
    """A class representing a Remodeler Group, either built of a single number column,
    or several number columns when a group is part of a nested structure.

    Attributes:
    layers:
    n_layers:
    layer_length:
    """

    def __init__(self, target, df):
        self.data = df
        self.target = target
        self.layers = self.get_layers()
        self.layer_length = len(self.layers[0])
        self.n_layers = len(self.layers)
        self.indexes = get_splits(self.layers)

    @property
    def layers(self):
        return self._layers

    @layers.setter
    def layers(self, value):
        invalid_nans = check_nans(value)
        if invalid_nans:
            raise ValueError("InvalidRemodelerGroup",
                             f"Column {invalid_nans} has an invalid nan value compared to outer group")

        increasing_subgroups = check_inner_increasing(value)
        if increasing_subgroups:
            raise ValueError("InvalidRemodelerGroup",
                             f"Values in column {increasing_subgroups} do not contain a valid remodeler group. Within "
                             f"a group numbers should increase.")
        self._layers = value

    def get_layers(self):
        """Takes a target number column name and a dataframe containing this number column
        and finds the higher order columns in that data frame"""

        layers = []
        collect_rmd_groups(layers, self.target, self.data)
        return layers


def check_increasing(pandas_series):
    """Takes a pandas series with numbers and returns True if non-NaN values are same or increasing

     Args:
         Pandas Series Object

     Returns:
         bool

     """

    pandas_series = pandas_series.dropna()
    if pandas_series.iloc[0] != 1:
        return False
    return all([(i == j - 1 or i == j) for i, j in
                zip(pandas_series.dropna(), pandas_series.dropna()[1:])])


def match_outer(outer, list_of_column_names):

    """Takes in an outer group name and finds the outer layer colum in a list of
    column names

    Args:
        outer: a string representing the name of an outer layer
        list_of_column_names: a list of the column_names in the events dataframe, where
        the column representing the outer layer can be found

    Returns:
        A string representing the colum name of the outer layer column in the events
        dataframe.

    Raises:
        ValueError "NonExistingOuterGroup": When the outer layer column cannot be found.

    TODO: Raise error when more than one match

    """

    part_match = outer + '_rno'
    try:
        matching_column = [x for x in list_of_column_names if part_match in x][0]
    except IndexError:
        raise ValueError("NonExistingOuterGroup",
                         f"Referenced column {outer} cannot be found in dataframe.")
    return matching_column


def collect_rmd_groups(layers, target, df):
    layers.append(df[target])
    if '_rno_' in target:
        [inner, outer] = target.split('_rno_')
    elif '_rno' in target:
        [inner, outer] = [target[:-4], False]
    if outer:
        outer_column = match_outer(outer, df.columns)
        collect_rmd_groups(layers, outer_column, df)
    else:
        pass


def get_splits(list_of_series):
    exclude_inner_nan = list_of_series[0].index[~np.isnan(list_of_series[0])]
    list_of_series = [x[exclude_inner_nan] for x in list_of_series]
    series_len = len(list_of_series[0])
    n_columns = len(list_of_series)

    boolean_shifts = np.zeros((series_len, n_columns), dtype=bool)

    for ind, ser in enumerate(list_of_series):
        to_check = zip(ser, ser[1:])
        boolean_shifts[:, ind] = [True] + [i == j for i, j in to_check]

    new_list = [[]]

    for (list_of_series_index, no_shift) in zip(list_of_series[0].index, boolean_shifts.all(axis=1)):
        if no_shift:
            new_list[-1].append(list_of_series_index)
        else:
            new_list.append([list_of_series_index])
    return new_list


def check_inner_increasing(value):
    boolean_increasing = []
    outer = []
    for series in reversed(value):
        if outer:
            boolean_increasing_subgroups = []
            splits = get_splits(list(reversed(outer)))
            for sublist in splits:
                boolean_increasing_subgroups.append((check_increasing(series[sublist])))
            boolean_increasing.append(all(boolean_increasing_subgroups))
            outer.append(series)
        else:
            boolean_increasing.append(check_increasing(series))
            outer.append(series)

    if all(boolean_increasing):
        return 0
    else:
        return value[list(reversed(boolean_increasing)).index(0)].name


def check_nans(value):
    """Determine if the Remodeler Group has a valid organization of nan values.
    Inner groups can not have values where the outer group is nan. All elements in
    an inner group must belong in it's specified outer layers.
    To check, data of the remodeler group is used in outer-inner order.
    All nan values in each layer are found.
    Starting with the indexes of the outer group, check if those indexes are found
    in every other group. Once a nan has been found in an outer group, it has to be
    in all the next inner groups.

    Args:

    Returns:
        0 if nans are valid, or the name of innermost invalid group
    """

    list_of_series = list(reversed(value))
    series_len = len(list_of_series[0])
    n_number_columns = len(list_of_series)
    boolean_nans = np.zeros((n_number_columns, series_len), dtype=bool)
    for ind, ser in enumerate(list_of_series):
        boolean_nans[ind, :] = np.isnan(ser)
    index_list = [np.argwhere(x).tolist() for x in boolean_nans]

    store = []
    found_elements = []
    for sublist in index_list:
        for element in sublist:
            store.append(element)
        found_elements.append(all([x in sublist for x in store]))
    if all(found_elements):
        return 0
    else:
        return value[list(reversed(found_elements)).index(0)].name