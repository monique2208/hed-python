""" Utilities for downstream analysis such as searching. """

import pandas as pd
from hed.models.tabular_input import TabularInput
from hed.models.expression_parser import TagExpressionParser


def assemble_hed(data_input, columns_included=None, expand_defs=False):
    """ Return assembled HED annotations in a dataframe.

    Parameters:
        data_input (TabularInput): The tabular input file whose HED annotations are to be assembled.
        columns_included (list or None):  A list of additional column names to include.
            If None, only the list of assembled tags is included.
        expand_defs (bool): If True, definitions are expanded when the events are assembled.

    Returns:
        DataFrame or None: A DataFrame with the assembled events.
        dict: A dictionary with definition names as keys and definition content strings as values.
    """

    if columns_included:
        columns = frozenset(list(data_input.dataframe.columns))
        eligible_columns = [x for x in columns_included if x in columns]
    else:
        eligible_columns = None

    hed_obj_list = get_assembled_strings(data_input, expand_defs=expand_defs)
    hed_string_list = [str(hed) for hed in hed_obj_list]
    if not eligible_columns:
        df = pd.DataFrame({"HED_assembled": hed_string_list})
    else:
        df = data_input.dataframe[eligible_columns].copy(deep=True)
        df['HED_assembled'] = hed_string_list
    definitions = data_input.get_definitions().gathered_defs
    return df, definitions


def get_assembled_strings(table, hed_schema=None, expand_defs=False):
    """ Return HED string objects for a tabular file.

    Parameters:
        table (TabularInput): The input file to be searched.
        hed_schema (HedSchema or HedschemaGroup): If provided the HedStrings are converted to canonical form.
        expand_defs (bool): If True, definitions are expanded when the events are assembled.

    Returns:
        list: A list of HedString or HedStringGroup objects.

    """
    hed_list = list(table.iter_dataframe(hed_ops=[hed_schema], return_string_only=True,
                                         expand_defs=expand_defs, remove_definitions=True))
    return hed_list


def search_tabular(data_input, hed_schema, query, columns_included=None):
    """ Return a dataframe with results of query.

    Parameters:
        data_input (TabularInput): The tabular input file (e.g., events) to be searched.
        hed_schema (HedSchema or HedSchemaGroup):  The schema(s) under which to make the query.
        query (str or list):     The str query or list of string queries to make.
        columns_included (list or None):  List of names of columns to include

    Returns:
        DataFrame or None: A DataFrame with the results of the query or None if no events satisfied the query.

    """
    if columns_included:
        columns = frozenset(list(data_input.dataframe.columns))
        eligible_columns = [x for x in columns_included if x in columns]
    else:
        eligible_columns = None

    hed_list = get_assembled_strings(data_input, hed_schema=hed_schema, expand_defs=True)
    expression = TagExpressionParser(query)
    hed_tags = []
    row_numbers = []
    for index, next_item in enumerate(hed_list):
        match = expression.search_hed_string(next_item)
        if not match:
            continue
        hed_tags.append(next_item)
        row_numbers.append(index)

    if not row_numbers:
        df = None
    elif not eligible_columns:
        df = pd.DataFrame({'row_number': row_numbers, 'HED_assembled': hed_tags})
    else:
        df = data_input.dataframe.iloc[row_numbers][eligible_columns].reset_index()
        df.rename(columns={'index': 'row_number'})
    return df
