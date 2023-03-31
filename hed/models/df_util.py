from functools import partial
import pandas as pd

from hed.models.sidecar import Sidecar
from hed.models.tabular_input import TabularInput
from hed import HedString
from hed.models.definition_dict import DefinitionDict
from hed.models.definition_entry import DefinitionEntry


def get_assembled(tabular_file, sidecar, hed_schema, extra_def_dicts=None, join_columns=True,
                  shrink_defs=False, expand_defs=True):
    """Load a tabular file and its associated HED sidecar file.

    Args:
        tabular_file: str or TabularInput
            The path to the tabular file, or a TabularInput object representing it.
        sidecar: str or Sidecar
            The path to the sidecar file, or a Sidecar object representing it.
        hed_schema: HedSchema
            If str, will attempt to load as a version if it doesn't have a valid extension.
        extra_def_dicts: list of DefinitionDict, optional
            Any extra DefinitionDict objects to use when parsing the HED tags.
        join_columns: bool
            If true, join all hed columns into one.
        shrink_defs: bool
            Shrink any def-expand tags found
        expand_defs: bool
            Expand any def tags found
    Returns:
        tuple: A list of HedStrings or a list of lists of HedStrings, DefinitionDict
        
    """
    if isinstance(sidecar, str):
        sidecar = Sidecar(sidecar)

    if isinstance(tabular_file, str):
        tabular_file = TabularInput(tabular_file, sidecar)

    def_dict = None
    if sidecar:
        def_dict = sidecar.get_def_dict(hed_schema=hed_schema, extra_def_dicts=extra_def_dicts)

    if join_columns:
        if expand_defs:
            return [HedString(x, hed_schema, def_dict).expand_defs() for x in tabular_file.series_a], def_dict
        elif shrink_defs:
            return [HedString(x, hed_schema, def_dict).shrink_defs() for x in tabular_file.series_a], def_dict
        else:
            return [HedString(x, hed_schema, def_dict) for x in tabular_file.series_a], def_dict
    else:
        return [[HedString(x, hed_schema, def_dict).expand_defs() if expand_defs
                 else HedString(x, hed_schema, def_dict).shrink_defs() if shrink_defs
                 else HedString(x, hed_schema, def_dict)
                 for x in text_file_row] for text_file_row in tabular_file.dataframe_a.itertuples(index=False)], def_dict


def convert_to_form(df, hed_schema, tag_form, columns=None):
    """ Convert all tags in underlying dataframe to the specified form.

        Converts in place
    Parameters:
        df (pd.Dataframe): The dataframe to modify
        hed_schema (HedSchema): The schema to use to convert tags.
        tag_form(str): HedTag property to convert tags to.
        columns (list): The columns to modify on the dataframe
    """
    if isinstance(df, pd.Series):
        df = df.apply(partial(_convert_to_form, hed_schema=hed_schema, tag_form=tag_form))
    else:
        if columns is None:
            columns = df.columns

        for column in columns:
            df[column] = df[column].apply(partial(_convert_to_form, hed_schema=hed_schema, tag_form=tag_form))

    return df


def shrink_defs(df, hed_schema, columns=None):
    """ Shrinks any def-expand tags found in the specified columns in the dataframe.

        Converts in place
    Parameters:
        df (pd.Dataframe or pd.Series): The dataframe or series to modify
        hed_schema (HedSchema or None): The schema to use to identify defs.
        columns (list or None): The columns to modify on the dataframe.
    """
    if isinstance(df, pd.Series):
        mask = df.str.contains('Def-expand/', case=False)
        df[mask] = df[mask].apply(partial(_shrink_defs, hed_schema=hed_schema))
    else:
        if columns is None:
            columns = df.columns

        for column in columns:
            mask = df[column].str.contains('Def-expand/', case=False)
            df[column][mask] = df[column][mask].apply(partial(_shrink_defs, hed_schema=hed_schema))

    return df


def expand_defs(df, hed_schema, def_dict, columns=None):
    """ Expands any def tags found in the dataframe.

        Converts in place

    Parameters:
        df (pd.Dataframe or pd.Series): The dataframe or series to modify
        hed_schema (HedSchema or None): The schema to use to identify defs
        def_dict (DefinitionDict): The definitions to expand
        columns (list or None): The columns to modify on the dataframe
    """
    if isinstance(df, pd.Series):
        mask = df.str.contains('Def/', case=False)
        df[mask] = df[mask].apply(partial(_expand_defs, hed_schema=hed_schema, def_dict=def_dict))
    else:
        if columns is None:
            columns = df.columns

        for column in columns:
            mask = df[column].str.contains('Def/', case=False)
            df[column][mask] = df[column][mask].apply(partial(_expand_defs, hed_schema=hed_schema, def_dict=def_dict))

    return df


def _convert_to_form(hed_string, hed_schema, tag_form):
    from hed import HedString
    return str(HedString(hed_string, hed_schema).get_as_form(tag_form))


def _shrink_defs(hed_string, hed_schema):
    from hed import HedString
    return str(HedString(hed_string, hed_schema).shrink_defs())


def _expand_defs(hed_string, hed_schema, def_dict):
    from hed import HedString
    return str(HedString(hed_string, hed_schema, def_dict).expand_defs())


def process_def_expands(hed_strings, hed_schema, known_defs=None, ambiguous_defs=None):
    """
    Processes a list of HED strings according to a given HED schema, using known definitions and ambiguous definitions.

    Args:
        hed_strings (list or pd.Series): A list of HED strings to process.
        hed_schema (HedSchema): The schema to use
        known_defs (DefinitionDict or list or str), optional):
            A DefinitionDict or anything its constructor takes.  These are the known definitions going in, that must
            match perfectly.
        ambiguous_defs (dict): A dictionary containing ambiguous definitions
            format TBD.  Currently def name key: list of lists of hed tags values
    """
    if not isinstance(hed_strings, pd.Series):
        hed_strings = pd.Series(hed_strings)

    if ambiguous_defs is None:
        ambiguous_defs = {}
    errors = {}
    def_dict = DefinitionDict(known_defs)

    def_expand_mask = hed_strings.str.contains('Def-Expand/', case=False)

    # Iterate over the strings that contain def-expand tags
    for i in hed_strings[def_expand_mask].index:
        string = hed_strings.loc[i]
        hed_str = HedString(string, hed_schema)

        for def_tag, def_expand_group, def_group in hed_str.find_def_tags(recursive=True):
            if def_tag == def_expand_group:
                continue

            def_tag_name = def_tag.extension.split('/')[0]
            # First check for known definitions.  If this is known, it's done either way.
            def_group_contents = def_dict._get_definition_contents(def_tag)
            def_expand_group.sort()
            if def_group_contents:
                if def_group_contents != def_expand_group:
                    errors.setdefault(def_tag_name.lower(), []).append(def_group)
                continue

            has_extension = "/" in def_tag.extension

            # If there's no extension, this is fine.
            if not has_extension:
                group_tag = def_expand_group.get_first_group()
                def_dict.defs[def_tag_name.lower()] = DefinitionEntry(name=def_tag_name, contents=group_tag,
                                                                      takes_value=False,
                                                                      source_context=[])
            else:
                def_extension = def_tag.extension.split('/')[-1]
                # Find any other tags in def_group.get_all_tags() with tags with the same extension
                matching_tags = [tag for tag in def_expand_group.get_all_tags() if tag.extension == def_extension and tag != def_tag]

                for tag in matching_tags:
                    tag.extension = "#"

                group_tag = def_expand_group.get_first_group()

                these_defs = ambiguous_defs.setdefault(def_tag_name.lower(), [])
                these_defs.append(group_tag)

                value_per_tag = []
                if len(these_defs) >= 1:
                    all_tags_list = [group.get_all_tags() for group in these_defs]
                    for tags in zip(*all_tags_list):
                        value_per_tag.append(next((tag.extension for tag in tags if tag.extension != "#"), None))
                    ambiguous_values = value_per_tag.count(None)
                    if ambiguous_values == 1:
                        new_contents = group_tag.copy()
                        for tag, value in zip(new_contents.get_all_tags(), value_per_tag):
                            if value is not None:
                                tag.extension = f"/{value}"
                        def_dict.defs[def_tag_name.lower()] = DefinitionEntry(name=def_tag_name, contents=new_contents,
                                                                              takes_value=True,
                                                                              source_context=[])
                        del ambiguous_defs[def_tag_name.lower()]

    return def_dict, ambiguous_defs, errors