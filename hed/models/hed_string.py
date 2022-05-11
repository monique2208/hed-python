"""
This module is used to split tags in a HED string.
"""
from hed.models.hed_group import HedGroup
from hed.models.hed_tag import HedTag
from hed.errors.error_reporter import ErrorHandler, check_for_any_errors
from hed.errors.error_types import ErrorContext
from hed.models.hed_ops import translate_ops
from hed.models.model_constants import DefTagNames
from hed.models.hed_group import HedGroupFrozen
from hed.models.hed_group_base import HedGroupBase


class HedString(HedGroup):
    """Class representing a HED string."""

    OPENING_GROUP_CHARACTER = '('
    CLOSING_GROUP_CHARACTER = ')'

    def __init__(self, hed_string, hed_schema=None):
        """ Constructor for the HedString class.

        Args:
            hed_string (str): A HED string consisting of tags and tag groups.

        Notes:
            The HedString object parses its component tags and groups into a tree-like structure.

        """

        try:
            contents = self.split_into_groups(hed_string, hed_schema)
        except ValueError:
            contents = []

        super().__init__(hed_string, contents=contents, startpos=0, endpos=len(hed_string))

    @property
    def is_group(self):
        """ Returns False since the underlying string is not a group with parentheses. """
        return False

    def convert_to_canonical_forms(self, hed_schema):
        """ Identify all tags using the given schema. If no schema, still identify "key" tags such as definitions.

        Args:
            hed_schema (HedSchema or None): The schema to use to validate/convert tags.

        Returns:
            list: A list of issues found while converting the string. Each issue is a dictionary.

        Notes:
            Sets the "isDefinition" property on tags and groups.

        """
        validation_issues = []
        for tag in self.get_all_tags():
            validation_issues += tag.convert_to_canonical_forms(hed_schema)

        return validation_issues

    def remove_definitions(self):
        """ Remove definition tags and groups from this string.

        Returns:
            list: An empty list as there are no possible issues, this list is always blank.

        """
        definition_groups = self.find_top_level_tags({DefTagNames.DEFINITION_KEY}, include_groups=1)
        if definition_groups:
            self.remove(definition_groups)

        return []

    def convert_to_short(self, hed_schema):
        """ Compute string canonical forms and return the short form and issues.

        Args:
            hed_schema (HedSchema or None): The schema to use to calculate forms.

        Returns:
            str:   The string with all tags converted to short form.
            list:  A list of issues found during conversion. Each issue is a dictionary.

        Notes:
            No issues will be found if no schema is passed.

        """
        conversion_issues = self.convert_to_canonical_forms(hed_schema)
        short_string = self.get_as_short()
        return short_string, conversion_issues

    def convert_to_long(self, hed_schema):
        """ Compute string canonical forms and return the long form and issues.

        Args:
            hed_schema (HedSchema or None): The schema to use to calculate forms.

        Returns:
            str:   The string with all tags converted to long form.
            list:  A list of issues found during conversion.  Each issue is a dictionary.

        Notes:
            No issues will be found if no schema is passed.

        """
        conversion_issues = self.convert_to_canonical_forms(hed_schema)
        short_string = self.get_as_long()
        return short_string, conversion_issues

    def convert_to_original(self):
        """ Return the original form of this string, though potentially with some extraneous spaces removed.

        Returns:
            str: The string with all the tags in their original form.

        """
        return self.get_as_form("org_tag")

    @staticmethod
    def split_into_groups(hed_string, hed_schema=None):
        """ Split the HED string into a tree of tag groups, tags, and delimiters.

        Args:
            hed_string (str): A hed string consisting of tags and tag groups to be processed.
            hed_schema (HedSchema or None): Hed schema to use to identify tags.

        Returns:
            list:  A list of HedTag and/or HedGroupBase.

        Raises:
            ValueError: If the string is significantly malformed, such as mismatched parentheses.

        """

        current_tag_group = [[]]

        input_tags = HedString.split_hed_string(hed_string)
        for is_hed_tag, (startpos, endpos) in input_tags:
            if is_hed_tag:
                new_tag = HedTag(hed_string, (startpos, endpos), hed_schema)
                current_tag_group[-1].append(new_tag)
            else:
                string_portion = hed_string[startpos:endpos]
                delimiter_index = 0
                for i, char in enumerate(string_portion):
                    if not char.isspace():
                        delimiter_index = i
                        break

                delimiter_char = string_portion[delimiter_index]

                if delimiter_char is HedString.OPENING_GROUP_CHARACTER:
                    current_tag_group.append(HedGroup(hed_string, startpos + delimiter_index))

                if delimiter_char is HedString.CLOSING_GROUP_CHARACTER:
                    # Terminate existing group, and save it off.
                    paren_end = startpos + delimiter_index + 1

                    if len(current_tag_group) > 1:
                        new_group = current_tag_group.pop()
                        new_group._endpos = paren_end

                        current_tag_group[-1].append(new_group)
                    else:
                        raise ValueError(f"Closing parentheses in hed string {hed_string}")

        # Comma delimiter issues are ignored and assumed already validated currently.
        if len(current_tag_group) != 1:
            raise ValueError(f"Unmatched opening parentheses in hed string {hed_string}")

        return current_tag_group[0]

    def _get_org_span(self, tag_or_group):
        """ If this tag or group was in the original hed string, find its original span.

        Args:
            tag_or_group (HedTag or HedGroup): The hed tag to locate in this string.

        Returns:
            int or None:   Starting position of the given item in the original string.
            int or None:   Ending position of the given item in the original string.

        Notes:
            If the hed tag or group was not in the original string, returns (None, None).

        """
        if self.check_if_in_original(tag_or_group):
            return tag_or_group.span

        return None, None

    @staticmethod
    def split_hed_string(hed_string):
        """ Split a HED string into delimiters and tags.

        Args:
            hed_string (str): The HED string to split.

        Returns:
            list:  A list of tuples where each tuple is (is_hed_tag, (start_pos, end_pos)).

        Notes:
            The tuple format is as follows
                is_hed_tag (bool): A (possible) hed tag if true, delimiter if not.
                start_pos (int):   Index of start of string in hed_string.
                end_pos (int):     Index of end of string in hed_string

            This function does not validate tags or delimiters in any form.

        """
        tag_delimiters = ",()"
        current_spacing = 0
        found_symbol = True
        result_positions = []
        tag_start_pos = None
        last_end_pos = 0
        for i, char in enumerate(hed_string):
            if char == " ":
                current_spacing += 1
                continue

            if char in tag_delimiters:
                if found_symbol:
                    # view_string = hed_string[last_end_pos: i]
                    if last_end_pos != i:
                        result_positions.append((False, (last_end_pos, i)))
                    last_end_pos = i
                elif not found_symbol:
                    found_symbol = True
                    last_end_pos = i - current_spacing
                    # view_string = hed_string[tag_start_pos: last_end_pos]
                    result_positions.append((True, (tag_start_pos, last_end_pos)))
                    current_spacing = 0
                    tag_start_pos = None
                continue

            # If we have a current delimiter, end it here.
            if found_symbol and last_end_pos is not None:
                # view_string = hed_string[last_end_pos: i]
                if last_end_pos != i:
                    result_positions.append((False, (last_end_pos, i)))
                last_end_pos = None

            found_symbol = False
            current_spacing = 0
            if tag_start_pos is None:
                tag_start_pos = i

        if last_end_pos is not None and len(hed_string) != last_end_pos:
            # view_string = hed_string[last_end_pos: len(hed_string)]
            result_positions.append((False, (last_end_pos, len(hed_string))))
        if tag_start_pos is not None:
            # view_string = hed_string[tag_start_pos: len(hed_string)]
            result_positions.append((True, (tag_start_pos, len(hed_string) - current_spacing)))
            if current_spacing:
                result_positions.append((False, (len(hed_string) - current_spacing, len(hed_string))))

        return result_positions

    def apply_funcs(self, string_funcs):
        """ Run the list of functions on this string and gather issues found.

        Args:
            string_funcs (list): A list of functions that take a hed string object and return a list of issues.

        Returns:
            list: A list of issues found by these operations. Each issue is a dictionary.

        Notes:
            This method potentially modifies the hed string object.

        """
        string_issues = []
        for string_func in string_funcs:
            string_issues += string_func(self)
            if string_issues:
                if check_for_any_errors(string_issues):
                    break

        return string_issues

    def validate(self, hed_ops=None, error_handler=None, **kwargs):
        """ Run the given hed_ops on this string.

        Args:
            hed_ops: (func, HedOps, or list): Operations to apply to this object.
            error_handler (ErrorHandler or None): Used to report errors in context.  Uses a default if None.
            kwargs:
                See models.hed_ops.translate_ops or the specific hed_ops for additional options

        Returns:
            list:  A list of issues encountered in applying these operations. Each issue is a dictionary.

        Notes:
            Although this function is called validation, the HedOps can represent other transformations.

        """
        if error_handler is None:
            error_handler = ErrorHandler()
        tag_funcs = translate_ops(hed_ops, **kwargs)

        error_handler.push_error_context(ErrorContext.HED_STRING, self, increment_depth_after=False)
        issues = self.apply_funcs(tag_funcs)
        error_handler.add_context_to_issues(issues)
        error_handler.pop_error_context()

        return issues

    def get_frozen(self):
        """ Return a frozen copy of this HedString.

        Returns:
            HedStringFrozen: A frozen copy of this HedString.

        Notes:
            Tags in frozen hed strings still point to the same place.  Do not alter them.

        """
        return HedStringFrozen(self)

    def find_top_level_tags(self, anchor_tags, include_groups=2):
        """ Find top level groups containing the given anchor tags.

        Args:
            anchor_tags (container):     A list/set/etc of short_base_tags to find groups by.
            include_groups (0, 1 or 2):  Parameter indicating what return values to include.

        Returns:
            list:
            tag (HedTag): The located tag
            group (HedGroup): The group the located tag is in.

        Notes:
            The include_groups parameter meanings are:
                If 0: return only tags.
                If 1: return only groups.
                If 2 or any other value: return both.

            A max of 1 tag located her top level group.

        """
        top_level_tags = []
        for group in self.groups():
            for tag in group.tags():
                if tag.short_base_tag.lower() in anchor_tags:
                    top_level_tags.append((tag, group))
                    # Only capture a max of 1 per group.  These are implicitly unique.
                    break

        if include_groups == 0 or include_groups == 1:
            return [tag[include_groups] for tag in top_level_tags]
        return top_level_tags


class HedStringFrozen(HedGroupFrozen):
    """ Class representing an immutable hed string. """

    def __init__(self, hed_string, hed_schema=None):
        """ Initialize a frozen hed string object.

        Args:
            hed_string (HedGroupBase or str): The source HED string.
            hed_schema (HedSchema, HedSchemaGroup or None): HedSchema to use to identify tags if hed_string is a str.
        """
        if isinstance(hed_string, HedGroupBase):
            contents = hed_string
            hed_string = hed_string._hed_string
        else:
            try:
                contents = HedString.split_into_groups(hed_string, hed_schema)
            except ValueError:
                contents = []

        super().__init__(contents, hed_string)

    @property
    def is_group(self):
        """ Return False since this does not have parentheses. """
        return False

    # Other functions from HedString we want.
    apply_funcs = HedString.apply_funcs
    validate = HedString.validate
    find_top_level_tags = HedString.find_top_level_tags
