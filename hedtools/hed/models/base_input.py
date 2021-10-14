import os
import openpyxl
import pandas
import copy

from hed.models.def_dict import DefDict
from hed.models.column_mapper import ColumnMapper
from hed.errors.exceptions import HedFileError, HedExceptions
from hed.errors.error_types import ErrorContext
from hed.errors.error_reporter import ErrorHandler
from hed.models import model_constants
from hed.models.util import translate_ops


class BaseInput:
    """Handles parsing the actual on disk hed files to a more general format."""
    TEXT_EXTENSION = ['.tsv', '.txt']
    EXCEL_EXTENSION = ['.xlsx']
    FILE_EXTENSION = [*TEXT_EXTENSION, *EXCEL_EXTENSION]
    STRING_INPUT = 'string'
    FILE_INPUT = 'file'
    TAB_DELIMITER = '\t'
    COMMA_DELIMITER = ','

    def __init__(self, file, file_type=None, worksheet_name=None, has_column_names=True, mapper=None, def_mapper=None,
                 name=None):
        """Constructor for the BaseInput class.

         Parameters
         ----------
         file: str or file like
             An xlsx/tsv file to open.
         file_type: str
            ".xlsx" for excel, ".tsv" or ".txt" for tsv. data.  Derived from file if file is a str.
         worksheet_name: str or None
             The name of the Excel workbook worksheet that contains the HED tags.  Not applicable to tsv files.
         has_column_names: bool
             True if file has column names. The validation will skip over the first line of the file. False, if
             otherwise.
         mapper: ColumnMapper
             Pass in a built column mapper(see HedInput or EventsInput for examples), or None to just
             retrieve all columns as hed tags.
         name: str or None
            Optional field for how this file will report errors.
         """
        if mapper is None:
            mapper = ColumnMapper()
        self._mapper = mapper
        self._def_mapper = def_mapper
        self._has_column_names = has_column_names
        self._name = name
        # This is the loaded workbook if we loaded originally from an excel file.
        self._loaded_workbook = None
        self._worksheet_name = worksheet_name
        self.file_def_dict = None
        pandas_header = 0
        if not self._has_column_names:
            pandas_header = None

        if not file:
            raise HedFileError(HedExceptions.FILE_NOT_FOUND, "Empty file passed to BaseInput.", file)

        input_type = file_type
        if file_type is None and isinstance(file, str):
            _, input_type = os.path.splitext(file)
            if self.name is None:
                self._name = file

        self._dataframe = None

        if input_type in self.TEXT_EXTENSION:
            self._dataframe = pandas.read_csv(file, delimiter='\t', header=pandas_header, dtype=str)
        elif input_type in self.EXCEL_EXTENSION:
            self._loaded_workbook = openpyxl.load_workbook(file)
            loaded_worksheet = self.get_worksheet(self._worksheet_name)
            self._dataframe = self._get_dataframe_from_worksheet(loaded_worksheet, has_column_names)
        else:
            raise HedFileError(HedExceptions.INVALID_EXTENSION, "", file)

        self.reset_mapper(mapper)

    def reset_mapper(self, new_mapper):
        """
            Set the column mapper to the passed in one, allowing you to view the file differently.

        Parameters
        ----------
        new_mapper : ColumnMapper
        """
        self._mapper = new_mapper
        if not self._mapper:
            self._mapper = ColumnMapper()

        if self._dataframe is not None and self._has_column_names:
            columns = self._dataframe.columns
            self._mapper.set_column_map(columns)

        self.file_def_dict = self.extract_definitions()

        self.update_definition_mapper_with_file(self.file_def_dict)

    @property
    def dataframe(self):
        return self._dataframe

    @property
    def name(self):
        return self._name

    @property
    def has_column_names(self):
        return self._has_column_names

    @property
    def loaded_workbook(self):
        return self._loaded_workbook

    @property
    def worksheet_name(self):
        return self._worksheet_name

    def _convert_to_form(self, hed_schema, tag_form, error_handler):
        """
        Converts all tags in a given spreadsheet to a given form

        Parameters
        ----------
        hed_schema : HedSchema
            The schema to use to convert tags.
        tag_form: str
            The form to convert the tags to.  (short_tag, long_tag, base_tag, etc)
        error_handler : ErrorHandler
            The error handler to use for context, uses a default one if none.

        Returns
        -------
        issues_list: [{}]
            A list of issues found during conversion
        """
        if error_handler is None:
            error_handler = ErrorHandler()
        error_list = []
        for row_number, column_to_hed_tags_dictionary in self:
            error_handler.push_error_context(ErrorContext.ROW, row_number)
            for column_number in column_to_hed_tags_dictionary:
                error_handler.push_error_context(ErrorContext.COLUMN, column_number)
                column_hed_string = column_to_hed_tags_dictionary[column_number]
                error_list += column_hed_string.convert_to_canonical_forms(hed_schema)
                self.set_cell(row_number, column_number, column_hed_string,
                              include_column_prefix_if_exist=False, tag_form=tag_form)
                error_handler.pop_error_context()
            error_handler.pop_error_context()

        return error_list

    def convert_to_short(self, hed_schema, error_handler=None):
        """
        Converts all tags in a given spreadsheet to short form

        Parameters
        ----------
        hed_schema : HedSchema
            The schema to use to convert tags.
        error_handler : ErrorHandler
            The error handler to use for context, uses a default one if none.

        Returns
        -------
        issues_list: [{}]
            A list of issues found during conversion
        """
        return self._convert_to_form(hed_schema, "short_tag", error_handler)

    def convert_to_long(self, hed_schema, error_handler=None):
        """
        Converts all tags in a given spreadsheet to long form

        Parameters
        ----------
        hed_schema : HedSchema
            The schema to use to convert tags.
        error_handler : ErrorHandler
            The error handler to use for context, uses a default one if none.

        Returns
        -------
        issues_list: [{}]
            A list of issues found during conversion
        """
        return self._convert_to_form(hed_schema, "long_tag", error_handler)

    def to_excel(self, file, output_processed_file=False):
        """

        Parameters
        ----------
        file : str or file like
            Location to save this file.  Can be file, or stream/file like.
        output_processed_file : bool
            Replace all definitions and labels in HED columns as appropriate.  Also fills in things like categories.
        Returns
        -------

        """
        if not file:
            raise ValueError("Empty file name or object passed in to BaseInput.save.")

        # For now just make a copy if we want to save a formatted copy.  Could optimize this further.
        if output_processed_file:
            output_file = self._get_processed_copy()
        else:
            output_file = self

        if self._loaded_workbook:
            old_worksheet = self.get_worksheet(self._worksheet_name)
            # excel spreadsheets are 1 based, then add another 1 for column names if present
            adj_row_for_col_names = 1
            if self._has_column_names:
                adj_row_for_col_names += 1
            adj_for_one_based_cols = 1
            for row_number, text_file_row in output_file._dataframe.iterrows():
                for column_number, column_text in enumerate(text_file_row):
                    old_worksheet.cell(row_number + adj_row_for_col_names,
                                       column_number + adj_for_one_based_cols).value = \
                        output_file._dataframe.iloc[row_number, column_number]
            self._loaded_workbook.save(file)
        else:
            output_file._dataframe.to_excel(file, header=self._has_column_names)

    def to_csv(self, file=None, output_processed_file=False):
        """
            Returns the file as a csv string.

        Parameters
        ----------
        file : str or file like or None
            Location to save this file.  Can be file, or stream/file like.
        output_processed_file : bool
            Replace all definitions and labels in HED columns as appropriate.  Also fills in things like categories.
        Returns
        -------
        """
        # For now just make a copy if we want to save a formatted copy.  Could optimize this further.
        if output_processed_file:
            output_file = self._get_processed_copy()
        else:
            output_file = self
        csv_string_if_filename_none = output_file._dataframe.to_csv(file, '\t', index=False,
                                                                    header=output_file._has_column_names)
        return csv_string_if_filename_none

    def __iter__(self):
        return self.iter_dataframe()

    def iter_raw(self, validators=None, **kwargs):
        """Generates an iterator that goes over every row in the file without modification.

           This is primarily for altering or re-saving the original file.(eg convert short tags to long)

        Parameters
        validators : [func or validator like] or func or validator like
            A validator or list of validators to apply to the hed strings before returning
        kwargs:
            See util.translate_ops or the specific validators for additional options

        Yields
        -------
        row_number: int
            The current row number
        column_to_hed_tags_dictionary: dict
            A dict with keys column_number, value the cell at that position.
        """
        default_mapper = ColumnMapper()
        return self.iter_dataframe(default_mapper, validators, **kwargs)

    def iter_dataframe(self, mapper=None, return_row_dict=False, validators=None, expand_defs=False, **kwargs):
        """
        Generates a list of parsed rows based on the given column mapper.

        Parameters
        ----------
        mapper : ColumnMapper
            The column name to column number mapper
        return_row_dict: bool
            If True, this returns the full row_dict including issues.
            If False, returns just the HedStrings for each column
        validators : [func or validator like] or func or validator like
            A validator or list of validators to apply to the hed strings before returning
        expand_defs: bool
            If True, this will fully remove all definitions found and expand all def tags to def-expand tags
        kwargs:
            See util.translate_ops or the specific validators for additional options
        Yields
        -------
        row_number: int
            The current row number
        row_dict: dict
            A dict containing the parsed row, including: "HED", "column_to_hed_tags", and possibly "column_issues"
        """
        if mapper is None:
            mapper = self._mapper

        tag_ops = []
        if validators or expand_defs:
            if not isinstance(validators, list):
                validators = [validators]
            if expand_defs:
                validators.append(self._def_mapper)
            tag_ops = translate_ops(validators, expand_defs=expand_defs, **kwargs)

        start_at_one = 1
        if self._has_column_names:
            start_at_one += 1
        for row_number, text_file_row in self._dataframe.iterrows():
            # Skip any blank lines.
            if all(text_file_row.isnull()):
                continue

            row_dict = mapper.expand_row_tags(text_file_row)
            column_to_hed_tags = row_dict[model_constants.COLUMN_TO_HED_TAGS]
            if tag_ops:
                for hed_string_obj in column_to_hed_tags.values():
                    hed_string_obj.apply_ops(tag_ops)
            if return_row_dict:
                yield row_number + start_at_one, row_dict
            else:
                yield row_number + start_at_one, column_to_hed_tags

    def set_cell(self, row_number, column_number, new_string_obj, include_column_prefix_if_exist=False,
                 tag_form="short_tag"):
        """

        Parameters
        ----------
        row_number : int
            The row number of the spreadsheet to set
        column_number : int
            The column number of the spreadsheet to set
        new_string_obj : HedString
            Text to enter in the given cell
        include_column_prefix_if_exist : bool
            If true and the column matches one from mapper _column_prefix_dictionary, remove the prefix
        tag_form: str
            The version of the tags we would like to use from the hed string.(short_tag, long_tag, base_tag, etc)
            Any attribute of a HedTag that returns a string is valid.
        Returns
        -------

        """
        if self._dataframe is None:
            raise ValueError("No data frame loaded")

        transform_func = None
        if not include_column_prefix_if_exist:
            transform_func = self._mapper.get_prefix_remove_func(column_number)

        new_text = new_string_obj.get_as_form(tag_form, transform_func)
        adj_row_number = 1
        if self._has_column_names:
            adj_row_number += 1
        self._dataframe.iloc[row_number - adj_row_number, column_number - 1] = new_text

    @staticmethod
    def _get_row_hed_tags_from_dict(row_dict):
        """Reads in the current row of HED tags from the Excel file. The hed tag columns will be concatenated to form a
           HED string.

        Parameters
        ----------
        row_dict: dict
            Contains the parsed info from a specific worksheet row.
            the "HED" entry contains the combined hed string for a given row
        Returns
        -------
        hed_string: str
            a HED string containing the concatenated HED tag columns.
        column_tags_dict: dict
            dictionary which associates columns with HED tags

        """
        return row_dict[model_constants.ROW_HED_STRING], row_dict[model_constants.COLUMN_TO_HED_TAGS]

    def get_worksheet(self, worksheet_name=None):
        """
            Returns the requested worksheet from the workbook by name

        Parameters
        ----------
        worksheet_name : str
            Returns the requested worksheet by name, or the first one if no name passed in.
        Returns
        -------
        worksheet
        """
        if worksheet_name and self._loaded_workbook:
            # return self._loaded_workbook.get_sheet_by_name(worksheet_name)
            return self._loaded_workbook[worksheet_name]
        elif self._loaded_workbook:
            return self._loaded_workbook.worksheets[0]
        else:
            return None

    def get_def_and_mapper_issues(self, error_handler):
        """
            Returns formatted issues found with definitions and columns.
        Parameters
        ----------
        error_handler : ErrorHandler
            The error handler to use
        Returns
        -------
        issues_list: [{}]
            A list of definition and mapping issues.
        """
        issues = []
        issues += self.file_def_dict.get_definition_issues()

        # Gather any issues from the mapper for things like missing columns.
        mapper_issues = self._mapper.get_column_mapping_issues()
        error_handler.add_context_to_issues(mapper_issues)
        issues += mapper_issues
        return issues

    def _get_processed_copy(self):
        """
        Returns a copy of this file with processing applied(definitions replaced, columns expanded, etc)

        Returns
        -------
        file_copy: BaseInput
            The copy.
        """
        output_file = copy.deepcopy(self)
        for row_number, column_to_hed_tags_dictionary in self:
            for column_number in column_to_hed_tags_dictionary:
                new_text = column_to_hed_tags_dictionary[column_number]
                output_file.set_cell(row_number, column_number, new_text)

        return output_file

    @staticmethod
    def _get_dataframe_from_worksheet(worksheet, has_headers):
        """
        Creates a pandas dataframe from the given worksheet object

        Parameters
        ----------
        worksheet : Worksheet
            The loaded worksheet to convert
        has_headers : bool
            If this worksheet has column headers or not.

        Returns
        -------
        dataframe: DataFrame
            The converted data frame.
        """
        if has_headers:
            data = worksheet.values
            # first row is columns
            cols = next(data)
            data = list(data)
            return pandas.DataFrame(data, columns=cols)
        else:
            return pandas.DataFrame(worksheet.values)

    def _run_validators(self, validators, error_handler, run_on_raw=False, expand_defs=False, **kwargs):
        validation_issues = []
        if run_on_raw:
            tag_ops = translate_ops(validators, expand_defs=expand_defs, error_handler=error_handler,
                                    **kwargs)
            for row_number, column_to_hed_tags_dictionary in self.iter_raw():
                error_handler.push_error_context(ErrorContext.ROW, row_number)
                validation_issues = self._run_column_ops(column_to_hed_tags_dictionary, tag_ops,
                                                         [], error_handler)
                error_handler.pop_error_context()
        else:
            tag_ops, string_ops = translate_ops(validators, split_tag_and_string_ops=True, expand_defs=expand_defs,
                                                error_handler=error_handler, **kwargs)
            for row_number, row_dict in self.iter_dataframe(return_row_dict=True):
                error_handler.push_error_context(ErrorContext.ROW, row_number)
                validation_issues += self._run_row_column_ops(row_dict, string_ops, tag_ops, error_handler)
                error_handler.pop_error_context()

        return validation_issues

    def _run_row_column_ops(self, row_dict, row_ops, column_ops, error_handler):
        row_hed_string = row_dict[model_constants.ROW_HED_STRING]
        column_to_hed_tags_dictionary = row_dict[model_constants.COLUMN_TO_HED_TAGS]
        expansion_column_issues = row_dict.get(model_constants.COLUMN_ISSUES, {})
        validation_issues = self._run_column_ops(column_to_hed_tags_dictionary, column_ops, expansion_column_issues,
                                                 error_handler)
        validation_issues += self._run_row_ops(row_hed_string, row_ops, error_handler)
        return validation_issues

    def _run_column_ops(self, column_to_hed_tags_dictionary, column_ops, expansion_column_isssues, error_handler):
        validation_issues = []
        if column_to_hed_tags_dictionary:
            for column_number, column_hed_string in column_to_hed_tags_dictionary.items():
                new_column_issues = []
                error_handler.push_error_context(ErrorContext.COLUMN, column_number)
                error_handler.push_error_context(ErrorContext.HED_STRING, column_hed_string,
                                                 increment_depth_after=False)
                if column_number in expansion_column_isssues:
                    new_column_issues += expansion_column_isssues[column_number]

                new_column_issues += column_hed_string.apply_ops(column_ops)
                error_handler.add_context_to_issues(new_column_issues)
                error_handler.pop_error_context()
                error_handler.pop_error_context()
                validation_issues += new_column_issues

        return validation_issues

    def _run_row_ops(self, row_hed_string, row_ops, error_handler):
        error_handler.push_error_context(ErrorContext.HED_STRING, row_hed_string, increment_depth_after=False)
        row_issues = row_hed_string.apply_ops(row_ops)
        error_handler.add_context_to_issues(row_issues)
        error_handler.pop_error_context()
        return row_issues

    def validate_file(self, validators, name=None, error_handler=None, **kwargs):
        """Run the given validators on all columns and rows of the spreadsheet

        Parameters
        ----------

        validators : [func or validator like] or func or validator like
            A validator or list of validators to apply to the hed strings in this sidecar.
        name: str
            If present, will use this as the filename for context, rather than using the actual filename
            Useful for temp filenames.
        error_handler : ErrorHandler or None
            Used to report errors.  Uses a default one if none passed in.
        kwargs:
            See util.translate_ops or the specific validators for additional options
        Returns
        -------
        validation_issues: [{}]
            The list of validation issues found
        """
        if not name:
            name = self.name
        if not isinstance(validators, list):
            validators = [validators]

        if error_handler is None:
            error_handler = ErrorHandler()

        error_handler.push_error_context(ErrorContext.FILE_NAME, name)
        validation_issues = self.get_def_and_mapper_issues(error_handler)
        validators = validators.copy()
        if self._def_mapper:
            validators.append(self._def_mapper)
        validation_issues += self._run_validators(validators, error_handler=error_handler, **kwargs)
        error_handler.pop_error_context()

        return validation_issues

    def extract_definitions(self, error_handler=None):
        """
        Gathers and validates all definitions found in this spreadsheet

        Parameters
        ----------
        error_handler : ErrorHandler
            The error handler to use for context, uses a default one if none.

        Returns
        -------
        def_dict: DefDict
            Contains all the definitions located in the file
        """
        if error_handler is None:
            error_handler = ErrorHandler()
        new_def_dict = DefDict()
        validators = [new_def_dict]
        def_issues = self._run_validators(validators, run_on_raw=True, error_handler=error_handler)
        return new_def_dict

    def update_definition_mapper_with_file(self, def_dict):
        """
        Adds label definitions gathered from the given list of inputs if this has a definition mapper.

        Parameters
        ----------
        def_dict : DefDict
            The gathered definitions to add to the mapper.
        """
        if self._def_mapper is not None:
            self._def_mapper.add_definitions(def_dict)
