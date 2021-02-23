"""
Examples of creating a HedValidator and validating various spreadsheets using it.
Also contains examples of catching HedFileErrors for invalid input.

Classes Demonstrated:
HedFileInput - Used to open/modify/save a spreadsheet
HedValidator - Validates a given input string or file
HedFileError - Exception thrown when a file cannot be opened.(parsing error, file not found error, etc)
"""

import os
import hed

from hed.validator.hed_validator import HedValidator
from hed.util.hed_file_input import HedFileInput
from hed.util.exceptions import HedFileError

if __name__ == '__main__':
    # Set up the file names for the tests
    local_hed_file = 'data/HED7.1.1.xml'  # path HED v7.1.1 stored locally
    example_data_path = 'data'  # path to example data
    valid_tsv_file = os.path.join(example_data_path, 'ValidTwoColumnHED7_1_1.tsv')
    valid_tsv_file_no_header = os.path.join(example_data_path, 'ValidTwoColumnHED7_1_1NoHeader.tsv')
    valid_tsv_file_separate_cols = os.path.join(example_data_path, 'ValidSeparateColumnTSV.txt')
    unsupported_csv_format = os.path.join(example_data_path, 'UnsupportedFormatCSV.csv')
    multiple_sheet_xlsx_file = os.path.join(example_data_path, 'ExcelMultipleSheets.xlsx')

    hed_validator_old = HedValidator(xml_version_number='7.1.1')
    hed_validator_local = HedValidator(hed_xml_file=local_hed_file)
    hed_validator_local_warnings = HedValidator(hed_xml_file=local_hed_file, check_for_warnings=True
                                                )
    # Example 1a: Valid TSV file with default version of HED
    print(valid_tsv_file)
    input_file = HedFileInput(valid_tsv_file, tag_columns=[2])
    validation_issues = hed_validator_old.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 1a] ValidTwoColumnHED7_1_1 is probably okay with default version of HED'))

    # Example 1b: Valid TSV file with specified local version of HED
    print(valid_tsv_file)
    input_file = HedFileInput(valid_tsv_file, tag_columns=[2])
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 1b] ValidTwoColumnHED7_1_1 should have no issues with local version 7.1.1'))

    # Example 1c: Valid TSV file with specified local version of HED and no column headers
    print(valid_tsv_file_no_header)
    input_file = HedFileInput(valid_tsv_file_no_header, has_column_names=False, tag_columns=[2, 3])
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 1c] ValidTwoColumnHED7_1_1NoHeaders should have no issues with version 7.1.1'))

    # Example 1d: Valid TSV with separate columns for required fields
    print(valid_tsv_file_separate_cols)
    prefixed_needed_tag_columns = {3: 'Event/Description/', 4: 'Event/Label/', 5: 'Event/Category/'}
    input_file = HedFileInput(valid_tsv_file_separate_cols, tag_columns=[6],
                              column_prefix_dictionary=prefixed_needed_tag_columns)
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 1d] Valid TSV with required tags in separate columns'))

    # Example 2a: CSV files are not supported (Use excel to convert)
    print(unsupported_csv_format)
    prefixed_needed_tag_columns = {3: 'Event/Description/', 4: 'Event/Label/', 5: 'Event/Category/'}
    try:
        input_file = HedFileInput(unsupported_csv_format, tag_columns=[6],
                                  column_prefix_dictionary=prefixed_needed_tag_columns)
    except HedFileError as e:
        print('[Example 2a] csv is unsupported format, but this call treats file name as HED string')
        print(e.format_error_message(return_string_only=True))

    # Example 2b: CSV files are not supported (Use excel to convert) - explicit file specification
    print(unsupported_csv_format)
    prefixed_needed_tag_columns = {3: 'Event/Description/', 4: 'Event/Label/', 5: 'Event/Category/'}
    try:
        input_file = HedFileInput(unsupported_csv_format, tag_columns=[6],
                                  column_prefix_dictionary=prefixed_needed_tag_columns)
    except HedFileError as e:
        print("['Example 2b] csv is unsupported format, now have right error message")
        print(e.format_error_message(return_string_only=True))

    prefixed_needed_tag_columns = {2: 'Event/Label/', 3: 'Event/Description/'}
    # Example 3a: XLSX file with multiple sheets - first sheet has no issues with 7.1.1
    input_file = HedFileInput(multiple_sheet_xlsx_file, tag_columns=[4],
                              column_prefix_dictionary=prefixed_needed_tag_columns,
                              worksheet_name='LKT Events')
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3a] Multiple sheet xlsl has LKT Events sheet with no issues'))

    # Example 3b: Valid XLSX file with multiple sheets - first sheet probably has no issues with default schema
    input_file = HedFileInput(multiple_sheet_xlsx_file, tag_columns=[4],
                              column_prefix_dictionary=prefixed_needed_tag_columns,
                              worksheet_name='LKT Events')
    validation_issues = hed_validator_local_warnings.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3b] LKT Events sheet probably has with no issues with the default schema'))

    # Example 3c: XLSX file with multiple sheets - assumes first sheet by default
    input_file = HedFileInput(multiple_sheet_xlsx_file, tag_columns=[4],
                              column_prefix_dictionary=prefixed_needed_tag_columns)
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3c] Multiple sheet xlsl has first sheet with no issues'))

    # Example 3d: XLSX file with multiple sheets - PVT sheet has several issues with 7.1.1
    input_file = HedFileInput(multiple_sheet_xlsx_file, tag_columns=[4],
                              column_prefix_dictionary=prefixed_needed_tag_columns,
                              worksheet_name='PVT Events')
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3d] Some PVT worksheet issues that are due to removal of extensionAllowed in places in 7.1.1'))

    # Example 3e: Invalid XLSX sheet with 7.1.1
    input_file = HedFileInput(multiple_sheet_xlsx_file, tag_columns=[4], worksheet_name='DAS Events')
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3e] DAS Events worksheet has multiple issues with 7.1.1'))

    # Example 3f: Invalid XLSX sheet with 7.1.1 - also can't duplicate Label and Description in 7.1.1
    input_file = HedFileInput(multiple_sheet_xlsx_file, tag_columns=[4],
                              column_prefix_dictionary=prefixed_needed_tag_columns,
                              worksheet_name='DAS Events')
    validation_issues = hed_validator_local.validate_input(input_file)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3f] DAS worse now because of duplicate Label and Description specifications'))
