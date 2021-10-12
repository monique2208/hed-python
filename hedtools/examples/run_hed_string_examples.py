"""
Examples of creating a HedValidator and validating various Hed Strings with it.

Classes Demonstrated:
HedValidator - Validates a given input string or file
"""

import hed
from hed import schema
from hed.validator.hed_validator import HedValidator

if __name__ == '__main__':
    local_hed_file = 'data/HED7.1.1.xml'   # path HED v7.1.1 stored locally
    hed_schema = schema.load_schema(local_hed_file)
    hed_validator_old = HedValidator(hed_schema)
    hed_validator_current = HedValidator()
    hed_validator_no_semantic = HedValidator(run_semantic_validation=False)
    hed_string_test = "Sensory-event,Visual,Experimental-stimulus,Green,Non-target,(Letter/D, Center-of-Screen)\n\n\n"
    test_string = hed.HedString(hed_string_test)
    validation_issues = test_string.validate(hed_validator_old)
    print(hed.get_printable_issue_string(validation_issues,
                                         title='[Example 1a] hed_string_1 should have no issues with HEDv7.1.1'))

    # Example 1a: Valid HED string for HED <= v7.1.1
    hed_string_1 = 'Event/Label/ButtonPuskDeny, Event/Description/Button push to deny access to the ID holder,' \
                   'Event/Category/Participant response, ' \
                   '(Participant ~ Action/Button press/Keyboard ~ Participant/Effect/Body part/Arm/Hand/Finger)'
    test_string = hed.HedString(hed_string_1)
    validation_issues = test_string.validate(hed_validator_old)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 1a] hed_string_1 should have no issues with HEDv7.1.1'))

    # Example 1b: Try with the latest version of HED.xml
    test_string = hed.HedString(hed_string_1)
    validation_issues = test_string.validate(hed_validator_current)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 1b] hed_string_1 has issues with the latest HED version'))

    # Example 2a: Invalid HED string (junk in last tag)
    hed_string_2 = 'Event/Category/Participant response,'  \
                   '(Participant ~ Action/Button press/Keyboard ~ Participant/Effect/Body part/Arm/Hand/Finger),' \
                   'dskfjkf/dskjdfkj/sdkjdsfkjdf/sdlfdjdsjklj'
    test_string = hed.HedString(hed_string_2)
    validation_issues = test_string.validate(hed_validator_old)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 2a] hed_string_2 has junk in the last tag'))

    # Example 2b: However HED string of Example 2 has valid syntax so syntactic validation works
    test_string = hed.HedString(hed_string_2)
    validation_issues = test_string.validate(hed_validator_no_semantic)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 2b] hed_string_2 is syntactically correct'))

    # Example 3a: Invalid HED string
    hed_string_3 = 'Event/Description/The start of increasing the size of sector, Event/Label/Sector start, ' \
                   'Event/Category/Experimental stimulus/Instruction/Attend, ' \
                   '(Item/2D shape/Sector, Attribute/Visual/Color/Red) ' \
                   '(Item/2D shape/Ellipse/Circle, Attribute/Visual/Color / Red), Sensory presentation/Visual, ' \
                   'Participant/Efffectt/Visual, Participant/Effect/Cognitive/Target'
    test_string = hed.HedString(hed_string_3)
    validation_issues = test_string.validate(hed_validator_current)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 3a] hed_string_3 has missing comma so fails before Efffectt typo'))

    # Example 3b: Using issue list directly - issues are returned as a list of dictionaries
    # print('[Example 3b] hed_string_3 has ', len(validation_issues), ' issues\n')

    # Example 4a: Example using the ~ notation
    hed_string_4 = 'Event/Label/ButtonDeny, Event/Description/Button push to deny access to the ID holder, ' \
                   'Event/Category/Participant response, ' \
                   '(Participant ~ Action/Button press/Keyboard ~ Participant/Effect/Body part/Arm/Hand/Finger), ' \
                   '(Participant ~ Action/Deny/Access ~ Item/Object/Person/IDHolder)'
    test_string = hed.HedString(hed_string_4)
    validation_issues = test_string.validate(hed_validator_current)
    print(hed.get_printable_issue_string(validation_issues, title='[Example 4a] the ~ notation in hed_string_4 works in v7.1.1'))
