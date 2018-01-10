import random;
import os;
import unittest;
from hedvalidation.tag_validator import TagValidator;
from hedvalidation.hed_dictionary import HedDictionary;


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hed_xml = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/HED.xml');
        cls.REQUIRE_CHILD_DICTIONARY_KEY = 'requireChild';
        cls.hed_dictionary = HedDictionary(cls.hed_xml);
        cls.tag_validator = TagValidator(cls.hed_dictionary);
        random_require_child_key = \
            random.randint(2, len(cls.hed_dictionary.get_dictionaries()[cls.REQUIRE_CHILD_DICTIONARY_KEY]));
        cls.required_child_tag = \
            cls.hed_dictionary.get_dictionaries()[cls.REQUIRE_CHILD_DICTIONARY_KEY][cls.hed_dictionary.get_dictionaries()[cls.REQUIRE_CHILD_DICTIONARY_KEY].keys()[random_require_child_key]];
        cls.invalid_original_tag = 'This/Is/A/Tag';
        cls.invalid_formatted_tag = 'this/is/a/tag';
        cls.valid_original_tag = 'Event/Label';
        cls.valid_formatted_tag = 'event/label';
        cls.valid_hed_string = 'event/label/hed string, event/description/this is a hed string, ' \
                               'event/category/participant response';
        cls.invalid_hed_string = 'event/label/hed string, event/description/this is a hed string, ' \
                                 'event/category/participant response, (()))';
        cls.valid_formatted_tag_without_attribute = 'event/category/participant response';
        cls.tilde = '~';
        cls.comma = ','
        cls.at_sign = '@';
        cls.valid_is_numeric_tag = 'Attribute/Repetition/20';
        cls.valid_formatted_is_numeric_tag = 'attribute/repetition/20';
        cls.valid_unit_class_tag = 'Attribute/Temporal rate/20 Hz';
        cls.valid_formatted_unit_class_tag = 'attribute/temporal rate/20 hz';
        cls.invalid_formatted_unit_class_tag = 'attribute/temporal rate/20 sdfkjsdfkjdfskjs';
        cls.valid_formatted_unit_class_tag_no_units = 'attribute/temporal rate/20';
        cls.valid_takes_value_tag = 'event/label/This is a label';
        cls.valid_tag_group_string = 'This/Is/A/Tag ~ This/Is/Another/Tag ~ This/Is/A/Different/Tag';
        cls.invalid_tag_group_string = 'This/Is/A/Tag ~ ~ This/Is/Another/Tag ~ This/Is/A/Different/Tag';
        cls.valid_original_unique_tag_list = ['Event/Label/This is a label',
                                              'Event/Description/This is a description'];
        cls.valid_formatted_unique_tag_list = ['event/label/this is a label',
             'event/description/this is a description'];
        cls.invalid_original_unique_tag_list = ['Event/Label/This is a label', 'Event/Label/This is another label',
             'Event/Description/This is a description'];
        cls.invalid_formatted_unique_tag_list = ['event/label/this is a label', 'event/label/this is another label',
             'event/description/this is a description'];
        cls.valid_formatted_required_tag_list = ['event/label/this is a label', 'event/category/participant response',
             'event/description/this is a description'];
        cls.invalid_formatted_required_tag_list = ['event/label/this is a label',
             'event/description/this is a description'];
        cls.extension_allowed_descendant_tag = 'Item/Object/Tool/Hammer';
        cls.missing_comma_hed_string = '(Item/2D shape/Sector, Attribute/Visual/Color/Red) f ' \
                                       '(Item/2D shape/Ellipse/Circle, Attribute/Visual/Color / Red)';
        cls.valid_formatted_time_string_tag = 'item/2d shape/clock face/8:30';
        cls.invalid_formatted_time_string_tag = 'item/2d shape/clock face/54:54';
        cls.valid_time_string = '12:24';
        cls.invalid_time_string = '54:54';


    def test_check_if_tag_is_valid(self):
        validation_error = self.tag_validator.check_if_tag_is_valid(self.invalid_original_tag,
                                                                    self.invalid_formatted_tag);
        self.assertIsInstance(validation_error, basestring);
        self.assertTrue(validation_error);
        validation_error = self.tag_validator.check_if_tag_is_valid(self.valid_original_tag, self.valid_formatted_tag);
        self.assertIsInstance(validation_error, basestring);
        self.assertFalse(validation_error);

    def test_check_if_tag_requires_child(self):
        validation_error = self.tag_validator.check_if_tag_requires_child(self.required_child_tag,
                                                                          self.required_child_tag);
        self.assertIsInstance(validation_error, basestring);
        self.assertFalse(validation_error);

    def test_check_number_of_group_tildes(self):
        validation_error = self.tag_validator.check_number_of_group_tildes(self.valid_tag_group_string);
        self.assertIsInstance(validation_error, basestring);
        self.assertFalse(validation_error);
        validation_error = self.tag_validator.check_number_of_group_tildes(self.invalid_tag_group_string);
        self.assertIsInstance(validation_error, basestring);
        self.assertTrue(validation_error);

    def test_check_if_multiple_unique_tags_exist(self):
        validation_error = self.tag_validator.check_if_multiple_unique_tags_exist(self.valid_original_unique_tag_list,
                                                                                  self.valid_formatted_unique_tag_list);
        self.assertIsInstance(validation_error, basestring);
        self.assertFalse(validation_error);
        validation_error = self.tag_validator.check_if_multiple_unique_tags_exist(
            self.invalid_original_unique_tag_list, self.invalid_formatted_unique_tag_list);
        self.assertIsInstance(validation_error, basestring);
        self.assertTrue(validation_error);

    def test_check_for_required_tags(self):
        validation_error = self.tag_validator.check_for_required_tags(self.valid_formatted_required_tag_list);
        self.assertIsInstance(validation_error, basestring);
        self.assertFalse(validation_error);
        validation_error = self.tag_validator.check_for_required_tags(self.invalid_formatted_required_tag_list);
        self.assertIsInstance(validation_error, basestring);
        self.assertTrue(validation_error);

    def test_get_tag_slash_indices(self):
        tag_slash_indices = self.tag_validator.get_tag_slash_indices(self.valid_formatted_tag);
        self.assertIsInstance(tag_slash_indices, list);

    def test_get_tag_substring_by_end_index(self):
        tag_slash_indices = self.tag_validator.get_tag_slash_indices(self.valid_formatted_tag);
        tag = self.tag_validator.get_tag_substring_by_end_index(self.valid_formatted_tag, tag_slash_indices[0]);
        self.assertIsInstance(tag, basestring);
        self.assertNotEqual(self.valid_formatted_tag, tag);
        tag = self.tag_validator.get_tag_substring_by_end_index(self.valid_formatted_tag, 0);
        self.assertEqual(self.valid_formatted_tag, tag);

    def test_is_extension_allowed_tag(self):
        extension_allowed_tag = self.tag_validator.is_extension_allowed_tag(self.extension_allowed_descendant_tag);
        self.assertTrue(extension_allowed_tag);
        extension_allowed_tag = self.tag_validator.is_extension_allowed_tag(self.valid_formatted_tag);
        self.assertFalse(extension_allowed_tag);

    def test_tag_takes_value(self):
        takes_value_tag = self.tag_validator.tag_takes_value(self.valid_takes_value_tag);
        self.assertTrue(takes_value_tag);
        takes_value_tag = self.tag_validator.tag_takes_value(self.valid_formatted_tag);
        self.assertFalse(takes_value_tag);

    def test_is_numeric_tag(self):
        numeric_tag = self.tag_validator.is_numeric_tag(self.valid_is_numeric_tag);
        self.assertTrue(numeric_tag);

    def test_is_unit_class_tag(self):
        unit_class_tag = self.tag_validator.is_unit_class_tag(self.valid_unit_class_tag);
        self.assertTrue(unit_class_tag);

    def test_check_capitalization(self):
        validation_warning = self.tag_validator.check_capitalization(self.valid_original_tag,
                                                                     self.valid_original_tag);
        self.assertFalse(validation_warning);
        validation_warning = self.tag_validator.check_capitalization(self.valid_formatted_tag,
                                                                     self.valid_formatted_tag);
        self.assertTrue(validation_warning);
        validation_warning = self.tag_validator.check_capitalization(self.valid_is_numeric_tag,
                                                                     self.valid_is_numeric_tag);
        self.assertFalse(validation_warning);
        validation_warning = self.tag_validator.check_capitalization(self.valid_unit_class_tag,
                                                                     self.valid_unit_class_tag);
        self.assertFalse(validation_warning);

    def test_replace_tag_name_with_pound(self):
        takes_value_tag = self.tag_validator.replace_tag_name_with_pound(self.valid_takes_value_tag);
        self.assertTrue(takes_value_tag);

    def test_get_unit_class_units(self):
        unit_class_units = self.tag_validator.get_tag_unit_class_units(self.valid_formatted_unit_class_tag);
        self.assertTrue(unit_class_units);
        self.assertIsInstance(unit_class_units, list);

    def test_get_tag_name(self):
        tag_name = self.tag_validator.get_tag_name(self.valid_original_tag);
        self.assertTrue(tag_name);
        self.assertIsInstance(tag_name, basestring);

    def test_get_unit_class_default_unit(self):
        default_unit = self.tag_validator.get_unit_class_default_unit(self.valid_original_tag);
        self.assertFalse(default_unit);
        self.assertIsInstance(default_unit, basestring);
        default_unit = self.tag_validator.get_unit_class_default_unit(self.valid_formatted_unit_class_tag);
        self.assertTrue(default_unit);
        self.assertIsInstance(default_unit, basestring);

    def test_check_if_tag_unit_class_units_exist(self):
        validation_warning = self.tag_validator.check_if_tag_unit_class_units_exist(self.valid_formatted_unit_class_tag,
                                                                                    self.valid_formatted_unit_class_tag);
        self.assertFalse(validation_warning);
        self.assertIsInstance(validation_warning, basestring);
        validation_warning = \
            self.tag_validator.check_if_tag_unit_class_units_exist(self.valid_formatted_unit_class_tag_no_units,
                                                                   self.valid_formatted_unit_class_tag_no_units);
        self.assertTrue(validation_warning);
        self.assertIsInstance(validation_warning, basestring);

    def test_check_if_tag_unit_class_units_are_valid(self):
        validation_error = \
            self.tag_validator.check_if_tag_unit_class_units_are_valid(self.valid_formatted_unit_class_tag,
                                                                       self.valid_formatted_unit_class_tag);
        self.assertFalse(validation_error);
        self.assertIsInstance(validation_error, basestring);
        validation_error = \
            self.tag_validator.check_if_tag_unit_class_units_are_valid(self.valid_formatted_unit_class_tag_no_units,
                                                                       self.valid_formatted_unit_class_tag_no_units);
        self.assertFalse(validation_error);
        self.assertIsInstance(validation_error, basestring);
        validation_error = \
            self.tag_validator.check_if_tag_unit_class_units_are_valid(self.invalid_formatted_unit_class_tag,
                                                                       self.invalid_formatted_unit_class_tag);
        self.assertTrue(validation_error);
        self.assertIsInstance(validation_error, basestring);
        validation_error = \
            self.tag_validator.check_if_tag_unit_class_units_are_valid(self.valid_formatted_time_string_tag,
                                                                       self.valid_formatted_time_string_tag);
        self.assertFalse(validation_error);
        self.assertIsInstance(validation_error, basestring);
        validation_error = \
            self.tag_validator.check_if_tag_unit_class_units_are_valid(self.invalid_formatted_time_string_tag,
                                                                       self.invalid_formatted_time_string_tag);
        self.assertTrue(validation_error);
        self.assertIsInstance(validation_error, basestring);

    def test_count_tag_group_brackets(self):
        validation_error = TagValidator.count_tag_group_brackets(self.valid_hed_string);
        self.assertFalse(validation_error);
        self.assertIsInstance(validation_error, basestring);
        validation_error = TagValidator.count_tag_group_brackets(self.invalid_hed_string);
        self.assertTrue(validation_error);
        self.assertIsInstance(validation_error, basestring);

    def test_find_missing_commas_in_hed_string(self):
        validation_error = TagValidator.find_missing_commas_in_hed_string(self.valid_hed_string);
        self.assertFalse(validation_error);
        self.assertIsInstance(validation_error, basestring);
        validation_error = TagValidator.find_missing_commas_in_hed_string(self.missing_comma_hed_string);
        self.assertTrue(validation_error);
        self.assertIsInstance(validation_error, basestring);

    def test_character_is_delimiter(self):
        is_a_delimiter = TagValidator.character_is_delimiter(self.comma);
        self.assertTrue(is_a_delimiter);
        is_a_delimiter = TagValidator.character_is_delimiter(self.comma);
        self.assertTrue(is_a_delimiter);
        is_a_delimiter = TagValidator.character_is_delimiter(self.at_sign);
        self.assertFalse(is_a_delimiter);

    def test_tag_is_valid(self):
        tag_is_valid = self.tag_validator.tag_is_valid(self.valid_formatted_tag_without_attribute);
        self.assertTrue(tag_is_valid);
        tag_is_valid = self.tag_validator.tag_is_valid(self.invalid_formatted_tag);
        self.assertFalse(tag_is_valid);


    def test_is_hh_mm_time(self):
        validation_error = TagValidator.is_hh_mm_time(self.valid_time_string);
        self.assertTrue(validation_error, basestring);
        validation_error = TagValidator.is_hh_mm_time(self.invalid_time_string);
        self.assertFalse(validation_error, basestring);

if __name__ == '__main__':
    unittest.main();