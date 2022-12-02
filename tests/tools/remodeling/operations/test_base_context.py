import os
import shutil
import unittest
from hed.tools.remodeling.operations.base_context import BaseContext


class TestContext(BaseContext):

    def __init__(self):
        super().__init__("TestContext", "Test", "test_context")

    def _get_summary_details(self, include_individual=True):
        summary = {"name": self.context_name}
        if include_individual:
            summary["more"] = "more stuff"
        return summary

    def _merge_all(self):
        return {"merged": self.context_name}

    def update_context(self, context_dict):
        pass


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        summary_dir = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                    '../../../data/remodel_tests/temp'))
        os.makedirs(summary_dir, exist_ok=True)
        cls.summary_dir = summary_dir

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.summary_dir)

    def test_constructor(self):
        with self.assertRaises(TypeError) as context:
            BaseContext('apple', 'banana', 'pear')
        self.assertTrue(context.exception.args[0])

        test = TestContext()
        self.assertIsInstance(test, TestContext)

    def test_get_text_summary(self):
        test = TestContext()
        str1 = test.get_text_summary()
        self.assertIsInstance(str1, str)
        self.assertTrue(str1)
        str2 = test.get_text_summary(title='this title')
        self.assertGreater(len(str2), len(str1))
        str3 = test.get_text_summary(title='this title', include_individual=False)
        self.assertGreater(len(str2), len(str3))

    def test_save(self):
        test1 = TestContext()
        file_list1 = os.listdir(self.summary_dir)
        self.assertFalse(file_list1)
        test1.save(self.summary_dir)
        file_list2 = os.listdir(self.summary_dir)
        self.assertEqual(len(file_list2), 1)
        test1.save(self.summary_dir, file_formats=['.json', '.tsv'], include_individual=False)
        file_list3 = os.listdir(self.summary_dir)
        self.assertEqual(len(file_list3), 2)


if __name__ == '__main__':
    unittest.main()