import sys
import unittest
import os
from combine_csv import CombinerFactory
from io import StringIO


class TestMethods(unittest.TestCase):

    combine_path = "./combine_csv.py"
    accessories_path = "./test_fixtures/accessories.csv"
    clothing_path = "./test_fixtures/clothing.csv"
    household_path = "./test_fixtures/household_cleaners.csv"
    errorFile = "./errorLog.txt"

    combiner = CombinerFactory.createCombiner()

    def setUp(self):
        self.output = StringIO()
        sys.stdout = self.output
        self.combiner.cleanLog()

    @classmethod
    def tearDownClass(cls):
        cls.combiner.cleanLog()
        cls.combiner = None



    # Checking for wrong path - Unit Test 1
    def test_wrong_path(self):
        output = self.combiner.validatefilepath("wrong.csv")

        self.assertFalse(output)
        self.assertTrue(os.path.exists(self.errorFile))
        self.assertEqual("Error: Please verify your input at - wrong.csv. Directory doesnt exist",
                         self.getErrorFileData(self.errorFile).strip())

    
    # Check for the existence of file - Unit Test 2
    def test_correct_path(self):
        output = self.combiner.validatefilepath(self.household_path)
        self.assertTrue(output)
        self.assertFalse(os.path.exists(self.errorFile))

    # Check for correct chunking - Unit Test 3
    def test_chunks(self):
        chunk_list = self.combiner.readfilechunks(self.clothing_path, 2)
        self.assertEqual(len(chunk_list), 2)

    # Check if chunk length is zero - Unit Test 4
    def test_chunklen_zero(self):
        chunks = self.combiner.readfilechunks(self.clothing_path, 0)
        self.assertTrue(os.path.exists(self.errorFile))
        self.assertEqual("Error: Size of chunk should be greater than 1.", self.getErrorFileData(self.errorFile).strip())

    # Check if the command has arguments - Unit Test 5
    def test_no_argument(self):
        output = self.combiner.combine_files(files=[])
        self.assertFalse(output)
        self.assertEqual("", self.output.getvalue().strip())

    # Check if CSVs are  combined completely - Unit Test 6
    def test_combine_files(self):
        self.combiner.combine_files([self.clothing_path, self.accessories_path, self.household_path], 2)

        output_value = self.output.getvalue()

        self.assertTrue('filename' in output_value)
        # 16 because 1 for header and 15 lines
        self.assertEqual(output_value.count('\n'), 16)

    @staticmethod
    def getErrorFileData(fileName):
        resp = ''
        with open(fileName) as f:
            resp += f.read()
        return resp
