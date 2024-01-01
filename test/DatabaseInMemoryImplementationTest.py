import unittest
from DatabaseInMemoryImplementation import DatabaseInMemoryImplementation


class DatabaseInMemoryImplementationTest(unittest.TestCase):

    def setUp(self):
        self.database = DatabaseInMemoryImplementation()
        self.database.create_table()

    def tearDown(self):
        self.database.close_connection()

    def test_example_1_basic_commands(self):
        self.assertEqual(self.database.get_value('a'), 'NULL')

        self.database.set_value('a', 'foo')
        self.database.set_value('b', 'foo')
        self.assertEqual(self.database.count('foo'), 2)

        self.assertEqual(self.database.count('bar'), 0)

        self.database.delete_value('a')
        self.assertEqual(self.database.count('foo'), 1)

        self.database.set_value('b', 'baz')
        self.assertEqual(self.database.count('foo'), 0)

        self.assertEqual(self.database.get_value('b'), 'baz')

        self.assertEqual(self.database.get_value('B'), 'NULL')

    def test_example_2_multiple_transactions(self):
        self.database.set_value('a', 'foo')
        self.database.set_value('a', 'foo')
        self.assertEqual(self.database.count('foo'), 1)

        self.assertEqual(self.database.get_value('a'), 'foo')

        self.database.delete_value('a')
        self.assertEqual(self.database.get_value('a'), 'NULL')

        self.assertEqual(self.database.count('foo'), 0)
        self.database.end()

    def test_example_3_nested_transactions(self):
        self.database.begin()
        self.database.set_value('a', 'foo')
        self.assertEqual(self.database.get_value('a'), 'foo')

        self.database.begin()
        self.database.set_value('a', 'bar')
        self.assertEqual(self.database.get_value('a'), 'bar')

        self.database.set_value('a', 'baz')
        self.database.rollback()
        self.assertEqual(self.database.get_value('a'), 'foo')

        self.database.rollback()
        self.assertEqual(self.database.get_value('a'), 'NULL')

        self.database.end()

    def test_example_4_nested_transactions_with_commit(self):
        self.database.set_value('a', 'foo')
        self.database.set_value('b', 'baz')
        self.database.begin()
        self.assertEqual(self.database.get_value('a'), 'foo')

        self.database.set_value('a', 'bar')
        self.assertEqual(self.database.count('bar'), 1)

        self.database.begin()
        self.assertEqual(self.database.count('bar'), 1)

        self.database.delete_value('a')
        self.assertEqual(self.database.get_value('a'), 'NULL')

        self.assertEqual(self.database.count('bar'), 0)

        self.database.rollback()
        self.assertEqual(self.database.get_value('a'), 'bar')

        self.assertEqual(self.database.count('bar'), 1)

        self.database.commit()
        self.assertEqual(self.database.get_value('a'), 'bar')

        self.assertEqual(self.database.get_value('b'), 'baz')

        self.database.end()


if __name__ == '__main__':
    unittest.main()
