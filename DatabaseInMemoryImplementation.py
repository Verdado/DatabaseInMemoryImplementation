import sqlite3
import sys


class DatabaseInMemoryImplementation:

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        # For simplicity purposes, storing savepoint order, can be converted to queue or etc.
        self.savepoints = []

    def create_table(self):
        self.cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users (
                name TEXT NOT NULL PRIMARY KEY UNIQUE,
                value TEXT NOT NULL
                )
            ''')

    def get_value(self, name):
        query = f"SELECT COALESCE(value, NULL) as value FROM users WHERE name = '{name}'"
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return 'NULL' if result is None else result[0]

    def set_value(self, name, new_value):
        self.cursor.execute(f"INSERT OR REPLACE INTO users (name, value) VALUES ('{name}','{new_value}')")

    def delete_value(self, value):
        self.cursor.execute(f"DELETE FROM users WHERE name = '{value[0]}'")

    def count(self, value=None):
        sql = f"SELECT count(name) FROM users"
        if value:
            sql = sql + f" WHERE value = '{value}'"
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def rollback(self):
        sql = "ROLLBACK"
        try:
            if self.conn.in_transaction and self.savepoints:
                savepoint_name = self.savepoints[-1]
                self.cursor.execute(f"{sql} TO SAVEPOINT {savepoint_name}")
                print(f"Rolled back to {savepoint_name}.")
                self.savepoints.pop()
            else:
                self.cursor.execute(sql)
                print("Transaction rolled back.")
        except sqlite3.Error as e:
            # cannot rollback - no transaction is active
            print(f"TRANSACTION NOT FOUND. {e}")

    def begin(self):
        # sqlite doesn't have a concept of nested transactions, thus using savepoints.
        savepoint = f"mySavePoint{str(len(self.savepoints))}"
        self.cursor.execute(f"SAVEPOINT '{savepoint}'")
        self.savepoints.append(savepoint)
        print(f"Begin new transaction named {savepoint}")

    def commit(self):
        self.conn.commit()

    def end(self):
        self.close_connection()

    def close_connection(self):
        self.conn.close()

    def main(self):
        self.create_table()
        for line in sys.stdin:
            cmd, *args = line.strip().split()
            try:
                if cmd == 'GET':
                    self.__handle_get(args)
                elif cmd == 'SET':
                    self.__handle_set(args)
                elif cmd == 'DELETE':
                    self.__handle_delete(args)
                elif cmd == 'COUNT':
                    self.__handle_count(args)
                elif cmd == 'ROLLBACK':
                    self.rollback()
                elif cmd == 'BEGIN':
                    self.begin()
                elif cmd == 'COMMIT':
                    self.commit()
                elif cmd == 'END':
                    self.end()
                else:
                    print(f"ERROR: Command '{cmd}' not recognized, try again.")
            except IndexError as e:
                print(f"{cmd} argument must be 1, try again.")

    def __handle_get(self, args):
        value = args[0]
        print(self.get_value(value))

    def __handle_set(self, args):
        column_name, new_value = args
        self.set_value(column_name, new_value)

    def __handle_delete(self, args):
        value = args[0]
        self.delete_value(value)

    def __handle_count(self, args):
        print(self.count(args))


if __name__ == '__main__':
    DatabaseInMemoryImplementation().main()
