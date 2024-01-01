# Take-Home Exam: In-Memory Database Implementation

### Objective:
Implement an in-memory database system with a command-line interface that meets specific functionality and performance requirements. Your code should be clear, well-documented, and handle edge cases effectively. You can use libraries except for database-specific or actual database systems. Feel free to use online references, such as Google and Stack Overflow.

### Instructions:
Create a command-line program that reads input from STDIN line by line and executes the following database functions. Additionally, provide a README file with instructions on how to run your program. We recomend that you use Github code spaces and be sure to push your branch with your initials and the date you completed the task. 

Database Functions:

- `SET` [name] [value]: Sets the value associated with the given name in the database.
- `GET` [name]: Retrieves and prints the value for the specified name. If the name is not in the database, print "NULL."
- `DELETE` [name]: Deletes the value associated with the specified name from the database.
- `COUNT` [value]: Returns the count of names that have the given value assigned to them. If the value is not assigned to any name, print "0."
- `BEGIN`: Starts a new transaction.
- `ROLLBACK`: Rolls back the most recent transaction. If there is no transaction to rollback, print "TRANSACTION NOT FOUND."
- `COMMIT`: Commits all open transactions and makes changes permanent.
- `END`: Exits the database.
### Performance Requirements:

Aim for efficient operations: Ensure that GET, SET, DELETE, and COUNT have a runtime of less than O(log n), if not better (where n is the number of items in the database).
Memory usage should not double with each transaction.
Minimum Requirements:

Pass the first three test cases outlined in the exam document.
Do not use actual database systems or database libraries.
Implement the basic commands (GET, SET, DELETE, and COUNT) as per the specification. They should accept the correct number of arguments and function correctly.

Certainly, I can reformat the examples to make them more readable. Here's the same set of examples in a more organized format:

**Example 1: Basic Commands**

1. `GET a`
>>> NULL
2. `SET a foo`
3. `SET b foo`
4. `COUNT foo`
>>> 2
5. `COUNT bar`
>>> 0
6. `DELETE a`
7. `COUNT foo`
>>> 1
8. `SET b baz`
9. `COUNT foo`
>>> 0
10. `GET b`
>>> baz
11. `GET B`
>>> NULL
12. END

**Example 2: Multiple Transactions**

1. `SET a foo`
2. `SET a foo`
3. `COUNT foo`
>>> 1
4. `GET a`
>>> foo
5. `DELETE a`
6. `GET a`
>>> NULL
7. `COUNT foo`
>>> 0
8. END

**Example 3: Nested Transactions**

1. `BEGIN`
2. `SET a foo`
3. `GET a`
>>> foo
4. `BEGIN`
5. `SET a bar`
6. `GET a`
>>> bar
7. `SET a baz`
8. `ROLLBACK`
9. `GET a`
>>> foo
10. `ROLLBACK`
11. `GET a`
>>> NULL
12. END

**Example 4: Nested Transactions with COMMIT**

1. `SET a foo`
2. `SET b baz`
3. `BEGIN`
4. `GET a`
>>> foo
5. `SET a bar`
6. `COUNT bar`
>>> 1
7. `BEGIN`
8. `COUNT bar`
>>> 1
9. `DELETE a`
10. `GET a`
>>> NULL
11. `COUNT bar`
>>> 0
12. `ROLLBACK`
13. `GET a`
>>> bar
14. `COUNT bar`
>>> 1
15. `COMMIT`
16. `GET a`
>>> bar
17. `GET b`
>>> baz
18. END
