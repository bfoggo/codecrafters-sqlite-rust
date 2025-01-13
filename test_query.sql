-- This is a single-line comment
/*
This is a
multi-line comment
*/
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER,
    balance REAL,
    email TEXT UNIQUE
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount REAL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

INSERT INTO users (name, age, balance, email) VALUES ('Alice', 30, 1000.50, 'alice@example.com');
INSERT INTO users (name, age, balance, email) VALUES ('Bob', 25, 500.75, 'bob@example.com');

INSERT INTO orders (user_id, amount) VALUES (1, 250.00);
INSERT INTO orders (user_id, amount) VALUES (2, 125.50);

SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 20 AND orders.amount < 300.00;
