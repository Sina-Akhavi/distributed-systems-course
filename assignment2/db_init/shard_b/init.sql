CREATE TABLE accounts (
    id VARCHAR(10) PRIMARY KEY,
    balance DECIMAL(15, 2) NOT NULL CHECK (balance >= 0)
);

INSERT INTO accounts (id, balance) VALUES ('A1500', 500.00);
