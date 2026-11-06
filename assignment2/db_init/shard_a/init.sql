CREATE TABLE accounts (
    id VARCHAR(10) PRIMARY KEY,
    balance DECIMAL(15, 2) NOT NULL CHECK (balance >= 0)
);

INSERT INTO accounts (id, balance) VALUES ('A0500', 1000.00);
INSERT INTO accounts (id, balance) VALUES ('A0501', 500.00);
