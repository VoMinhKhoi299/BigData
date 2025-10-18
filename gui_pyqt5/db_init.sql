CREATE DATABASE IF NOT EXISTS bigdata;
USE bigdata;

CREATE TABLE IF NOT EXISTS test_connection (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255)
);

INSERT INTO test_connection (message)
VALUES ('Hello from remote MySQL!');
