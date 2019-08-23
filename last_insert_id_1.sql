DROP DATABASE IF EXISTS TEST;
CREATE DATABASE TEST;
USE TEST;
CREATE TABLE test (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        age INT
    );

INSERT INTO test (age) VALUES (1), (2), (3);

SELECT LAST_INSERT_ID();
