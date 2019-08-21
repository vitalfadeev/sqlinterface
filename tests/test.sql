drop table test_table;

CREATE TABLE `test_table` (
    Col1 TEXT CHARACTER SET utf8,
    Col2 TEXT CHARACTER SET utf8,
    INDEX (Col1(255)),
    FULLTEXT INDEX (Col1),
    FULLTEXT INDEX (Col2)
)
DEFAULT CHARSET=utf8;

SELECT `Col1` FROM `test_table` WHERE MATCH ( `Col1` ) AGAINST ('in' IN NATURAL LANGUAGE MODE);

SELECT * FROM `test_table` WHERE MATCH ( `Col1`, `Col2` ) AGAINST ('in' IN NATURAL LANGUAGE MODE);

SELECT `Col1`, `Col5`, `Col6` FROM `test_table` WHERE MATCH ( `Col1`, `Col5`, `Col6` ) AGAINST ('in' IN NATURAL LANGUAGE MODE);

CREATE TABLE `test_table` (
    Col1 TEXT CHARACTER SET utf8,
    Col2 INT,
    Col3 FLOAT,
    Col4 DATETIME,
    Col5 TEXT CHARACTER SET utf8,
    Col6 TEXT CHARACTER SET utf8
    ,
    FULLTEXT INDEX (Col1, Col5, Col6)
)
DEFAULT CHARSET=utf8;
