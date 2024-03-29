#import settings
import os
import MySQLdb
import time
import logging
from functools import wraps


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MY_CNF_PATH = os.path.join(BASE_DIR, 'my.cnf')

type_map = {
    "PRIMARYKEYAUTO": "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
    "VARCHAR"       : "VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci",
    "TEXT"          : "TEXT CHARACTER SET utf8 COLLATE utf8_unicode_ci",
    "INT"           : "INT",
    "FLOAT"         : "FLOAT",
    "DATETIME"      : "DATETIME",
    "TIME"          : "DATETIME",
    "NUMERIC"       : "FLOAT",
    "BOOLEAN"       : "INT",
    "TAGS"          : "TEXT",
    "OPTIONS"       : "TEXT",
}


def profile(func):
     """ Decorator to measure delay
     """
     @wraps(func)
     def wrapper(*args, **kwargs):
         started_at = time.time()
         result = func(*args, **kwargs)
         logging.debug(' ' + (func.__name__ + ': ').ljust(24) + str(time.time() - started_at))
         return result

     return wrapper


def get_ctypes(ColumnsNamesType):
    """ in:  ["Col1:Text", "Col2:INT"]
        out: {
                 Col1: TEXT,
                 Col2: INT
             }
    """
    ctypes = {}

    for col in ColumnsNamesType:
        splits = col.split(":")
        cname = splits[0]
        ctype = splits[1]
        ctype = ctype.upper()
        ctypes[cname] = ctype

    return ctypes


def format_cols_to_sql(ctypes):
    """ in:  { "Col1":"TEXT", "Col2":"INT" |
        out:
             Col1 Text CHARACTER SET utf8,
             Col2 INT
    """
    sqls = []

    for cname, ctype in ctypes.items():
        sql_type = type_map[ctype]
        sqls.append("{} {}".format(cname, sql_type))

    sql = ", \n".join(sqls)

    return sql


def format_indexes_to_sql(ctypes, ColumnsIndexs):
    """ in:  ["Col1", "Col2"]
        out: INDEX Col1, INDEX Col2
    """
    sqls = []

    for col in ColumnsIndexs:
        ctype = ctypes[col]

        if ctype == "TEXT":
            key_params = "(255)"
        else:
            key_params = ""

        sqls.append(
            "INDEX ({}{})".format(
                col, key_params
            )
        )

    sql = ", \n".join(sqls)

    return sql


def format_fts_indexes_to_sql(ColumnsIndexsFTS):
    """ in:  ["Col5", "Col6"]
        out: "Col5, Col6"
    """
    sqls = []

    for col in ColumnsIndexsFTS:
        sqls.append("{}".format(col))

    sql = ", \n".join(sqls)

    return sql


def format_query_columns_to_sql( ColToReturnNames ):
    """ in:  ["Col1", "Col2"]
        out: "Col1, Col2"
    """
    sqls = []

    for col in ColToReturnNames:
        sqls.append( "`{}`".format(col) )

    sql = ", ".join(sqls)

    return sql


def format_where_to_sql( ColToSearchIntoName ):
    """ in:  ["Col1", "Col2"]
        out: "Col1 = %s AND Col2 = %s"
    """
    sqls = []

    for col in ColToSearchIntoName:
        sqls.append( "`{}` = %s".format(col) )

    sql = " AND ".join(sqls)

    return sql


def format_where_fts_to_sql( ColToSearchIntoName ):
    """ in:  ["Col1", "Col2"]
        out: "Col1, Col2"
    """
    sqls = []

    for col in ColToSearchIntoName:
        sqls.append( "`{}`".format(col) )

    sql_cols = ", ".join(sqls)

    return sql_cols


def format_update_columns_to_sql( ColToReturnNames ):
    """ in:  {"Col1":1, "Col2":2}
        out: (
                "Col1 = %s, Col2 = %s",
                [1, 2]
              )
    """
    sqls = []
    values = []

    for col, value in ColToReturnNames.items():
        sqls.append( "`{}` = %s".format(col) )
        values.append( value )

    sql = ", ".join(sqls)

    return (sql, values)


def format_dict_where_to_sql( ColsSearch ):
    """ in:  {"Col1":1, "Col2":2}
        out: (
              "Col1 = %s AND Col2 = %s",
              [1, 2]
             )
    """
    sqls = []
    values = []

    for col, value in ColsSearch.items():
        sqls.append( "`{}` = %s".format(col) )
        values.append( value )

    sql = " AND ".join(sqls)

    return (sql, values)


def format_insert_columns_to_sql( ColSet ):
    """ in:  {"Col1":1, "Col2":2}
        out: (
              "Col1, Col2",
              "%s, %s",
              [1, 2]]
             )
    """
    cols_sqls = []
    vals_sqls = []
    values = []

    for col, value in ColSet.items():
        cols_sqls.append( "`{}`".format(col) )
        vals_sqls.append( "%s" )
        values.append( value )

    cols_sql = ", ".join(cols_sqls)
    vals_sql = ", ".join(vals_sqls)

    return (cols_sql, vals_sql, values)


def format_manyinsert_columns_to_sql( Cols ):
    """ in:  ["Col1", "Col2"]
        out: (
              "Col1, Col2",
              "%s, %s",
             )
    """
    cols_sqls = []
    vals_sqls = []
    values = []

    for col in Cols:
        cols_sqls.append( "`{}`".format(col) )
        vals_sqls.append( "%s" )

    cols_sql = ", ".join(cols_sqls)
    vals_sql = ", ".join(vals_sqls)

    return (cols_sql, vals_sql)


def get_connetion(connection_string):
    # mysql://user:password@host:port/database
    # sqlite:///path/file.sqlite3
    from urllib.parse import urlparse
    o = urlparse(connection_string)

    if o.scheme == 'mysql':
        import MySQLdb
        port = o.port       if o.port else 3306
        host = o.hostname   if o.hostname else 'localhost'
        dbc = MySQLdb.connect( host=host, port=port, user=o.username, password=o.password, db=o.path.strip('/') )
        return dbc

    elif o.scheme == 'sqlite':
        import sqlite3
        return sqlite3.connect( o.path )

    else:
        raise Exception("Unsupported")


class SQLInterface:
    def __init__(self, autoconnect=True, connection_string=None, my_cnf_path=MY_CNF_PATH):
        """
        :param autoconnect:        bool  Connect to db on __init__
        :param connection_string:  ""    sqlite:///path/Brain01.sqlite3 | mysql://testuser:testpassword@localhost:3306/Brain01
        :param my_cnf_path:        ""    Path to my.cnf
        """
        self._db = None
        self.my_cnf_path = my_cnf_path
        self.connection_string = connection_string
        if autoconnect:
            self.connect()


    def connect(self):
        """ Connect to db
            host, username, password in my.cnf, see, please MY_CNF_PATH
        """
        if self.connection_string:
            # sqlite | mysql with connectin string. ex: mysql://username:password@host:port/DB or sqlite://dbfile.sqlite3
            self._db = get_connetion(self.connection_string)
            if self.connection_string.startswith('mysql'):
                self.setup_utf8_support()
        elif self.my_cnf_path:
            # mysql with my.cnf
            self._db = MySQLdb.connect(charset='utf8', read_default_file=self.my_cnf_path, use_unicode=True)
            self.setup_utf8_support()
        else:
            raise Exception('SQLInterface(): expect one of args: connection_string= or my_cnf_path=')


    def setup_utf8_support(self):
        """ Setup UTF-8 support
        """
        self._db.set_character_set('utf8')
        self._db.query('SET NAMES utf8;')
        self._db.query('SET CHARACTER SET utf8;')
        self._db.query('SET character_set_connection=utf8;')
        
        
    def get_connection(self):
        return self._db


    @profile
    def SqlCreateDatabase( self, DBname ):
        """ Create database
            :param DBname: "" Database to create
        """
        self._db.query("CREATE DATABASE `{}`;".format(DBname))


    def SqlDropDatabase( self, DBname ):
        """ Drop database
            :param DBname: "" database to drop
        """
        self._db.query("DROP DATABASE IF EXISTS `{}`;".format(DBname))


    def UseDatabase( self, DBname ):
        """ Use database
            :param DBname: "" database to use
        """
        self._db.query("USE `{}`;".format(DBname))


    @profile
    def SqlCreateTable( self, TableName,  ColumnsNamesType, ColumnsIndexs=None, ColumnsIndexsFTS=None):
        """ Creata table
            :param TableName:           ""  table to create
            :param ColumnsNamesType:    {}  columns. ex: { "Col1":"INT", "Col2":"TEXT" }
            :param ColumnsIndexs:       []  columns for index
            :param ColumnsIndexsFTS:    []  columns for fulltext search indexed
        """

        if isinstance(ColumnsNamesType, dict):      # ex: { "Col1":"INT", "Col2":"TEXT" }
            ctypes = ColumnsNamesType
        elif isinstance(ColumnsNamesType, list):    # ex: [ "Col1:INT", "Col2:TEXT" ]
            ctypes = get_ctypes(ColumnsNamesType)
        else:
            raise Exception("unsupported")

        cols_sql = format_cols_to_sql(ctypes)

        if ColumnsIndexs:
            indexes_sql = format_indexes_to_sql(ctypes, ColumnsIndexs)

            if indexes_sql:
                indexes_sql = ',\n' + indexes_sql

        else:
            indexes_sql = ""

        if ColumnsIndexsFTS:
            fts_indexes_sql = format_fts_indexes_to_sql(ColumnsIndexsFTS)

            if fts_indexes_sql:
                fts_indexes_sql = ',\n' + "FULLTEXT INDEX (" + fts_indexes_sql + ")"

        else:
            fts_indexes_sql = ""

        #
        sql = """
            CREATE TABLE `{}` (
                {}
                {}
                {}
                )
                DEFAULT CHARSET=utf8
                ENGINE = InnoDB
                COLLATE utf8_unicode_ci;
            """.format(
                TableName,
                cols_sql,
                indexes_sql,
                fts_indexes_sql
            )

        self._db.query(sql)


    def SqlDropTable( self, TABLEname ):
        """ Drop table.
            :param TABLEname: table to drop
        """
        self._db.query("DROP TABLE IF EXISTS `{}`;".format(TABLEname))


    @profile
    def SqlSearch( self, TABLEname, ColToReturnNames=None, ColToSearchIntoName=None, valueToSearch=None ):
        """ Select data from table
            :param TABLEname:           ""  Table name
            :param ColToReturnNames:    []  Columns to return
            :param ColToSearchIntoName: []  Columns to search in
            :param valueToSearch:       []  Values in search columns
            :return:                    [[],[],]    rows
            Note: Select ColToReturnNames FROM table WHERE ColToSearchIntoName=valueToSearch AND …
        """
        Datas = []

        #
        if ColToReturnNames is None:
            sql_cols = "*"
        else:
            sql_cols = format_query_columns_to_sql( ColToReturnNames )

        #
        if  ColToSearchIntoName is None:
            sql_where = ""
        else:
            sql_where = "WHERE " + format_where_to_sql( ColToSearchIntoName )

        #
        sql = """
            SELECT {} FROM `{}` {}
        """.format(
                sql_cols,
                TABLEname,
                sql_where
            )

        #
        c = self._db.cursor()
        c.execute(sql, valueToSearch)
        Datas = c.fetchall()

        return Datas


    @profile
    def SqlFTSearch( self, TABLEname, ColToReturnNames, ColToSearchIntoName, OneStringToSearchForIdenticalMatch ):
        """ Select data from table
            :param TABLEname:                           ""  Table name
            :param ColToReturnNames:                    []  Columns to return
            :param ColToSearchIntoName:                 []  Columns for seach in
            :param OneStringToSearchForIdenticalMatch:  ""  String to search
            :return:                                    [[],[],] rows

            Note: WHERE MATCH(ColToSearchIntoNames[]) AGAINST( OneStringToSearchForIdenticalMatch IN NATURAL LANGUAGE MODE)
        """
        Datas = []

        #
        sql_cols = format_query_columns_to_sql( ColToReturnNames )
        sql_where_cols = format_where_fts_to_sql( ColToSearchIntoName )
        sql_where = "WHERE MATCH ( " + sql_where_cols + " ) AGAINST (%s IN NATURAL LANGUAGE MODE)"

        #
        sql = """
            SELECT {} FROM `{}` {}
        """.format(
                sql_cols,
                TABLEname,
                sql_where,
                OneStringToSearchForIdenticalMatch
            )

        #
        c = self._db.cursor()
        c.execute(sql, (OneStringToSearchForIdenticalMatch,))
        Datas = c.fetchall()

        return Datas


    @profile
    def SqlUpdate( self, TABLEname, ColsUpdate, ColsSearch ):
        """ Update data in table
            :param TABLEname    ""  ex: DataTable_1
            :param ColsUpdate   {}  ex: ColsSet:"ValueToReplaceInCol"
            :param ColsSearch   {}  ex: Col1:"ValueToSearch"
            :return             Cursor (affected rows: c.rowcount, new id: c.lastrowid)
            Note: UPDATE TABLEname SET ColsSet.key = ColsSet.Value WHERE ColsSearch.key = ColsSearch.value
        """
        #
        (sql_cols, update_values) = format_update_columns_to_sql( ColsUpdate )
        (sql_where, where_values) = format_dict_where_to_sql(ColsSearch)
        sql_where = "WHERE " + sql_where

        #
        sql = """
            UPDATE `{}` SET {} {}
        """.format(
                TABLEname,
                sql_cols,
                sql_where
            )

        values = update_values + where_values

        #
        c = self._db.cursor()
        c.execute(sql, values)
        self._db.commit()
        return c


    @profile
    def SqlInsert( self, TableName, ColSet ):
        """ Insert data in table
            :param TableName:   "" Table
            :param ColSet:      {} Values to insert. ex: {'Col1':'The name', 'Col2':1}
            :return:            Cursor (affected rows: c.rowcount, new id: c.lastrowid)
            Note: Dict(ColsSet:"ValueInCol")
            Note: INSERT INTO TableName (ColsSet.Key) VALUES (ColsSet.Value)
        """
        #
        (cols_sql, vals_sql, values) = format_insert_columns_to_sql( ColSet )

        #
        sql = """
            INSERT INTO `{}` ({}) VALUES ({})
        """.format(
                TableName,
                cols_sql,
                vals_sql
            )

        #
        c = self._db.cursor()
        c.execute(sql, values)
        self._db.commit()
        return c


    def SQLExecuteMany( self, Sql, Array ):
        c = self._db.cursor()
        c.executemany(Sql, Array)

        return c


    @profile
    def SqlExecuteManyInsert( self, TableName, ColumnNames, ArrayData ):
        """ Insert Many rows in table at once
            :param TableName:   ""
            :param ColumnNames: []
            :param ArrayData:   [[], [],]
            :return:            Cursor (affected rows: c.rowcount, new id: c.lastrowid)
        """
        (cols_sql, vals_sql) = format_manyinsert_columns_to_sql( ColumnNames )

        insert_sql = """
            INSERT INTO `{}`
            ({})
            VALUES
            ({});
        """.format(
            TableName,
            cols_sql,
            vals_sql
        )

        #
        sql = insert_sql

        c = self.SQLExecuteMany( sql, ArrayData )
        self._db.commit()

        return c


    @profile
    def SqlExecuteManyRead( self, TABLEname, ColumnName, sqlWhere=None ):
        """ Custom select with raw WHERE
            :param TABLEname:   ""  Table name
            :param ColumnName:  []  Columns to return
            :param sqlWhere:    ""  Where expression without WHERE keyword. ex: "Col1 = 1 OR Col2 = 1"
            :return:            [[], [], ]
        """
        Datas = []

        #
        if ColumnName is None:
            sql_cols = "*"
        else:
            sql_cols = format_query_columns_to_sql( ColumnName )

        #
        if sqlWhere is None:
            sql_where = ""
        else:
            sql_where = "WHERE " + sqlWhere

        #
        sql = """
            SELECT {} FROM `{}` {}
        """.format(
                sql_cols,
                TABLEname,
                sql_where
            )

        #
        c = self._db.cursor()
        c.execute(sql)
        Datas = c.fetchall()

        return Datas


    def raw(self, sql):
        c = self._db.cursor()
        c.execute(sql)
        return c
