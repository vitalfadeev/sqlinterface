import os
import unittest
import datetime
import sqlinterface


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.company_id = 0
        self.DBname = 'test-BrainData-{}'.format(self.company_id)


    def test_1_connection(self):
        import MySQLdb
        db = sqlinterface.SQLInterface()
        self.assertIsNotNone(db._db)
        self.assertIsInstance(db._db, MySQLdb.connections.Connection)


    def test_1_connection_sqlite3(self):
        # get_connetion('mysql://testuser:testpassword@localhost:3306/Brain01')
        # get_connetion('sqlite:///path/Brain01.sqlite3')
        import sqlite3
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        db = sqlinterface.SQLInterface(
            connection_string="sqlite://{}".format( os.path.join(BASE_DIR, 'Brain01.sqlite3' ) )
        )
        self.assertIsNotNone(db._db)
        self.assertIsInstance(db._db, sqlite3.Connection)


    def test_1_connection_mysql(self):
        import MySQLdb
        db = sqlinterface.SQLInterface()
        self.assertIsNotNone(db._db)
        self.assertIsInstance(db._db, MySQLdb.connections.Connection)


    def test_2_prepare(self):
        db = sqlinterface.SQLInterface()
        db.SqlDropDatabase( self.DBname )


    def test_3_SqlCreateDatabase(self):
        db = sqlinterface.SQLInterface()
        db.SqlCreateDatabase( self.DBname )


    def test_4_UseDatabase(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )


    def test_5_SqlCreateTable(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]

        T = db.SqlCreateTable( TABLEname, ColumnsNamesType )


    def test_6_SqlCDropTable(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"

        db.SqlDropTable( TABLEname )


    def test_7_SqlCreateTable_with_indexes(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        ColumnsIndexs       = ['Col1','Col3']

        T = db.SqlCreateTable( TABLEname, ColumnsNamesType, ColumnsIndexs )

        db.SqlDropTable( TABLEname )


    def test_9_SqlCreateTable_with_fts_indexes(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        ColumnsIndexs       = ['Col1','Col3']
        ColumnsIndexsFTS    = ['Col5','Col6']

        db.SqlCreateTable( TABLEname, ColumnsNamesType, ColumnsIndexs, ColumnsIndexsFTS )

        db.SqlDropTable( TABLEname )


    def test_10_SqlSearch_string(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable( TABLEname, ColumnsNamesType )

        db.SqlSearch( TABLEname, ['Col1'], ['Col1'], ['one'] )

        db.SqlDropTable( TABLEname )


    def test_11_SqlSearch_int(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable( TABLEname, ColumnsNamesType )

        db.SqlSearch( TABLEname, ['Col1'], ['Col2'], [1] )

        db.SqlDropTable( TABLEname )


    def test_11_SqlSearch_float(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable( TABLEname, ColumnsNamesType )

        db.SqlSearch( TABLEname, ['Col1'], ['Col2'], [1.01] )

        db.SqlDropTable( TABLEname )


    def test_11_SqlSearch_datetime(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable( TABLEname, ColumnsNamesType )

        db.SqlSearch( TABLEname, ['Col1'], ['Col3'], [datetime.datetime.now()] )

        db.SqlDropTable( TABLEname )


    def test_12_SqlSearch_fulltext(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase( self.DBname )
        TABLEname           = "test_table"
        ColumnsNamesType    = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable( TABLEname, ColumnsNamesType, [], ['Col6'] )

        db.SqlFTSearch( TABLEname, ['Col1'], ['Col6'], ['in'] )

        db.SqlDropTable( TABLEname )


    def test_13_SqlUpdate(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        db.SqlUpdate( TABLEname, {'Col1':'The new'}, {'Col2':1} )

        db.SqlDropTable(TABLEname)


    def test_14_SqlInsert(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        db.SqlInsert( TABLEname, {'Col1':'The new', 'Col2':1} )

        db.SqlDropTable( TABLEname )


    def test_15_SqlInsert_Then_Search(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)
        db.SqlInsert( TABLEname, {'Col1':'The new', 'Col2':1} )

        data = db.SqlSearch( TABLEname, ['Col1'], ['Col1'], ['The new'] )
        self.assertEqual(data, (('The new',),))

        data = db.SqlSearch( TABLEname, ['Col2'], ['Col2'], [1] )
        self.assertEqual(data, ((1,),))

        data = db.SqlSearch( TABLEname, ['Col1', 'Col2'], ['Col1', 'Col2'], ['The new', 1] )
        self.assertEqual(data, (('The new', 1),))

        db.SqlDropTable( TABLEname )


    def test_16_SqlInsert_Then_FtsSearch(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType, [], ['Col1', 'Col5', 'Col6'])
        db.SqlInsert( TABLEname, {'Col1':'The new', 'Col2':1, 'Col5':'Cat in home', 'Col6':'Sun in space'} )

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'cat' )
        self.assertEqual(data, (('The new', 'Cat in home', 'Sun in space'),))

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'The' )
        self.assertEqual(data, tuple()) # empty

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'in' )
        self.assertEqual(data, tuple()) # empty

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'Sun' )
        self.assertEqual(data, (('The new', 'Cat in home', 'Sun in space'),))

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'sun' )
        self.assertEqual(data, (('The new', 'Cat in home', 'Sun in space'),))

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'SPACE' )
        self.assertEqual(data, (('The new', 'Cat in home', 'Sun in space'),))

        data = db.SqlFTSearch( TABLEname, ['Col1', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'space' )
        self.assertEqual(data, (('The new', 'Cat in home', 'Sun in space'),))

        db.SqlDropTable( TABLEname )


    def test_17_SqlExecuteManyInsert(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        db.SqlExecuteManyInsert(TABLEname, ['Col1', 'Col2'], [
            ['One', 1],
            ['Two', 2],
            ['Three', 3],
        ])

        db.SqlDropTable(TABLEname)


    def test_18_SqlExecuteManyRead(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)
        db.SqlExecuteManyInsert(TABLEname, ['Col1', 'Col2'], [
            ['One', 1],
            ['Two', 2],
            ['Three', 3],
        ])

        data = db.SqlExecuteManyRead(TABLEname, ['Col1', 'Col2'], 'Col2 = 1')
        self.assertEqual(data, (('One', 1),))

        data = db.SqlExecuteManyRead(TABLEname, ['Col1', 'Col2'], 'Col2 = 1 OR Col2 = 2')
        self.assertEqual(data, (('One', 1), ('Two', 2)))

        db.SqlDropTable(TABLEname)


    def test_19_Insert_Update_Select(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        db.SqlInsert( TABLEname, {'Col1':'The cat 1', 'Col2':1} )
        db.SqlUpdate( TABLEname, {'Col1':'The cat 2'}, {'Col2': 1})
        res = db.SqlSearch( TABLEname, ['Col1'], ['Col1'], ['The cat 2'] )

        self.assertEqual(res, (('The cat 2',),))

        db.SqlDropTable(TABLEname)


    def test_20_InsertMany_Update_Select(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        db.SqlExecuteManyInsert( TABLEname, ['Col1', 'Col2'], [['The cat 1', 1], ['The cat 2', 2], ] )
        db.SqlUpdate( TABLEname, {'Col1':'The cat 2'}, {'Col2': 1})
        res = db.SqlSearch( TABLEname, ['Col1'], ['Col1'], ['The cat 2'] )

        self.assertEqual(res, (('The cat 2',), ('The cat 2',)))

        db.SqlDropTable(TABLEname)

    def test_21_SqlUpdate_affected(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]
        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        c = db.SqlInsert( TABLEname, {'Col1':'The cat 1', 'Col2':1} )
        c = db.SqlUpdate( TABLEname, {'Col1':'The cat 2'}, {'Col2':1} )

        self.assertEqual(c.rowcount, 1)

        db.SqlDropTable(TABLEname)


    def test_22_SqlInsert_PK(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ['ID:PrimaryKeyAuto', "Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]

        db.SqlCreateTable(TABLEname, ColumnsNamesType)
        c = db.SqlInsert( TABLEname, {'Col1':'The cat 1', 'Col2':1} )
        c = db.SqlUpdate( TABLEname, {'Col1':'The cat 2'}, {'Col2':1} )

        self.assertEqual(c.rowcount, 1)

        db.SqlDropTable(TABLEname)


    def test_23_SqlInsert_PK_Indexes(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ['ID:PrimaryKeyAuto', "Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:VARCHAR"]

        db.SqlCreateTable(TABLEname, ColumnsNamesType, ['Col2', 'Col6'], ['Col1', 'Col5'])

        db.SqlDropTable(TABLEname)


    def test_24_SqlInsert_PK_rowid(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ['ID:PrimaryKeyAuto', "Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text",
            "Col6:Text"]

        db.SqlCreateTable(TABLEname, ColumnsNamesType)

        c = db.SqlInsert(TABLEname, {'Col1': 'The cat 1', 'Col2': 1})
        self.assertEqual(c.rowcount, 1)
        self.assertEqual(c.lastrowid, 1)

        c = db.SqlInsert(TABLEname, {'Col1': 'The cat 1', 'Col2': 1})
        self.assertEqual(c.lastrowid, 2)

        db.SqlDropTable(TABLEname)


    def test_25_CaseInsense(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ['ID:PrimaryKeyAuto', "Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text",
            "Col6:Text"]

        db.SqlCreateTable(TABLEname, ColumnsNamesType, ['Col1'])
        c = db.SqlInsert(TABLEname, {'Col1': 'The cat 1', 'Col2': 1})

        r = db.SqlSearch(TABLEname, ['Col1'], ['Col1'], ['The cat 1'])
        self.assertEqual(r, (('The cat 1',),))

        r = db.SqlSearch(TABLEname, ['Col1'], ['Col1'], ['the cat 1'])
        self.assertEqual(r, (('The cat 1',),))

        r = db.SqlSearch(TABLEname, ['Col1'], ['Col1'], ['THE CAT 1'])
        self.assertEqual(r, (('The cat 1',),))

        db.SqlDropTable(TABLEname)


    def test_25_CaseInsense_FTS(self):
        db = sqlinterface.SQLInterface()
        db.UseDatabase(self.DBname)
        TABLEname = "test_table"
        ColumnsNamesType = ['ID:PrimaryKeyAuto', "Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text",
            "Col6:Text"]

        db.SqlCreateTable(TABLEname, ColumnsNamesType, ['Col1'], ['Col1'])
        c = db.SqlInsert(TABLEname, {'Col1': 'The cat 1', 'Col2': 1})

        r = db.SqlFTSearch(TABLEname, ['Col1'], ['Col1'], 'cat')
        self.assertEqual(r, (('The cat 1',),))

        r = db.SqlFTSearch(TABLEname, ['Col1'], ['Col1'], 'Cat')
        self.assertEqual(r, (('The cat 1',),))

        r = db.SqlFTSearch(TABLEname, ['Col1'], ['Col1'], 'CAT')
        self.assertEqual(r, (('The cat 1',),))

        db.SqlDropTable(TABLEname)


if __name__ == '__main__':
    unittest.main()

