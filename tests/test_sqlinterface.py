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


if __name__ == '__main__':
    unittest.main()

