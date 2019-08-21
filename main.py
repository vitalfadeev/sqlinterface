import sqlinterface
import logging


logging.basicConfig(level=logging.DEBUG)
logging.addLevelName(logging.DEBUG,   "DBG")
logging.addLevelName(logging.INFO,    "NFO")
logging.addLevelName(logging.WARNING, "WRN")
logging.addLevelName(logging.ERROR,   "ERR")


@sqlinterface.profile
def make_test_data(insert_rows):
    data = []
    for i in range(insert_rows):
        data.append(['The cat' + str(i % 100), i%100, i])

    return data


def main():
    db = sqlinterface.SQLInterface()

    company_id = 0
    DBname = 'test-BrainData-{}'.format(company_id)

    # db
    db.SqlDropDatabase(DBname)
    db.SqlCreateDatabase(DBname)
    db.UseDatabase(DBname)

    # table
    TABLEname = "test_table"
    ColumnsNamesType = ["Col1:Text", "Col2:INT", "Col3:FLOAT", "Col4:DATETIME", "Col5:Text", "Col6:Text"]

    db.SqlCreateTable(TABLEname, ColumnsNamesType, ['Col2'], ['Col1', 'Col5', 'Col6'])

    # make data
    INSERT_ROWS = 10000
    data = make_test_data(INSERT_ROWS)

    # insert
    c = db.SqlExecuteManyInsert(TABLEname, ['Col1', 'Col2', 'Col3'], data)
    logging.debug("   inserted: %s", c.rowcount)

    # updating
    c = db.SqlUpdate( TABLEname, {'Col1':'updated'}, {'Col2':0} )
    logging.debug("   updated: %s", c.rowcount)

    # searching with index
    res = db.SqlSearch( TABLEname, ['Col1', 'Col2', 'Col3'], ['Col2'], [0] )
    logging.debug("   selected: %s", len(res))

    # searching with FTS
    res = db.SqlFTSearch( TABLEname, ['Col1', 'Col2', 'Col3', 'Col5', 'Col6'], ['Col1', 'Col5', 'Col6'], 'cat1' )
    logging.debug("   selected fts: %s", len(res))


if __name__ == '__main__':
    main()


