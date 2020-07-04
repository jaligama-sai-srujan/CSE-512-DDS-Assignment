import psycopg2

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    table = open(ratingsfilepath,'r')
    cursor = openconnection.cursor()
    cursor.execute("create table %s(%s int , %s int , %s float)"%(ratingstablename,USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME))
    for i in table:
        row = i.split('::')
        cursor.execute("insert into %s values(%s,%s,%s)"%(ratingstablename,row[0],row[1],row[2]))
    cursor.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    tablenames = []
    cursor = openconnection.cursor()
    interval = 5.0/numberofpartitions
    for i in range(numberofpartitions):
        tablenames.append(RANGE_TABLE_PREFIX+str(i))
    for i in range(numberofpartitions):
        cursor.execute("create table %s(%s int , %s int , %s float)"%(tablenames[i],USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME))
        if i==0:
            cursor.execute(" INSERT INTO %s SELECT * FROM %s WHERE %s >= %f and %s <= %f"%(tablenames[i],ratingstablename ,RATING_COLNAME, i * interval,RATING_COLNAME,(i * interval)+interval))
        else:
            cursor.execute(" INSERT INTO %s SELECT * FROM %s WHERE rating > %d and rating <= %d"%(tablenames[i],ratingstablename,i * interval,(i * interval)+interval))
    cursor.execute("create table %s(%s int)"%('metadata_range','numofpartition'))
    cursor.execute(" INSERT INTO %s values(%d)"%('metadata_range',numberofpartitions))
    cursor.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    tablenames = []
    cursor = openconnection.cursor()
    for i in range(numberofpartitions):
        cursor.execute("create table %s(%s int , %s int , %s float)"%(RROBIN_TABLE_PREFIX+str(i),USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME))
    cursor.execute(" SELECT * FROM %s"%(ratingstablename))
    data = cursor.fetchall()
    table_number = 0
    for tuple in data:
        cursor.execute(" INSERT INTO %s values(%s,%s,%f)"%(RROBIN_TABLE_PREFIX+str(table_number),tuple[0],tuple[1],tuple[2]))
        table_number = (table_number+1)%numberofpartitions
    cursor.execute("create table metadata_rrobin(numofpartition int,tablenumber int)")
    cursor.execute(" INSERT INTO metadata_rrobin values(%d,%d)"%(numberofpartitions,table_number))
    cursor.close()
    openconnection.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("SELECT * FROM %s"%('metadata_rrobin'))
    metadata = cursor.fetchone()
    numberofpartition = metadata[0]
    table_number = metadata[1]
    cursor.execute(" INSERT INTO %s values(%d,%d,%f)"%(ratingstablename,userid, itemid, rating))
    cursor.execute(" INSERT INTO %s values(%d,%d,%f)"%(RROBIN_TABLE_PREFIX+str(table_number),userid, itemid, rating))
    table_number = (table_number+1)%numberofpartition
    cursor.execute("UPDATE metadata_rrobin set tablenumber = %d"%(table_number))
    cursor.close()
    openconnection.commit()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("SELECT * FROM %s"%('metadata_range'))
    numberofpartitions = cursor.fetchone()[0]
    interval = 5.0/numberofpartitions
    for i in range(numberofpartitions):
        if i==0:
            if rating >= i * interval and rating <= (i * interval)+interval:
                break
        else:
            if rating >= i * interval and rating <= (i * interval)+interval:
                break
    cursor.execute(" INSERT INTO %s values(%d,%d,%f)"%(ratingstablename,userid, itemid, rating))
    cursor.execute(" INSERT INTO %s values(%d,%d,%f)"%(RANGE_TABLE_PREFIX+str(i),userid, itemid, rating))
    cursor.close()
    openconnection.commit()

def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()


#loadRatings('ratings','test_data1.txt',getOpenConnection())
#rangePartition('ratings',12,getOpenConnection())

