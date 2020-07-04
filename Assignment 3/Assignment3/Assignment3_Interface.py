#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import _thread
import threading 

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def parallel_sort_thread(min,max,col,num,cursor,input,OutputTable,openconnection):
    cursor.execute("create table Partition"+str(num)+"(like "+input+") ")
    cursor.execute("insert into Partition"+str(num)+" select * from "+input+" where "+col+" >="+str(min)+" and "+col+" <"+str(max))

    openconnection.commit()
    
    
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cursor = openconnection.cursor()
    threads = []
    cursor.execute("select DISTINCT "+SortingColumnName+" from "+InputTable+"")
    total_list = cursor.fetchall()
    total = len(total_list)
    cursor.execute("select MIN("+SortingColumnName+"),MAX("+SortingColumnName+") from "+InputTable+"")
    minmax = cursor.fetchall()[0]
    cursor.execute("create table "+OutputTable+"(like "+InputTable+") ")
    step = (minmax[1]-minmax[0])/4.0
    min = minmax[0]
    step = total/5.0
    for i in range(5):
        t = threading.Thread(target=parallel_sort_thread, args=(min,min+step,SortingColumnName,i,cursor,InputTable,OutputTable,openconnection))
        t.start()
        threads.append(t)
        min += step
    for thread in threads:
        thread.join()
        newTableName = "partition" + str(threads.index(thread))
        cursor.execute("INSERT INTO %s SELECT * FROM %s" %(OutputTable,newTableName))
    openconnection.commit();
    


def parallel_join_thread(min,max,col1,col2,num,cursor,input1,input2,OutputTable,openconnection):
    cursor.execute("create table Partition1"+str(num)+"(like "+input1+") ")
    cursor.execute("insert into Partition1"+str(num)+" select * from "+input1+" where "+col1+" >="+str(min)+" and "+col1+" <"+str(max))
    cursor.execute("create table Partition2"+str(num)+"(like "+input2+") ")
    cursor.execute("insert into Partition2"+str(num)+" select * from "+input2+" where "+col2+" >="+str(min)+" and "+col2+" <"+str(max))
    cursor.execute("insert into "+OutputTable+" select * from Partition1"+str(num)+",Partition2"+str(num)+" where Partition1"+str(num)+"."+col1+ " = Partition2"+str(num)+"."+col2)
    openconnection.commit()
    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cursor = openconnection.cursor()
    threads = []
    cursor.execute("select DISTINCT "+Table1JoinColumn+" from "+InputTable1+"")
    total_list = cursor.fetchall()
    total = len(total_list)
    cursor.execute("select MIN("+Table1JoinColumn+"),MAX("+Table1JoinColumn+") from "+InputTable1+"")
    minmax = cursor.fetchall()[0]
    cursor.execute("create table "+OutputTable+"(like "+InputTable1+", like "+InputTable2+") ")
    min = minmax[0]
    step = (minmax[1]-minmax[0])/4.0
    
    #print(step)
    for i in range(5):
        t = threading.Thread(target=parallel_join_thread, args=(min,min+step,Table1JoinColumn,Table2JoinColumn,i,cursor,InputTable1,InputTable2,OutputTable,openconnection))
        t.start()
        threads.append(t)
        min += step
    for thread in threads:
        thread.join()
    openconnection.commit();
        
    


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


