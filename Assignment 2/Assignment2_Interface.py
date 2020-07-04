
import psycopg2
import os
import sys
import Assignment1
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    
    cursor = openconnection.cursor()
    cursor.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    tables = cursor.fetchall()
    tables = tables[0][0]
    #original_stdout = sys.stdout
    f = open(outputPath, 'w+')
    #sys.stdout = f
    for table in range(tables):
        cursor.execute("select * from %s where rating >= %s and rating <= %s"%('RangeRatingsPart'+str(table),ratingMinValue,ratingMaxValue))
        output = cursor.fetchall()
        for row in output:
            f.write('RangeRatingsPart'+str(table)+','+str(row[0])+','+str(row[1])+','+str(row[2])+'\n')
    cursor.execute("SELECT * FROM RoundRobinRatingsMetadata")
    rr_table = cursor.fetchall()
    rr_table = rr_table[0][0]
    for table in range(rr_table):
        cursor.execute("select * from %s where rating >= %s and rating <= %s"%('RoundRobinRatingsPart'+str(table),ratingMinValue,ratingMaxValue))
        output = cursor.fetchall()
        for row in output:
            f.write('RoundRobinRatingsPart'+str(table)+','+str(row[0])+','+str(row[1])+','+str(row[2])+'\n')
    #sys.stdout = original_stdout
    f.close()
    
def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    cursor = openconnection.cursor()
    cursor.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    tables = cursor.fetchall()
    tables = tables[0][0]
    #original_stdout = sys.stdout
    f = open(outputPath, 'w+')
    #sys.stdout = f
    for table in range(tables):
        cursor.execute("select * from %s where rating = %s"%('RangeRatingsPart'+str(table),ratingValue))
        output = cursor.fetchall()
        for row in output:
            f.write('RangeRatingsPart'+str(table)+','+str(row[0])+','+str(row[1])+','+str(row[2])+'\n')
    cursor.execute("SELECT * FROM RoundRobinRatingsMetadata")
    rr_table = cursor.fetchall()
    rr_table = rr_table[0][0]
    for table in range(rr_table):
        cursor.execute("select * from %s where rating = %s "%('RoundRobinRatingsPart'+str(table),ratingValue))
        output = cursor.fetchall()
        for row in output:
            f.write('RoundRobinRatingsPart'+str(table)+','+str(row[0])+','+str(row[1])+','+str(row[2])+'\n')
    #sys.stdout = original_stdout
    f.close()
