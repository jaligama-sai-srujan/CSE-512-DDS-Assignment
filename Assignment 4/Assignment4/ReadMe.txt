equijoin.java in present in Equijoin/Equijoin.
So while running the jar use Equijoin.Equijoin.equijoin as <class_with_main_function>.

Approach:
equijoinMapper: Mapper function reads the file in the HDFS and based on the join columns key value pairs are generated i.e., key is the join column and value is the whole line.
equijoinReducer: Reducer function splits the value into two based on the table name. Then append the table 1 value is added to table 2 value using ','. This is done because for the value of the join column we are appending tuple from table 1 with tuple from table 2.
Driver: Driver function is a main function which calls all the required functions.