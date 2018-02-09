Mini SQL engine which can parse any permutation and combinations of SQL that MySQL permits

To Run
  - Run the bash script on the terminal with query as command line arguement.
  - $bash 20172062.sh "Select * from table_name where some_condition"

It is assumed that "metadata.txt" and "tables.csv" is in the same directory in which program resides.

Engine can fetch data from single as well multiple tables using join.

Moreover, aggregate functions can also be used such as max, min, avg, count, sum and distinct(single attribute).

Engine is also able to execute "where clause" with multiple AND/OR operators.

Conditions support binary operators like '=','>','<','<=','>=' and '!='

All possible errors are handled.

Following are some examples of queries supported:-

"Select * from table1"
"sElect A,B froM table1"
"sElect A,B froM table1 where A>=0"
"Select A,table1.B from table1,table2"
"Select avg(A) from table1 where A=43 and B<0"
"Select count(A) from table1 where A=43 or B<0"
"Select distinct(A) from table1,table2 where A=43 and B<0 or C!=100"
"Select max(A) from table1,table2,table3 where A=43 and table2.B<=0"
"Select A,table2.B,E from table1,table2,table3 where A!=43 and table2.B>table1.B"

