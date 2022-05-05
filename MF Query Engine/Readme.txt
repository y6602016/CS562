Please follow the following instructions to execute the program:

1. The program is developed with "Python3" in virtual environment "virtualenv". 
Please make sure you have installed Python3, pip3 and virtulenv.

You can install virtualenv by pip3 with the command:
pip3 install virtualenv

2. Before executing the main program, please enter the virtual environment. 
The virtual environment has been established in the folder, you don't need to install a new one.

You can enter virtual environment with the command: 
source myenv/bin/activate (for Mac)
.\myenv\Scripts\activate  (for Windows)

!! Please make sure you enter the virtual environment, it's importand and required to execute the program

3. The only third-party package used in the program is psycopg2. It's already installed in the
default virtual environment. 
You can check it by with the command:
pip3 freeze

If there is no psycopg2, you can install it in the virtual environment by the command: 
pip3 install -r requirements.txt  (Please make sure you are in the vurtual environment)

4. Please modify the "database.ini" file in Config folder to config your local database
There are total 4 parameters need to be set: host, database, user, password
Ex:
[postgresql]
host=localhost
database=project
user=mike
password="" 

5. The only input file used by the program is "query_input.txt" file. The inputs are in the format
of 6 operators. You can use the provided test cases or modify the inputs manually.

(1) If you use the test cases:
You can use the test cases from the files in the folder "Test cases", there are 10 test cases for 
testing. You can copy the part above "<Standard SQL>" and paste it in query_input.txt. Please don't
copy the part from "<Standard SQL>"

(2) If you modify the query_input.txt manually:
You should follow the following requirements:
  1. For SELECT ATTRIBUTE(S):
    1. Must select all grouping attributes
    2. Except grouping attributes, please use "." to definde grouping variable's attributes, ex: 1.year, 1.state
    3. If grouping variable's attributes are projected, please make sure the grouping variable can be narrowed to
      a single row. (ex: 1.quant == 0_max_quant) If the grouping variable is defined to multiple rows, it doesn't
      make sense to project the specific row's attribute.

  2. For NUMBER OF GROUPING VARIABLES(n):
    1. Must set non-negtive value, value should be >= 0

  3. For F-VECT([F]):
    1. Please write the aggregation function in the format "<grouping variable>_<aggregation function>_<attribute>"
      aggregation function options: avg, sum, max, min, count
      ex: 0_avg_quant, 1_avg_quant, 1_count_quant, 2_avg_quant, 2_count_quant
      
  4. For SELECT CONDITION-VECT([σ]):
    1. Space between operators is required, ex: 1.cust == cust and 1.quant > 0_avg_quant
    2. Always start with grouping attribute definition
    3. All conditions are in the same line, no new line
    4. Order is in the same order of grouping variables
    5. Using '==' to express 'equal'
    6. If there is no condition, please put an empty new line
      ex: 
        SELECT CONDITION-VECT([σ]):

        HAVING_CONDITION(G):

  5. For HAVING_CONDITION(G):
    1. Space between operators is required, ex: 1.cust == cust and 1.quant > 0_avg_quant
    2. All conditions are in the same line, no new line
    3. Using '==' to express 'equal'
    4. If there is no condition, please put an empty new line
      ex: 
        SELECT CONDITION-VECT([σ]):

        HAVING_CONDITION(G):
        

6. After the input file is defined well, you can execute the program with the command:
python3 mfqe.py    (Please make sure you are in the vurtual environment)

7. After executing the program, it generates a program named "query.py"
You can execute the output program to view the data witht the command:
python3 query.py    (Please make sure you are in the vurtual environment)

