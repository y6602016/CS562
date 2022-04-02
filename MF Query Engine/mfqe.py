from logging.config import valid_ident
import psycopg2
import collections
from config import config
from convertMFStructure import convertMFStructure
from checkOperands import checkOperands

def process_input():
  valid_select = False
  while not valid_select:
    value = input("Please select the way to read operands:\n1: From the query_input.txt file\n2: Enter value by keyboard\n")
    if value == "1":
      valid_select = True
    elif value == "2":
      file = open('query_input2.txt',mode='w')

      file.write("SELECT ATTRIBUTE(S):\n")
      inputs = []
      while 1:
        value = input("Please enter a selected attribute.\nEx: 1_sum_quant\nEnter -1 to end\n")
        if value == "-1":
          break
        else:
          inputs.append(value)
      S = ", ".join(inputs)
      file.write(S + "\n")
      inputs = []

      file.write("NUMBER OF GROUPING VARIABLES(n):\n")
      N = input("Please enter the number of grouping variable.\nEx: 3\n")
      file.write(N + "\n")

      file.write("GROUPING ATTRIBUTES(V):\n")
      while 1:
        value = input("Please enter a grouping attribute.\nEx: cust\nEnter -1 to end\n")
        if value == "-1":
          break
        else:
          inputs.append(value)
      V = ", ".join(inputs)
      file.write(V + "\n")
      inputs = []

      file.write("F-VECT([F]):\n")
      while 1:
        value = input("Please enter an aggregation function.\nEx: 1_sum_quant\nEnter -1 to end\n")
        if value == "-1":
          break
        else:
          inputs.append(value)
      F = ", ".join(inputs)
      file.write(F + "\n")
      inputs = []

      file.write("SELECT CONDITION-VECT([σ]):\n")
      while 1:
        value = input("Please enter an condition to define grouping variable.\n1.cust = cust and 1.state=’NY’\nEnter -1 to end\n")
        if value == "-1":
          break
        else:
          inputs.append(value)
      conditions = "\n".join(inputs)
      file.write(conditions + "\n")
      inputs = []

      file.write("HAVING_CONDITION(G):\n")
      G = input("Please enter the having clause.\nEx: 1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant\n")
      file.write(G + "\n")

      valid_select = True
    else:
      print("Invalid input, please select 1 or 2")



      




def connect():
  conn = None
  try:
    # ===== Connect to db =====
    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    conn = psycopg2.connect(**params)

    # create a cursor
    cur = conn.cursor()
    # ===== Connect to db =====


    # Open a file: file
    file = open('query_input.txt',mode='r')
    
    # read all lines at once
    input_file = file.read()
    checkOperands(input_file)
    # close the file
    file.close()

    # 1. Call a function to produce MF-Struture
    convertMFStructure(input_file, cur)

    
    
  

    # close the communication with the PostgreSQL
    cur.close()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      # print('Database connection closed.')


if __name__ == '__main__':
    # connect()
    process_input()