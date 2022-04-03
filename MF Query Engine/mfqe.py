import psycopg2
import collections
from config import config
from convertMFStructure import convertMFStructure
from inputProcess import checkOperands, menu
from outputProcess import writeMFStructure 

globalIndentation = 1
script = "import psycopg2\nimport collections\ndef query():\n"

def connect():
  global globalIndentation
  template = open('template.txt',mode='r')
  script = template.read() + "\n"
  template.close()

  # script = "import psycopg2\nimport collections\ndef query():\n"
  menu()
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

    # close the file
    file.close()

    operands = checkOperands(input_file)
    if not operands:
      print("Input values are not valid")
    else:
      # 1. Call a function to produce MF-Struture
      mf_structure = convertMFStructure(operands, cur)
      script = writeMFStructure(mf_structure, script, globalIndentation)
      file = open('output.py',mode='w')
      file.write(script)
      file.close()

      # group = collections.defaultdict(lambda:mf_structure)
      

    
    
  

    # close the communication with the PostgreSQL
    cur.close()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()


if __name__ == '__main__':
    connect()