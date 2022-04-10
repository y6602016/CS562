import psycopg2
import collections
from config import config
from convertMFStructure import convertMFStructure
from inputProcess import checkOperands, menu
from outputProcess import writeMFStructure, writeFirstScan, writeProject
from groupVariableProcess import processRel
global_indentation = 2
script = "import psycopg2\nimport collections\ndef query():\n"

def connect():
  global global_indentation
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

    query = "select column_name from information_schema.columns where table_name = 'sales' order by ordinal_position"
    cur.execute(query)
    schema = {attr[0] : str(i) for i, attr in enumerate(cur.fetchall())}

    if not operands:
      print("Input values are not valid")
    else:
      S = operands["SELECT ATTRIBUTE(S)"]
      N = operands["NUMBER OF GROUPING VARIABLES(n)"]
      V = operands["GROUPING ATTRIBUTES(V)"]
      F = operands["F-VECT([F])"]
      C = operands["SELECT CONDITION-VECT([σ])"]
      G = operands["HAVING_CONDITION(G)"]

      # 1. Call a function to produce MF-Struture
      mf_structure = convertMFStructure(operands, cur)
      
      script = writeMFStructure(mf_structure, script, global_indentation)

      # find attr which are in mf-structure but no
      script = writeFirstScan(V, F, schema, script, global_indentation)

      group_variable_fs, group_variable_conditions, depend_map = processRel(N, F, C)

      # topological sorting preparation
      edges = collections.defaultdict(list)   
      in_degrees = [0] * (int(N[0]) + 1)
      for value, depend_list in depend_map.items():
        for depend_val in depend_list:
          in_degrees[value] += 1
          edges[depend_val].append(value)

      queue = collections.deque([index for index, val in enumerate(in_degrees) if index > 0 and val == 0])
      while queue:
        q_length = len(queue)
        to_be_scan = []
        for i in range(q_length):
          val = queue.popleft()
          to_be_scan.append(val)
          for next_val in edges[val]:
            in_degrees[next_val] -= 1
            if in_degrees[next_val] == 0:
              queue.append(next_val)
        print(to_be_scan)
      
      script = writeProject(S, script, global_indentation)
      

    
    script += ('if __name__ == "__main__":\n' + (" " * global_indentation) + "query()")
  
    file = open('output.py',mode='w')
    file.write(script)
    file.close()
    # close the communication with the PostgreSQL
    cur.close()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()


if __name__ == '__main__':
    connect()