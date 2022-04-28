import psycopg2
import collections
from config import config
from CoreProcess.convertMFStructure import *
from CoreProcess.groupVariableProcess import *
from CoreProcess.schemaProcess import *
from InputProcess.inputProcess import *
from OutputProcess.outputProcess import *



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
    file = open('query_input7.txt',mode='r')
  
    # read all lines at once
    input_file = file.read()

    # close the file
    file.close()

    operands = checkOperands(input_file)

    schema = processSchema(cur)

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
      mf_structure = convertMFStructure(operands)
      
      script = writeMFStructure(mf_structure, script, global_indentation)
      
      # ==============
      # Analyze the related columns of grouping variables that need to be updated in a scan
      # We divide the related columns to rel attributes and rel aggregate functions
      # 1. Process each grouping variables rel attributes
      group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate = processAttr(S, N, V, C, G)
      
      # 1. Process each grouping variables rel aggregate functions
      # 1. aggregate function
      # 2. dependency of other grouping variables
      # 3. dependency of other grouping variables' fnction
      group_variable_fs, depend_map, depend_fun = processRel(N, C, F)
      # ==============

      
      # initial scan
      script += ("\n\n" + (" " * global_indentation) + "#1th Scan:\n")
      script = writeFirstScan(V, F, schema, script, global_indentation)
      
      # remaining scan
      # topological sorting preparation
      edges = collections.defaultdict(list)   
      in_degrees = [0] * (int(N[0]) + 1)
      for value, depend_list in depend_map.items():
        for depend_val in depend_list:
          in_degrees[value] += 1
          edges[depend_val].append(value)

      scan_times = 1
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
        script += ("\n\n" + (" " * global_indentation) + f"#{scan_times + 1}th Scan:\n")
        scan_times += 1
        script = writeGroupVariableScan(V, C, schema, to_be_scan, group_variable_fs, depend_fun, 
          group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate, script, global_indentation)

      script = writeProject(S, G, schema, script, global_indentation)
      

    
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