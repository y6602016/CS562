import psycopg2
import collections
from config import config
from CoreProcess.convertMFStructure import *
from CoreProcess.groupVariableProcess import *
from CoreProcess.schemaProcess import *
from InputProcess.inputProcess import *
from OutputProcess.outputProcess import *


def connect():
  global_indentation = 2
  template = open('template.txt',mode='r')
  script = template.read() + "\n"
  template.close()

  menu()
  conn = None

  try:
    #=======================================
    #= connect to db and create the cursor =
    #=======================================
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()


    #=======================
    #= read the input file =
    #=======================
    file = open('query_input8.txt',mode='r')
    input_file = file.read()
    file.close()


    #===============================================
    #= convert operands and get the db column type =
    #===============================================
    operands = convertOperands(input_file)
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

      #=======================
      #= create mf-structure =
      #=======================
      mf_structure, mf_type = convertMFStructure(operands, schema)
      
      script += writeMFStructure(mf_structure, mf_type, global_indentation)
      

      # =======================================================================================
      # = analyze the related columns of grouping variables that need to be updated in a scan =
      # = we divide the related columns to "rel attributes" and "rel aggregate functions"     =
      # = (1). Process each grouping variables rel attributes                                 =
      group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate = processAttr(S, N, V, C, G)
      
      # = (2). Process each grouping variables rel aggregate functions                        =
      # =   1. aggregate function                                                             =
      # =   2. dependency of other grouping variables                                         =
      group_variable_fs, depend_map= processRel(N, C, F)
      # =======================================================================================

      
      #=========================
      #= output the first scan =
      #=========================

      script += ("\n\n" + (" " * global_indentation) + "#=====================================================\n")
      script += ((" " * global_indentation) + "#= the first scan to fill all grouping attributes    =\n")
      script += ((" " * global_indentation) + "#= and aggregatation function of grouping variable_0 =\n")
      script += ((" " * global_indentation) + "#=====================================================\n")
      script += ("\n" + (" " * global_indentation) + "#1th Scan:\n")
      script = writeFirstScan(V, F, schema, script, global_indentation)
      

      #====================================================================================
      #= output the remaining scans with topological sort to minimize the number of scans =
      #====================================================================================

      script += ("\n\n\n" + (" " * global_indentation) + "#===================================================================\n")
      script += ((" " * global_indentation) + "#= the following scans process all grouping variables              =\n")
      script += ((" " * global_indentation) + "#= non-dependent grouping variables are processed in the same scan =\n")
      script += ((" " * global_indentation) + "#===================================================================\n")

      # topological sort
      edges = collections.defaultdict(list)   
      in_degrees = [0] * (int(N[0]) + 1)
      for value, depend_set in depend_map.items():
        for depend_val in depend_set:
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
        script += ("\n" + (" " * global_indentation) + f"#{scan_times + 1}th Scan:\n")
        scan_times += 1
        script = writeGroupVariableScan(V, C, schema, to_be_scan, group_variable_fs, 
          group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate, script, global_indentation)


      #=========================
      #= output the projection =
      #=========================
      script += ("\n\n" + (" " * global_indentation) + "#===================================================\n")
      script += ((" " * global_indentation) + "#= formatter process and output the query result   =\n")
      script += ((" " * global_indentation) + "#===================================================\n")
      script = writeProject(S, G, schema, script, global_indentation)
      

    
    script += ('if __name__ == "__main__":\n' + (" " * global_indentation) + "query()")
  
    file = open('output.py',mode='w')
    file.write(script)
    file.close()
    cur.close()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()


if __name__ == '__main__':
    connect()