def menu():
  """The menu function for users to key in inputs by keyboard"""

  valid_select = False
  while not valid_select:
    value = input("\nPlease select the way to read operands:\n1: From the query_input.txt file\n2: Enter value by keyboard\n")
    if value == "1":
      valid_select = True
    elif value == "2":
      file = open('query_input.txt',mode='w')

      file.write("SELECT ATTRIBUTE(S):\n")
      inputs = []
      while 1:
        value = input("\nPlease enter a selected attribute.\nEx: 1_sum_quant\nEnter -1 to end\n")
        if not value:
          continue
        elif value == "-1":
          break
        else:
          inputs.append(value)
      S = ", ".join(inputs)
      file.write(S + "\n")
      inputs = []

      file.write("NUMBER OF GROUPING VARIABLES(n):\n")
      while 1:
        value = input("\nPlease enter the number of grouping variable.\nEx: 3\n")
        if not value:
          continue
        elif int(value) < 0:
          print("Please enter a value >= 0\n")
        else:
          file.write(value + "\n")
          break
      

      file.write("GROUPING ATTRIBUTES(V):\n")
      while 1:
        value = input("\nPlease enter a grouping attribute.\nEx: cust\nEnter -1 to end\n")
        if not value:
          continue
        elif value == "-1":
          break
        else:
          inputs.append(value)
      V = ", ".join(inputs)
      file.write(V + "\n")
      inputs = []

      file.write("F-VECT([F]):\n")
      while 1:
        value = input("\nPlease enter an aggregation function.\nEx: 1_sum_quant\nPlease enter aggregation function in the same order with grouping variable\nEnter -1 to end\n")
        if not value:
          continue
        elif value == "-1":
          break
        else:
          inputs.append(value)
      F = ", ".join(inputs)
      file.write(F + "\n")
      inputs = []

      file.write("SELECT CONDITION-VECT([σ]):\n")
      while 1:
        value = input("\nPlease enter an condition to define grouping variable.\nEx: 1.cust == cust and 1.state == ’NY’\nPlease MUST put a space between all operators\nPlease enter aggregation function in the same order with grouping variable\nEnter -1 to end\n")
        if not value:
          continue
        elif value == "-1":
          break
        else:
          inputs.append(value)
      conditions = ", ".join(inputs)
      file.write(conditions + "\n")
      inputs = []

      file.write("HAVING_CONDITION(G):\n") 
      while 1:
        value = input("\nPlease enter the having clause.\nEx: 1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant\nIf no having clause, just enter -1\n")
        if not value:
          continue
        elif value == "-1":
            break
        else:
          file.write(value + "\n")
          break

      valid_select = True
    else:
      print("Invalid input, please select 1 or 2")


def convertOperands(input_file):
  """The function to convert inputs to seperated lists, all inputs are stored as string type"""


  # use a dict to store all operands' values
  operands = {
    "SELECT ATTRIBUTE(S)" : list(),
    "NUMBER OF GROUPING VARIABLES(n)": list(),
    "GROUPING ATTRIBUTES(V)": list(),
    "F-VECT([F])": list(),
    "SELECT CONDITION-VECT([σ])": list(),
    "HAVING_CONDITION(G)": list()
  }

  # split input with new line
  chunks = input_file.split('\n')
  operand = ""

  for chunk in chunks:
    # if the chunk is an operand, record it's name then read next line
    if chunk[:-1] in operands:
      operand = chunk[:-1]
      continue

    # if not an operand, it's a value, split it then put into operands dict
    spliited = chunk.split(", ")
    for value in spliited:
      operands[operand].append(value)

  if not operands["SELECT ATTRIBUTE(S)"] or not operands["NUMBER OF GROUPING VARIABLES(n)"]:
    raise (ValueError("Empty SELECT ATTRIBUTE(S) or NUMBER OF GROUPING VARIABLES(n)"))


  if int(operands["NUMBER OF GROUPING VARIABLES(n)"][0]) < 0:
    raise (ValueError("NUMBER OF GROUPING VARIABLES(n) must be non-negative value"))
  

  if int(operands["NUMBER OF GROUPING VARIABLES(n)"][0]) != len(operands["SELECT CONDITION-VECT([σ])"]):
    raise (ValueError("NUMBER OF GROUPING VARIABLES(n) is wrong"))

  for group_attr in operands["GROUPING ATTRIBUTES(V)"]:
    if group_attr not in operands["SELECT ATTRIBUTE(S)"]:
      raise (ValueError("SELECT ATTRIBUTE(S) must contain all grouping attributes"))
  
  return operands
