def menu():
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
      while 1:
        N = input("Please enter the number of grouping variable.\nEx: 3\n")
        if int(N) < 0:
          print("Please enter a value >= 0\n")
        else:
          break
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
      conditions = ", ".join(inputs)
      file.write(conditions + "\n")
      inputs = []

      file.write("HAVING_CONDITION(G):\n")
      G = input("Please enter the having clause.\nEx: 1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant\n")
      if value == "-1":
          break
      else:
        file.write(G + "\n")

      valid_select = True
    else:
      print("Invalid input, please select 1 or 2")


def checkOperands(input_file):
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
    return False


  if int(operands["NUMBER OF GROUPING VARIABLES(n)"][0]) < 0:
    return False
  

  if int(operands["NUMBER OF GROUPING VARIABLES(n)"][0]) >= 0 and not operands["GROUPING ATTRIBUTES(V)"]:
    return False
  
  return operands
