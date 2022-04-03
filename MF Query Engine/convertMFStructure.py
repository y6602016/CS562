def convertMFStructure(input_file, cursor):
  
  # query = "select column_name, data_type from information_schema.columns where table_name = 'sales'"
  # cursor.execute(query)

  mf_structure = {}

  for attr in input_file["SELECT ATTRIBUTE(S)"]:
    mf_structure[attr] = None
  
  for attr in input_file["F-VECT([F])"]:
    mf_structure[attr] = None
  
  return mf_structure

  
  

