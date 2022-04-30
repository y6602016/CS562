def convertMFStructure(input_file, schema):
  """convert the S and F lists to mf_structure hashtable"""

  mf_structure = {}
  mf_type = {}

  for attr in input_file["SELECT ATTRIBUTE(S)"]:
    mf_structure[attr] = None
    getType(attr, mf_type, schema)
      
  
  for attr in input_file["F-VECT([F])"]:
    mf_structure[attr] = None
    getType(attr, mf_type, schema)
  

  return mf_structure, mf_type


def getType(attr, mf_type, schema):
  # type only works against the db of the project, it can expand to more types
  type = { 
    'character varying': "str",
    'character': "str",
    'integer': "int",
    'date': "dt",
    'float': "float",
  }

  if "." in attr: # case1: normal attribute
      splitted = attr.split(".")
      mf_type[attr] = type[schema[splitted[1]][1]]
  elif "_" in attr: # case2: aggregation function
    splitted = attr.split("_")
    if splitted[1] == "count":
      mf_type[attr] = "int"
    elif splitted[1] == "avg":
      mf_type[attr] = "float"
    else:
      mf_type[attr] = type[schema[splitted[2]][1]]
  else: # case3: grouping attribute
    mf_type[attr] = type[schema[attr][1]]


