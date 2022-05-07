def convertMFStructure(S, F, G, schema):
  """The function to convert the S/F lists and G to mf_structure hashtable"""

  mf_structure = {}
  mf_type = {}

  for attr in S:
    mf_structure[attr] = None
    getType(attr, mf_type, schema)
      
  
  for attr in F:
    mf_structure[attr] = None
    getType(attr, mf_type, schema)

  if len(G) and len(G[0]):
    splitted = G[0].split()
    for word in splitted:
      if "." in word or "_" in word:
        mf_structure[word] = None
        getType(word, mf_type, schema)
  

  return mf_structure, mf_type


def getType(attr, mf_type, schema):
  """The function to get columns data type"""

  # type only works against the db of the project, it can expand to more types 
  type = { 
    'character varying': "str",
    'character': "str",
    'integer': "int",
    'date': "date",
    'datetime': 'datetime',
    'float': "float",
  }

  try:
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
  except (KeyError) as error:
    raise(KeyError("Non-existent Column " + str(error)))


