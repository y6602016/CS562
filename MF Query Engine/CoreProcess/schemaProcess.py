def processSchema(table_name, cur):
  """The function to receive the db column type and index in the order"""

  # check whether the table exists
  query = f"select * from information_schema.tables where table_name = '{table_name}'"
  cur.execute(query)
  if cur.rowcount == 0:
    raise(KeyError("Non-existent Table " + table_name))

  query = f"select column_name, data_type from information_schema.columns where table_name = '{table_name}' order by ordinal_position"
  cur.execute(query)
  schema = {attr[0] : [str(i), attr[1]] for i, attr in enumerate(cur.fetchall())}
  return schema