def processSchema(cur):
  query = "select column_name, data_type from information_schema.columns where table_name = 'sales' order by ordinal_position"
  cur.execute(query)
  schema = {attr[0] : [str(i), attr[1]] for i, attr in enumerate(cur.fetchall())}
  return schema