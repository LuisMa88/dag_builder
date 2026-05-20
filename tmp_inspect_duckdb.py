import duckdb
con = duckdb.connect('posts_data.duckdb')
print('SHOW TABLES ->', con.execute('SHOW TABLES').fetchall())
print('SHOW SCHEMAS ->', con.execute('SHOW SCHEMAS').fetchall())
print('TABLES ->', con.execute("SELECT table_schema, table_name FROM information_schema.tables ORDER BY table_schema, table_name").fetchall())
