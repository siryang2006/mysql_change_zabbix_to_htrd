import MySQLdb   #yum install MySQL-python
import sys

def mysql_db_string_replace(host, user_name, password, port, db_name, src_string, des_string):
  try:
    conn = MySQLdb.connect(host, user_name, password, db_name, port)
  except MySQLdb.Error, e:
     print "connect mysql error:", host, user_name, password, db_name, port
     return -1

  cur = conn.cursor()
  sql = "select CONCAT ( 'update ', table_schema, '.', table_name, ' set ', column_name, '=replace(', column_name,',''"+src_string+"'', ''"+des_string+"'');') as statement From information_schema.columns Where (data_type Like '%char%' or data_type like'%text' or data_type like '%binary') And table_schema ='"+db_name+"'"; 
  count = cur.execute(sql)

  rows = cur.fetchall()
  #print rows
  #cur.executemany(rows, [])
  count = 0
  try:
    for (row,) in rows:
      if not row:
          break

      print row
      this_count = cur.execute(row)
      print "changed count:",this_count
      count = count + this_count
    conn.commit()
    print "total changed count :", count
  except MySQLdb.Error, e:
      conn.rollback()
      cur.close()
      conn.close()
      return -1
    
  cur.close()
  conn.close()

if __name__ == '__main__':
    if(len(sys.argv)<8):
      print "useage<path><mysql host ip><mysql user name><mysql password><mysql port><mysql database name><source string to replace><destination string to replace>"
      
    else:
      mysql_db_string_replace(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5], sys.argv[6], sys.argv[7])
