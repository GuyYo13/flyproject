import mysql.connector as mc


# MySQL

host = 'localhost'

mysql_port = 3306

mysql_database_name = 'classicmodels' #'srcdb'

mysql_table_name = 'fly' #'src_events'

mysql_username = 'naya'

mysql_password = 'NayaPass1!'




# connector to mysql

mysql_conn = mc.connect(

user=mysql_username,

password=mysql_password,

host=host,

port=mysql_port,

autocommit=True, # <--

database=mysql_database_name)


