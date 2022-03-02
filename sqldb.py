from sqlalchemy import create_engine
import pandas as pd

# BD
host = 'trainbee-billing.c6zttpwnotj0.us-west-2.rds.amazonaws.com'
port = 3306
database = 'datawarehouse'
user = 'dataengineer'
pswd = 'goXC7KH!WfPc'


mysql_url = f'mysql+pymysql://{user}:{pswd}@{host}/{database}'
engine = create_engine(mysql_url, pool_pre_ping=True)
# print(engine.table_names())

#df = pd.read_sql_table(table_name='companies', con=connection)