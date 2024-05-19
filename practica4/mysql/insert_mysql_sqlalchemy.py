import sqlalchemy
import pandas

if __name__ == '__main__':
    data = pandas.read_csv('ree_header.csv')
    data['Date'] = data['Date'].apply(lambda x: pandas.Timestamp(x))

    database_connection = sqlalchemy.create_engine('mysql+pymysql://root:my-secret-pw@172.17.0.3/superset').connect()
    data.to_sql(con=database_connection, name='test_table_sql', if_exists='replace')