import sqlalchemy
import pandas

if __name__ == '__main__':
    contact_center = pandas.read_csv('contac_center_data.csv', sep=';')
    delitos_por_municipio = pandas.read_csv('delitos_por_municipio.csv', sep=';')
    renta_por_hogar = pandas.read_csv('renta_por_hogar.csv', sep=';')

    database_connection = sqlalchemy.create_engine('mysql+pymysql://root:my-secret-pw@172.17.0.2/superset').connect()
    contact_center.to_sql(con=database_connection, name='contact_center', if_exists='replace')
    delitos_por_municipio.to_sql(con=database_connection, name='delitos_por_municipio', if_exists='replace')
    renta_por_hogar.to_sql(con=database_connection, name='renta_por_hogar', if_exists='replace')