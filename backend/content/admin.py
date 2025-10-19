import pandas as pd
import source
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy import Integer, String, Float, Boolean, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.declarative import declarative_base
import psycopg2 as sql
import asyncpg
import asyncio
import nest_asyncio

nest_asyncio.apply()

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from io import BytesIO


def send_mail(subject, body, img = False):
    sender = "rodicecco5@gmail.com"
    receiver = "rodicecco@outlook.com"
    app_password = "xkwy gyko fnfd yigd"


    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if img != False:
        image = MIMEImage(img.read(), name='chart.png')
        image.add_header('Content-Disposition', 'attachment', filename="chart.png")
        msg.attach(image)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, app_password)
        server.sendmail(sender, receiver, msg.as_string())

    print("Email sent successfully!")

#Decorator to convert string dates into datetime type
def convert_dates(date_code):
    def convert_dates_arg(func):
        def wrapper(symbols, *args, **kwargs):
            result = func(symbols, *args, **kwargs)
            result[date_code] = pd.to_datetime(result[date_code])
            return result
        return wrapper
    return convert_dates_arg

class Database:

    def __init__(self, table_name, constraints):

        self.dtype_mapping = {'int64': 'BIGINT',
                                'object': 'VARCHAR',
                                'float64': 'FLOAT',
                                'bool': 'BOOLEAN',
                                'datetime64[ns]': 'TIMESTAMP'}
        
        self.host = '34.60.50.195'
        self.database = 'datadb'
        self.user = 'postgres'
        self.password = 'Vic24278175.'
        self.port = '5432'
        self.conn_string = f'postgresql://{self.user}:{self.password}@{self.host}/{self.database}'
        self.alt_conn_string = f'host={self.host} dbname={self.database} user={self.user} password={self.password} port={self.port}'
        self.table_name = table_name
        self.constraints = constraints

    def engine(self):
        return create_engine(self.conn_string)
    
    def connection(self):
        conn = sql.connect(self.alt_conn_string)
        return conn
        
    def create_table_stmt(self):
        dtypes = self.dtypes

        cols_string = ''.join([f'{col} {self.dtype_mapping[str(dtyp)]}, ' for col,dtyp in dtypes]).rstrip(', ')
        const_string = ', '.join(const for const in self.constraints).rstrip(', ')


        stmt = f'''CREATE TABLE IF NOT EXISTS {self.table_name} 
                    ({cols_string}, CONSTRAINT {self.table_name}_uniques UNIQUE ({const_string}) )'''
        
        return stmt

    def create_table(self):
        stmt = self.create_table_stmt()

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stmt)
                conn.commit()
        
        return True

    def gather_values(self):
        data = self.raw_data
        values = []
        for _ in data:
            temp_tup=[]
            for entry in _:
                temp_tup.append(_[entry])
            values.append(tuple(temp_tup))

        return values

    def upsert_sql(self, cursor):
        columns = self.columns
        constraints = self.constraints
        cols_string = ', '.join(columns).rstrip(', ')
        const_string = ', '.join(constraints).rstrip(', ')

        excludes = list(set(columns) - set(constraints))
        excludes_str = ', '.join([f'{exclude} = EXCLUDED.{exclude}' for exclude in excludes])

        values = self.gather_values()
        placeholders = '('+','.join(['%s' for x in columns])+')'
        morg = ','.join(cursor.mogrify(placeholders, i).decode('utf-8') for i in values)


        query = f'''INSERT INTO {self.table_name} ({cols_string})
                    VALUES {morg}
                    ON CONFLICT ({const_string})
                    DO UPDATE SET {excludes_str};'''
        
        return query


    def upsert_exec(self):

        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute(self.upsert_sql(cur))
            conn.commit()
        
        return True

    async def async_upsert_sql(self, pool):
        async with pool.acquire() as connection:
            columns = self.columns
            constraints = self.constraints
            values = self.gather_values()

            cols_string = ', '.join(columns)
            const_string = ', '.join(constraints)
            excludes = list(set(columns) - set(constraints))
            excludes_str = ', '.join([f'{exclude} = EXCLUDED.{exclude}' for exclude in excludes])

            placeholders = ', '.join(['$' + str(i + 1) for i in range(len(columns))])
            
            insert_query = f'''
                        INSERT INTO {self.table_name} ({cols_string})
                        VALUES ({placeholders})
                        ON CONFLICT ({const_string})
                        DO UPDATE SET {excludes_str};
                        '''

            await connection.executemany(insert_query, values)

    async def main(self):
        pool = await asyncpg.create_pool(user=self.user, password=self.password , database=self.database, host=self.host)

        await self.async_upsert_sql(pool)

        await pool.close()
    
    def upsert_async(self):
        asyncio.run(self.main())
        return True
    

class Views:

    def __init__(self):
        self.connect = Database('', []).connection

    def create_mat_view(self):
        view_name = self.view
        query = f'''CREATE MATERIALIZED VIEW IF NOT EXISTS {self.view} AS 
                    {self.query()};'''

        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()

        return True
    
    def update_full_view(self):
        query = f'''REFRESH MATERIALIZED VIEW 
                    {self.view} WITH DATA;'''
        
        with self.connect() as conn:
            cur  = conn.cursor()
            cur.execute(query)
            conn.commit()

        return True
    
    
    def update_sequence(self):
        self.create_mat_view()
        self.update_full_view()
        return True
