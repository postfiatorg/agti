import pandas as pd
import sqlalchemy
import datetime
from utilities import settings as gset
import numpy as np
import psycopg2
import pandas as pd
import numpy as np
import os

class DBConnectionManager:
    ''' supports 1 database for the collective and one for the user'''
    def __init__(self, pw_map):
        self.pw_map = pw_map
        
    def spawn_sqlalchemy_db_connection_for_user(self, user_name):
        db_connstring = self.pw_map[f'{user_name}__postgresconnstring']
        engine = sqlalchemy.create_engine(db_connstring)
        return engine
    
    def list_sqlalchemy_db_table_names_for_user(self, user_name):
        engine = self.spawn_sqlalchemy_db_connection_for_user(user_name)
        table_names = sqlalchemy.inspect(engine).get_table_names()
        return table_names
    
    def spawn_psycopg2_db_connection(self,user_name):
        
        db_connstring = self.pw_map[f'{user_name}__postgresconnstring']

        db_user = db_connstring.split('://')[1].split(':')[0]
        db_password = db_connstring.split('://')[1].split(':')[1].split('@')[0]
        db_host = db_connstring.split('://')[1].split(':')[1].split('@')[1].split('/')[0]
        db_name = db_connstring.split('/')[-1:][0]
        psycop_conn = psycopg2.connect(user=db_user, password=db_password, 
                                            host=db_host, database=db_name)
        return psycop_conn