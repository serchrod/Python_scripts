
#!/usr/bin/python3

import sys
import pandas
import psycopg2



#connection class for postgres or amazon redshift

class PostgresConnection():

    def __init__(self):
        self.conn=None
        self.cursor=None
        self.host='localhost'
        self.user='postgres'
        self.database=''
        self.isredshift=False
        self.password=''
        self.psypg2_url=''

        if self.isredshift:
            self.port=5439
        else:
            self.port=5432
        
    def connect(self):
        if self.psypg2_url.strip()=='':
            try:
                self.conn=psycopg2.connect("dbname='{db}' user='{usr}' host='{ip}' password='{passwd}'"
                .format(db=self.database,usr=self.user,ip=self.host,passwd=self.password))
                self.cursor = self.conn.cursor()

            except(psycopg2.OperationalError):
                print("could not connect")
                sys.exit(0)
        else:
            try:
                self.conn=psycopg2.connect(self.psypg2_url)
                self.cursor=self.conn.cursor()
            except(psycopg2.OperationalError):
                print("could not connect")
                sys.exit(0)

    def executeQuery(self,query):
        if self.cursor != None:
            self.cursor.execute(query)
            results=self.cursor.fetchall()
            

            return results
        else:
            print('Connection not established')
            return []

    def queryToPandas(self,query):
        results=self.executeQuery(query)
        column_names = [desc[0] for desc in self.cursor.description]
        try:
            df = pandas.DataFrame(results,columns=column_names)
            return df
        except:
            print('failed on create dataframe operation')
            sys.exit()

    def closeConnection(self):
        self.conn.close()
    
