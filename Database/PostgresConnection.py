
#!/usr/bin/python3

import sys
import pandas
import psycopg2


class PostgresConnection():

    def __init__(self):
        self.conn=None
        self.cursor=None
        self.host='localhost'
        self.user='postgres'
        self.database='postgres'
        self.password=''
        self.psypg2_url=''
        self.port=5432
        
    def connect(self):
        if self.psypg2_url.strip()=='':
            try:
                self.conn=psycopg2.connect("dbname='{db}' user='{usr}' host='{ip}' password='{passwd}' port='{port}'"
                .format(db=self.database,usr=self.user,ip=self.host,passwd=self.password,port=self.port))
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

    #return rows affected
    def executeCommand(self,query):
        if self.cursor != None:
            self.cursor.execute(query)
            rows_deleted = self.cursor.rowcount
            self.conn.commit()
            return rows_deleted
        else:
            print('Connection not established')
            return []

    #return results from a query
    def executeQuery(self,query):
        if self.cursor != None:
            self.cursor.execute(query)
            results=self.cursor.fetchall()
            return results
        else:
            print('Connection not established')
            return []


    def executeQueryToDic(self,query):
        if self.cursor != None:
            try:
                self.cursor.execute(query)
                results=self.cursor.fetchall()
                return dict(results)
            except ValueError:
                print('could not convert to dict probably many columns on operation')
                return {}
        else:
            print('Connection not established')
            return []


    #transform a pandas object 
    def queryToPandas(self,query):
        results=self.executeQuery(query)
        column_names = [desc[0] for desc in self.cursor.description]
        try:
            df = pandas.DataFrame(results,columns=column_names)
            return df
        except:
            print('failed on create dataframe operation')
            sys.exit()


    #close a connection
    def closeConnection(self):
        self.cursor.close()
        self.conn.close()
    
