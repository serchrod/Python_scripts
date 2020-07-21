from impala.dbapi import connect
from impala.util import as_pandas
import csv

#install using pip not pip3
#dependecies
#thrift,bit_array,six, could you install using pip


class ImpalaConnection():

    def __init__(self):
        self.conn=None
        self.cursor=None
        self.host=''
        self.port=21050
    
    def connect(self):
        self.conn = connect(host=self.host, port=self.port)
        self.cursor=self.conn.cursor()
 
    def query(self,query):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        for row in results:
            print(row)
        
        self.cursor.close()

    def queryToCsv(self,query,filename):
        
        self.cursor.execute(query)
        columns = [datum[0] for datum in self.cursor.description]
        targetfile = filename
        with open(targetfile, 'w', newline='') as outcsv:
            writer = csv.writer(outcsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
            writer.writerow(columns)
            for row in self.cursor:
                writer.writerow(row)

        self.cursor.close()
        

    def queryToDataframe(self,query):
        self.cursor.execute(query)
        df = as_pandas(self.cursor)
        self.cursor.close()
        return df

    def closeConnection(self):
        self.conn.close()
 
    
if __name__ == "__main__":
    new=ImpalaConnection()
    new.host=''
    new.connect()
    df=new.query("select count(*) from table_impala")
    
