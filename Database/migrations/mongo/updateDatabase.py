
import configparser
import subprocess
import os
import shutil
from pymongo import MongoClient
import datetime


class updateDatabase():

    def __init__(self):
        ##setting up variables
        self.config=configparser.ConfigParser()
        self.config.read('.env')
        self.server_url=self.config['DEFAULT']['remote_mongo']
        self.local_url=self.config['DEFAULT']['local_mongo']
        self.database_name=self.config['DEFAULT']['database_name']
        self.local_client=MongoClient(self.local_url)[self.database_name]
        self.server_client=MongoClient(self.server_url)[self.database_name]
        self.remote_collections=self.server_client.list_collection_names(include_system_collections=False)
        self.versionControl=self.local_client.version_control


    def versionControlSeeder(self):
        insertArray=[]
        for collections in self.remote_collections:
            data={
                    "name":collections,
                    "lastUpdateAt":'',
                    'registerCount':0
            }
            insertArray.append(data)
        
        print("seeding version control")
        self.versionControl.insert_many(insertArray)

    def forceUpdate(self):
        self.versionControlSeeder()

    def getVersionControl(self):

        candidates=[]

        if(self.versionControl.estimated_document_count() == 0):
            self.versionControlSeeder()
        
        for collection in self.remote_collections:
            local_collection=self.versionControl.find_one({'name':collection})
            serverCount=self.server_client[collection].estimated_document_count()
            if(local_collection["registerCount"] < serverCount ):
                print('local collection {} is outdated'.format(collection))
                candidates.append(collection)

        self.remote_collections=candidates
           

    def updateVersionControl(self):

        for collection in self.remote_collections:
            myquery = { "name": collection }
            newvalues = { "$set": { "registerCount":self.local_client[collection].estimated_document_count(),
                                "lastUpdateAt": datetime.datetime.now()
            }}
        
            print("---------Updating Version Control for :{0}".format(collection))
            self.versionControl.update_one(myquery, newvalues)
 

    def getBackup(self):
        for collections in self.remote_collections:
            print("-------------Getting Collection: {0} ------------".format(collections))
            cmd="""mongodump --gzip --uri="{remoteMongo}"  --collection={collectionName} --forceTableScan""".format(remoteMongo=self.server_url,collectionName=collections)
            print(subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True))

    def restoreBackup(self,cleanFiles=False):
        for collections in self.remote_collections:
            print("-------------Deleting Local Collection: {0}--------------------".format(collections))
            self.local_client.drop_collection(collections)
        
        print('-----------Executing MongoRestore---------------------')
        cmd="mongorestore --gzip  dump/{database_name}/ --uri={local_url} --db={database_name}".format(database_name=self.database_name,local_url=self.local_url)
        subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
        if(cleanFiles):
            print("-----------cleaning local backup files------------")
            cmd="rm -rf dump/".format(database_name=self.database_name)
            subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
            print("-----------finishing cleaning local backup files------------")

    def pipeline(self):
        self.getVersionControl()
        if(len(self.remote_collections)>0):
            print('your local database seems outdated')
            self.getBackup()
            self.restoreBackup(cleanFiles=True)
            self.updateVersionControl()
            print("Update Process seems to finish sucessfully, please check")
        else:
            print("nothing to do here, already update")

if __name__ == '__main__':
    update=updateDatabase()
    update.pipeline()