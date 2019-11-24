#!/usr/bin/python3
import os
import sys
from pymongo import MongoClient, errors

class Mongo:
    def __init__(self, connectionString, databaseName, collectionName):
        self.connectionString = connectionString
        self.databaseName = databaseName
        self.collectionName = collectionName

    def checkValidDbAndConnectionName(mongoDbName, mongoCollectionName):
        invalidCharDbList = ['/',"\\",'.',' ','"','$']

        if not ((mongoDbName[0] == '"') and (mongoDbName[-1] == '"')):
            sys.exit('Database name must be wrapped in 2 levels of quotes, \'"<DB NAME>"\', \
                      \nthe quotes will be removed automatically, program terminating')
        else:
            mongoDbName = mongoDbName[1:-1]
        
        if not ((mongoCollectionName[0] == '"') and (mongoCollectionName[-1] == '"')):
            sys.exit('Collection name must be wrapped in 2 levels of quotes, \'"<COLLECTION NAME>"\', \
                      \nthe quotes will be removed automatically, program terminating')
        else:
            mongoCollectionName = mongoCollectionName[1:-1]

        if len(mongoDbName) >= 64:
            sys.exit('Length of database name is over 63 characters, program terminating')

        for invalidChar in invalidCharDbList:
            if invalidChar in mongoDbName:
                sys.exit("Invalid char: '" + invalidChar + "' in database name, program terminating")

        if '$' in mongoCollectionName:
            sys.exit("Invalid char: '$' in collection name, program terminating")

        if mongoCollectionName == '':
            sys.exit('Collection name cannot be empty string, program terminating')
        if mongoDbName == '':
            sys.exit('Database name cannot be empty string, program terminating') 
    
        if '.' in mongoCollectionName:
            if mongoCollectionName.split('.')[0] == 'system':
                sys.exit("Collection name cannot start with 'system.', program terminating")

        if len(mongoDbName) + len(mongoCollectionName) > 119:
            sys.exit("Max size of collection namespace '<db name>.<collection name>' is 120 bytes, program terminating")

        return mongoDbName, mongoCollectionName

    def askUserForConnectionString():
        userAnswer = ''
        connectionString = ''

        print('Program by default connects to Mongo Database @ localhost, port 27017')

        while True:
            print('Would you like to change this? (Y/N)')
            userAnswer = input()

            if userAnswer == 'Y' or userAnswer == 'N':
                break
    
        if userAnswer == 'N':
            return None
        
        print('Please insert the connection string into a text file and provide the file name below: ')
        connectionStringFileName = input()
    
        if not os.path.isfile(connectionStringFileName):
            sys.exit('Could not open file containing connection string, program terminating')    
        
        with open(connectionStringFileName) as openFile:
            connectionString = openFile.read()

        return connectionString

    def checkForValidConnection(userDefinedConnectionString):
        timeOutVal = 1
        connectionString = ''

        if userDefinedConnectionString != None:
            connectionString = 'mongodb://localhost:27017'
        else:
            connectionString = userDefinedConnectionString

        client = MongoClient(connectionString, serverSelectionTimeoutMS = timeOutVal)
        db= client.admin

        try:
            client.server_info()
        except errors.ServerSelectionTimeoutError:
            sys.exit('Could not establish connection with Mongo Database, program terminating')

    def writeToDatabase(self, compressedStream, tag):
        dbName = self.databaseName
        dataTagDict = {'Test': 1}
        client = MongoClient(self.connectionString) 

        dbString = 'db = client.' + dbName
        exec(dbString)
        
        collectionString = 'result = db.' + self.collectionName + '.insert_one(dataTagDict)'
        exec(collectionString)
