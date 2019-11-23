#!/usr/bin/python3
import sys
from pymongo import MongoClient

class Mongo:
    def __init__(self, connectionString, databaseName, collectionName):
        self.connectionString = connectionString
        self.databaseName = databaseName
        self.collectionName = collectionName

    def askUserForConnectionString(self):
        userAnswer = ''
        connectionString = ''

        print('Program by default connects to Mongo Database @ localhost, port 27017')

        while True:
            print('Would you like to change this? (Y/N)')
            userAnswer = input()

            if userAnswer == 'Y' or userAnswer == 'N':
                break
    
        if userAnswer == 'N':
            return (False, None)
        
        print('Please insert the connection string into a text file and provide the file name below: ')
        connectionStringFileName = input()
    
        if not os.path.isfile(connectionStringFileName):
            sys.exit('Could not open file containing connection string, program terminating')    
        
        with open(connectionStringFileName) as openFile:
            connectionString = openFile.read()

        return (True, connectionString)

    def checkForValidConnection(self, userDefinedConnectionString):
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
        except pymongo.errors.ServerSelectionTimeoutError:
            sys.exit('Could not establish connection with Mongo Database, program terminating')

    def writeToDatabase(self):
        
