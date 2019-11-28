#!/usr/bin/python3
import os
import sys
from pymongo import MongoClient, errors

class Mongo:
    def __init__(self, connectionString, databaseName, collectionName):
        self.connectionString = connectionString
        self.databaseName = databaseName
        self.collectionName = collectionName

    #Function ensures that user defined database and collection names are valid
    def checkValidDbAndConnectionName(mongoDbName, mongoCollectionName):
        #Illegal characters in a Mongo database name
        invalidCharDbList = ['/',"\\",'.',' ','"','$']

        #Database name has to be wrapped in two levels of quotes, single quotes, and inside them, double quotes
        #This is done in an attempt to sanitize the data, though this can still be overcome by using 4 levels of quotes
        if not ((mongoDbName[0] == '"') and (mongoDbName[-1] == '"')):
            sys.exit('Database name must be wrapped in 2 levels of quotes, \'"<DB NAME>"\', \
                      \nthe quotes will be removed automatically, program terminating')
        else:
            #Stripping away double quotes, single quotes are eaten by bash automatically
            mongoDbName = mongoDbName[1:-1]
    
        #Python 'exec' function will be used later, pass cannot be present in this function
        if (mongoDbName == 'pass') or (mongoCollectionName == 'pass'):
            sys.exit("Database name or collection name cannot be 'pass', program terminating")
        
        #Collection name has to be wrapped in two levels of quotes, single quotes, and inside them, double quotes
        #This is done in an attempt to sanitize the data, though this can still be overcome by using 4 levels of quotes
        if not ((mongoCollectionName[0] == '"') and (mongoCollectionName[-1] == '"')):
            sys.exit('Collection name must be wrapped in 2 levels of quotes, \'"<COLLECTION NAME>"\', \
                      \nthe quotes will be removed automatically, program terminating')
        else:
            #Stripping away double quotes, single quotes are eaten by bash automatically
            mongoCollectionName = mongoCollectionName[1:-1]

        #Mongo Database will not allow database names to be over 63 characters
        if len(mongoDbName) >= 64:
            sys.exit('Length of database name is over 63 characters, program terminating')

        #Making sure any character from list above ^, is not present
        for invalidChar in invalidCharDbList:
            if invalidChar in mongoDbName:
                sys.exit("Invalid char: '" + invalidChar + "' in database name, program terminating")

        #Only illegal character for collections is '$'
        if '$' in mongoCollectionName:
            sys.exit("Invalid char: '$' in collection name, program terminating")
        
        #Both collection and database names cannot be empty string
        if mongoCollectionName == '':
            sys.exit('Collection name cannot be empty string, program terminating')
        if mongoDbName == '':
            sys.exit('Database name cannot be empty string, program terminating') 
    
        #Collection name cannot start with 'system.'
        if '.' in mongoCollectionName:
            if mongoCollectionName.split('.')[0] == 'system':
                sys.exit("Collection name cannot start with 'system.', program terminating")

        #Total length of collection namespace has to be below 120 bytes, so 119 characters (there is a mandatory dot)
        if len(mongoDbName) + len(mongoCollectionName) > 119:
            sys.exit("Max size of collection namespace '<db name>.<collection name>' is 120 bytes, program terminating")

        return mongoDbName, mongoCollectionName

    #Function checks to see if the user wants to use default connection string or supply their own in a file
    def askUserForConnectionString():
        userAnswer = ''
        connectionString = ''

        print('Program by default connects to Mongo Database @ localhost, port 27017')

        #Will only take 'Y' or 'N' as a response
        while True:
            print('Would you like to change this? (Y/N)')
            userAnswer = input()

            if userAnswer == 'Y' or userAnswer == 'N':
                break
    
        if userAnswer == 'N':
            return None
        
        #If user does want to modify connection string:
        #User enters file name that contains connection string into stdin
        print('Please insert the connection string into a text file and provide the file name below: ')
        connectionStringFileName = input()
    
        #Seeing if file actually exists
        if not os.path.isfile(connectionStringFileName):
            sys.exit('Could not open file containing connection string, program terminating')    
        
        #If it does, opening it, entire contents become connection string
        #If file is correct, it should only be one line long
        with open(connectionStringFileName) as openFile:
            connectionString = openFile.read()

        return connectionString

    #Function checks to see if a connection can be established to a Mongo Database instance
    def checkForValidConnection(userDefinedConnectionString):
        timeOutVal = 1
        connectionString = ''

        #If the user opted to use the default string, with connects to localhost at port 27017
        if userDefinedConnectionString != None:
            connectionString = 'mongodb://localhost:27017'
        #If they provided their own string
        else:
            connectionString = userDefinedConnectionString

        #Modifying timeout, default is 30 seconds, now it is 1 microsecond
        client = MongoClient(connectionString, serverSelectionTimeoutMS = timeOutVal)
        db= client.admin

        #Seeing if server responds, or if timeout error occurs
        try:
            client.server_info()
            print("Established connection to Mongo Database")
        except errors.ServerSelectionTimeoutError:
            sys.exit('Could not establish connection with Mongo Database, program terminating')

    #Function writes compressed string, and lookup information into Mongo Database
    def writeToDatabase(self, compressedStream, tag, mongoFieldNames):
        #mongoFieldNames are the field (column) names defined by the user to break up the CSV on
        #tag is the set of actual values for each field that make up this segment
        #Zipping them together to create dictionary (JSON), allows for lookup in Mongo Db
        dataTagDict = dict(zip(mongoFieldNames, tag))
        #Adding a new field to dictionary for our compressed object
        dataTagDict['compressedObject'] = compressedStream

        #Reconnecting to Mongo Database
        client = MongoClient(self.connectionString) 

        #Has to be done in exec, because code doesn't allow for dynamic database names
        dbString = 'db = client.' + self.databaseName
        exec(dbString)
        
        #Has to be done in exec, because code doesn't allow for dynamic collection names
        collectionString = 'result = db.' + self.collectionName + '.insert_one(dataTagDict)'
        exec(collectionString)

        print('Segment successfully written to Mongo Database:', list(tag))

    #Function retrieves compressed string from Mongo Databased based on lookup information
    def retrieveSegmentsFromDatabase(self, columnsToReconstructOn):
        queryDict = {}
        queryResult = None

        #Reconnecting into Mongo Database
        client = MongoClient(self.connectionString)

        #Since argument is in format FIELD=VALUE, splitting on '='
        for fieldValuePair in columnsToReconstructOn:
            brockenUp = fieldValuePair.split('=')
            #User forgot to include '='
            if len(brockenUp) == 1:
                sys.exit('Field name for decompression must be in format FIELD=VALUE, no equal sign detected, program terminating')
            #'FIELD=VALUE' becomes {FIELD: VAlUE} entries in a dictionary
            else:
                queryDict[brockenUp[0]] = brockenUp[1]

        #Has to be done in exec, because code doesn't allow for dynamic database names
        dbString = 'db = client.' + self.databaseName
        exec(dbString)
        
        #Has to be done in exec, because code doesn't allow for dynamic collection names
        _locals = locals()
        queryString = 'queryResult = db.' + self.collectionName + '.find(' + str(queryDict) + ')'
        exec(queryString, globals(), _locals)

        queryResult = list(_locals['queryResult'])

        #Query returned nothing
        if len(queryResult) == 0:
            sys.exit('Successfully queried input, 0 results, program terminating')
        #Query returned something
        else:
            print('Successfully retrieved query results on:', self.collectionName + ', from Mongo Database:', self.databaseName)

        return queryResult
