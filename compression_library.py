#!/usr/bin/python3
import gc
import sys
import csv
import bz2
import gzip
import lzma
import zlib
import multiprocessing 

class Compression:
    def __init__(self, inputFileName, columnsToBreakUpOn, whichCompressionAlgToUse, mongoObject):
        self.fileName = inputFileName
        self.columnsToBreakUpOn = columnsToBreakUpOn
        self.whichCompressionAlgToUse = whichCompressionAlgToUse
        self.mongoObject = mongoObject

        self.memoryCap = None
        self.dataChunks = []
   
    #Function breaks up a csv file, on a given set of columns, into different segments
    #This function runs in the default version of Smart Compress, with no 'memory' flag, loads entire CSV into memory 
    def iterateCsvBreakUpOnAttributes(self):
        dataChunks = {}
        
        #Opening CSV file, using generator
        with open(self.fileName) as openFile:
            reader = csv.reader(openFile, delimiter = ',')
            
            #Finding column positions of the fields to segment on in the header row of the CSV
            positionList = Table.findAttributeInHeaderRow(next(reader), self.columnsToBreakUpOn)
            print('Breaking up CSV into segments, based on field/s:', self.columnsToBreakUpOn)
        
            #For every row, use known column positions to create segments of different values at those positions
            for dataRow in reader:
                tupleTag = Table.getChunkTuple(dataRow, positionList)
                #If the segment with that 'tag' already exists, add to it
                if tupleTag in dataChunks:
                    dataChunks[tupleTag].append(dataRow)
                #Else, create the segment
                else:
                    dataChunks[tupleTag] = [dataRow]
            
        self.dataChunks = dataChunks

    #Function launches multiprocessing Pool to compress and write to a Mongo Database instance in parallel
    #This function runs in the default version of Smart Compress, with no 'memory' flag, it takes a full list already in memory
    def compressChunksInParallel(self):
        print("Compressing segments in parallel using '" + self.whichCompressionAlgToUse + "' compression algorithm")
        self.dataChunks = [(tag, data, self.whichCompressionAlgToUse, self.mongoObject, self.columnsToBreakUpOn) for tag, data in self.dataChunks.items()]
    
        cpuCores = multiprocessing.cpu_count()

        #Pool is created on # of system cores + 1 for performance
        pool = multiprocessing.Pool(cpuCores + 1)
        pool.map(Parallel.compressionParallelized, self.dataChunks)

    #Function breaks up a csv file, on a given set of columns, into different segments
    #This function runs when the 'memory' flag is set, each segment has a max size, when this size is reached
    #it is yielded out of the function into a Pool, and the segment, and its memory is deleted from Smart Compress
    #This ensures that thrashing in memory will not occur for very large CSV files
    def getCsvChunkGenerator(self):
        dataChunks = {}
        firstYield = 1

        #Opening CSV file, using generator
        with open(self.fileName) as openFile:
            reader = csv.reader(openFile, delimiter = ',')
        
            #Finding column positions of the fields to segment on in the header row of the CSV
            positionList = Table.findAttributeInHeaderRow(next(reader), self.columnsToBreakUpOn)
            print("Breaking up CSV into segments of max length '" + str(self.memoryCap) + "', based on field/s:", self.columnsToBreakUpOn)
        
            #For every row, use known column positions to create segments of different values at those positions
            for dataRow in reader:
                tupleTag = Table.getChunkTuple(dataRow, positionList)
                #If the segment with the 'tag' already exists, add to it
                if tupleTag in dataChunks:
                    dataChunks[tupleTag].append(dataRow)
                #Else, create the segment
                else:
                    dataChunks[tupleTag] = [dataRow]
                
                #If the segment just added to has reached the maximum length
                if len(dataChunks[tupleTag]) >= self.memoryCap:
                    #Yield it into the Pool to be compressed and written to Mongo Db
                    yield (tupleTag, dataChunks[tupleTag], self.whichCompressionAlgToUse, self.mongoObject, self.columnsToBreakUpOn) 
                    #Delete it from Smart Compress's memory
                    del dataChunks[tupleTag]
                    gc.collect()
                
                if firstYield == 1:
                    firstYield = 0
                    print("Compressing segments in parallel once max length of '" + str(self.memoryCap) + \
                          "' is reached, using '" + self.whichCompressionAlgToUse + "' compression algorithm")
                    
    #Function launches multiprocessing Pool to compress and write to a Mongo Database instance in parallel
    #This function runs when the 'memory' flag is set, it uses a generator to pass into the Pool the moment a segment is ready
    #so the entire iterable for the Pool does not have to be in memory
    def breakUpCsvAndCompressChunksMemorySensative(self):
        cpuCores = multiprocessing.cpu_count()

        #Pool is created on # of system cores + 1 for performance
        pool = multiprocessing.Pool(cpuCores + 1)
        list(pool.imap(Parallel.compressionParallelized, self.getCsvChunkGenerator()))


class Table(Compression):
    #Function checks field names to make sure they are valid
    def checkValidFieldNames(columnsToBreakUpOn):
        fieldListWithRemovedQuotes = []

        #Field names have to be wrapped in two levels of quotes, single quotes, and inside them, double quotes
        #This is done in an attempt to sanitize the data, though this can still be overcome by using 4 levels of quotes
        for field in columnsToBreakUpOn:
            if not ((field[0] == '"') and (field[-1] == '"')):
                sys.exit('Each field name must be wrapped in 2 levels of quotes, \'"<FIELD NAME>"\' \
                          \nor \'"<FIELD NAME=VALUE>"\' , the quotes will be removed automatically, \
                          \nfor compression, they do not have to be present in the CSV header row, \
                          \nprogram terminating')
            #Stripping away double quotes for each field name, single quotes are eaten by bash automatically
            else:
                field = field[1:-1]
                fieldListWithRemovedQuotes.append(field)

            #Making sure NULL character is not present in any field name, NULL characters are invalid in Mongo Db
            if len(field.split('\x00')) > 1:
                sys.exit('Fields, and, for decompression, values, cannot contain NULL character, program terminating')
            
            #Field names cannot start with '$' in Mongo Db
            if field[0] == '$':
                sys.exit("Fields cannot start with '$', program terminating")

            return fieldListWithRemovedQuotes
    
    #Function searches for the position of fields to segment on in the header row of the CSV
    def findAttributeInHeaderRow(headerRow, columnsToBreakUpOn):
        positionList = []
        headerRowDict = {}
        counter = 0
    
        #Turning header row list into a dictionary, key is field name, value is position
        for column in headerRow:
            headerRowDict[column] = counter
            counter += 1

        #Searching for field name in dictionary
        for column in columnsToBreakUpOn:
            #If found, position (value) is added
            try: 
                positionList.append(headerRowDict[column])
            #Else, program terminates
            except:
                sys.exit("Attribute: '" + column + "' not found in header row, program terminating")

        return positionList
    
    #Function returns tuple of values at certain positions in a row, corresponds to fields to segment on
    def getChunkTuple(dataRow, positionList):
        labelList = []

        for columnPosition in positionList:
            labelList.append(dataRow[columnPosition])

        return tuple(labelList)


class Parallel():
    #Function compresses segment
    def compressionParallelized(chunk):
        compressedStream = ''
        #Unpacking tuple, passed in through Pool iterator
        tag = chunk[0]
        algToUse = chunk[2]
        #Converting list to string and then to byte array
        dataToCompress = str(chunk[1]).encode("utf-8")
        mongoConnectionObject = chunk[3]
        mongoFieldNames = chunk[4]
       
        #Compressing according to user input
        if algToUse == 'bzip2':
            compressedStream = bz2.compress(dataToCompress)
        elif algToUse == 'gzip':
            compressedStream = gzip.compress(dataToCompress)
        elif algToUse == 'xz':
            compressedStream = lzma.compress(dataToCompress)
        else:
            compressedStream = zlib.compress(dataToCompress)

        #Writing compressed object, and some other metadata to Mongo Db instance
        mongoConnectionObject.writeToDatabase(compressedStream, tag, mongoFieldNames)

    #Function decompresses a segment
    def decompressionParallelized(chunk):
        #Unpacking tuple, passed in through Pool iterator
        sharedList = chunk[1]
        dataToDecompress = chunk[0]
        algToUse = chunk[2]
        tag = chunk[3]

        print('Decompressing segment:', tag)

        #Decompressing according to user input
        if algToUse == 'bzip2':
            compressedStream = bz2.decompress(dataToDecompress)
        elif algToUse == 'gzip':
            compressedStream = gzip.decompress(dataToDecompress)
        elif algToUse == 'xz':
            compressedStream = lzma.decompress(dataToDecompress)
        else:
            compressedStream = zlib.decompress(dataToDecompress)

        #Passing decompressed string to multiprocessing shared list
        #Write function in thread of Smart Compress has access to this list
        sharedList.append((compressedStream, tag))
