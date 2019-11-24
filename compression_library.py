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

        self.dataChunks = []
    
    def iterateCsvBreakUpOnAttributes(self):
        dataChunks = {}

        with open(self.fileName) as openFile:
            reader = csv.reader(openFile, delimiter = ',')
        
            positionList = Table.findAttributeInHeaderRow(next(reader), self.columnsToBreakUpOn)
        
            for dataRow in reader:
                tupleTag = Table.getChunkTuple(dataRow, positionList)
                if tupleTag in dataChunks:
                    dataChunks[tupleTag].append(dataRow)
                else:
                    dataChunks[tupleTag] = [dataRow]
            
        self.dataChunks = dataChunks

    def compressChunksInParallel(self):
        self.dataChunks = [(tag, data, self.whichCompressionAlgToUse, self.mongoObject) for tag, data in self.dataChunks.items()]
    
        cpuCores = multiprocessing.cpu_count()

        pool = multiprocessing.Pool(cpuCores + 1)
        pool.map(Parallel.compressionParallelized, self.dataChunks)

    def getCsvChunkGenerator(self):
         dataChunks = {}

        with open(self.fileName) as openFile:
            reader = csv.reader(openFile, delimiter = ',')
        
            positionList = Table.findAttributeInHeaderRow(next(reader), self.columnsToBreakUpOn)
        
            for dataRow in reader:
                tupleTag = Table.getChunkTuple(dataRow, positionList)
                if tupleTag in dataChunks:
                    dataChunks[tupleTag].append(dataRow)
                else:
                    dataChunks[tupleTag] = [dataRow]

                if len(dataChunks[tupleTag] >= self.memoryCap:
                    yield (tupleTag, dataChunks[tupleTag], self.whichCompressionAlgToUse, self.mongoObject) 
                    del dataChunks[tupleTag]
                    gc.collect()
                    
    def breakUpCsvAndCompressChunksMemorySensative():
        cpuCores = multiprocessing.cpu_count()

        pool = multiprocessing.Pool(cpuCores + 1)
        list(pool.imap(Parallel.compressionParallelized, getCsvChunkGenerator()))


class Table(Compression):
    def checkValidFieldNames(columnsToBreakUpOn):
        fieldListWithRemovedQuotes = []

        for field in columnsToBreakUpOn:
            if not ((field[0] == '"') and (field[-1] == '"')):
                sys.exit('Each field name must be wrapped in 2 levels of quotes, \'"<FIELD NAME>"\' , \
                          \nthe quotes will be removed automatically, they do not have to be present in the CSV header row, \
                          \nprogram terminating')
            else:
                field = field[1:-1]
                fieldListWithRemovedQuotes.append(field)

            if len(field.split('\x00')) > 1:
                sys.exit('Fields cannot contain NULL character, program terminating')
            
            if field[0] == '$':
                sys.exit("Fields cannot start with '$', program terminating")

            return fieldListWithRemovedQuotes
    
    def findAttributeInHeaderRow(headerRow, columnsToBreakUpOn):
        positionList = []
        headerRowDict = {}
        counter = 0
    
        for column in headerRow:
            headerRowDict[column] = counter
            counter += 1

        for column in columnsToBreakUpOn:
            try: 
                positionList.append(headerRowDict[column])
            except:
                sys.exit("Attribute: '" + column + "' not found in header row, program terminating")

        return positionList
    
    def getChunkTuple(dataRow, positionList):
        labelList = []

        for columnPosition in positionList:
            labelList.append(dataRow[columnPosition])

        return tuple(labelList)


class Parallel():
    def compressionParallelized(chunk):
        compressedStream = ''
        tag = chunk[0]
        algToUse = chunk[2]
        dataToCompress = str(chunk[1]).encode("utf-8")
        mongoConnectionObject = chunk[3]
       
        if algToUse == 'bzip2':
            compressedStream = bz2.compress(dataToCompress)
        elif algToUse == 'gzip':
            compressedStream = gzip.compress(dataToCompress)
        elif algToUse == 'xz':
            compressedStream = lzma.compress(dataToCompress)
        else:
            compressedStream = zlib.compress(dataToCompress)

        mongoConnectionObject.writeToDatabase(compressedStream, tag)
