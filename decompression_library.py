#!/usr/bin/python3
import ast
import sys
import csv
import threading
import multiprocessing
from compression_library import Parallel

class Decompression:
    def __init__(self, outputFileName, columnsToReconstructOn, whichCompressionAlgToUse):
        self.fileName = outputFileName
        self.columnsToReconstructOn = columnsToReconstructOn
        self.whichCompressionAlgToUse = whichCompressionAlgToUse

        self.segments = None

    def checkValidOutputFileName(self):
        if not ((self.fileName[0] == '"') and (self.fileName[-1] == '"')):
            sys.exit('Output file name must be wrapped in 2 levels of quotes, \'"<FILE NAME>"\', \
                      \nthe quotes will be removed automatically, program terminating')
        else:
            self.fileName = self.fileName[1:-1]

        if self.fileName[0].isdigit() == True:
            sys.exit('Output filename cannot start with a digit, program terminating')
        
        for char in self.fileName:
            if char == '/':
                sys.exit("Output filename contains illegal character '/', program terminating")

        if len(self.fileName.split('\x00')) > 1:
            sys.exit("Output filename contains NULL character, program terminating")

    def getTagFromSubsegment(self, subsegment):
        tagString = '['

        for key, value in subsegment.items():
            if not ((str(key) == 'compressedObject') or (str(key) == '_id')):
                tagString += str(key) + ': ' + str(value) + ', '

        tagString = tagString[:-2] + ']'
        return tagString
    
    def decompressAndCombineInParallel(self): 
        sharedList = multiprocessing.Manager().list()
    
        compressedList = [(subsegment['compressedObject'], sharedList, self.whichCompressionAlgToUse, self.getTagFromSubsegment(subsegment)) for subsegment in self.segments]
       
        cpuCores = multiprocessing.cpu_count()
        poolDecompress = multiprocessing.Pool(cpuCores + 1)
        writeThread = threading.Thread(target=self.writeOutputToCsv, args=(sharedList,))
    
        print("Decompressing in parallel using '" + self.whichCompressionAlgToUse + "' algorithm") 
     
        writeThread.start()
        poolDecompress.map(Parallel.decompressionParallelized, compressedList)
        sharedList.append('KILL')
    
    def writeOutputToCsv(self, uncompressedList):
        with open(self.fileName, 'w') as openOutputFile:
            writer = csv.writer(openOutputFile, delimiter = ',')
    
            print("Writing uncompressed output to file '" + self.fileName + "' in parallel with decompression")

            while True:
                if len(uncompressedList) != 0:
                    if uncompressedList[0] == 'KILL':
                        return
                    else:
                        print('Writing segment', uncompressedList[0][1], "to file '" + self.fileName + "'")
                        csvRows = ast.literal_eval((uncompressedList[0][0].decode("utf-8")))
                        for row in csvRows:
                            writer.writerow(row)
                        uncompressedList.pop(0)
