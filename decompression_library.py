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

    #Function checks output file name to see if it is valid
    def checkValidOutputFileName(self):
        #Output file name has to be wrapped in two levels of quotes, single quotes, and inside them, double quotes
        #This is done in an attempt to sanitize the data, though this can still be overcome by using 4 levels of quotes
        if not ((self.fileName[0] == '"') and (self.fileName[-1] == '"')):
            sys.exit('Output file name must be wrapped in 2 levels of quotes, \'"<FILE NAME>"\', \
                      \nthe quotes will be removed automatically, program terminating')
        
        #Stripping away double quotes, single quotes are eaten by bash automatically
        else:
            self.fileName = self.fileName[1:-1]
        
        #Making sure file name does not start with a number
        if self.fileName[0].isdigit() == True:
            sys.exit('Output filename cannot start with a digit, program terminating')
        
        #Making sure file name does not contain '/' - illegal character in linux
        for char in self.fileName:
            if char == '/':
                sys.exit("Output filename contains illegal character '/', program terminating")

        #Making sure file name does not contain NULL character in python
        if len(self.fileName.split('\x00')) > 1:
            sys.exit("Output filename contains NULL character, program terminating")

    #For each segment returned by the query, function finds idenfifying field names
    def getTagFromSubsegment(self, subsegment):
        tagString = '['

        #Ignoring '_id' and 'compressedObject' since every segment has them, not unique
        for key, value in subsegment.items():
            if not ((str(key) == 'compressedObject') or (str(key) == '_id')):
                tagString += str(key) + ': ' + str(value) + ', '

        #Getting rid of last comma
        tagString = tagString[:-2] + ']'
        return tagString
    
    #Function launches multiprocessing Pool to decompress in parallel and a thread to write in parallel
    def decompressAndCombineInParallel(self): 
        #Shared list allows decompressed results to come back into Smart Compress from child processes
        sharedList = multiprocessing.Manager().list()
    
        #For all the segments returned by the query, making tuple of info for each one
        #Each tuple will be passed into the Pool
        compressedList = [(subsegment['compressedObject'], sharedList, self.whichCompressionAlgToUse, \
                          self.getTagFromSubsegment(subsegment)) for subsegment in self.segments]
       
        cpuCores = multiprocessing.cpu_count()
        #Pool is created on # of system cores + 1 for performance
        poolDecompress = multiprocessing.Pool(cpuCores + 1)
        #Shared list is passed into thread for writing, as decompressed results come available, they are written
        writeThread = threading.Thread(target=self.writeOutputToCsv, args=(sharedList,))
    
        print("Decompressing in parallel using '" + self.whichCompressionAlgToUse + "' algorithm") 
     
        writeThread.start()
        poolDecompress.map(Parallel.decompressionParallelized, compressedList)
        #'KILL' signal kills thread
        sharedList.append('KILL')
    
    #Function writes uncompressed segments to an output file
    def writeOutputToCsv(self, uncompressedList):
        #Opening output file, writing CSV
        with open(self.fileName, 'w') as openOutputFile:
            writer = csv.writer(openOutputFile, delimiter = ',')
    
            print("Writing uncompressed output to file '" + self.fileName + "' in parallel with decompression")

            #Will run until receives 'KILL' signal
            while True:
                #Decompression may take a while, so it only writes when decompressed objects are in queue
                if len(uncompressedList) != 0:
                    if uncompressedList[0] == 'KILL':
                        return
                    else:
                        print('Writing segment', uncompressedList[0][1], "to file '" + self.fileName + "'")
                        #Decoding byte array to string, converting to list of rows
                        csvRows = ast.literal_eval((uncompressedList[0][0].decode("utf-8")))
                        #For each row, printing in CSV format
                        for row in csvRows:
                            writer.writerow(row)
                        #Removing segment from shared list (shared queue)
                        uncompressedList.pop(0)
