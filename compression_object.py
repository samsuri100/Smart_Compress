#!/usr/bin/python3
import csv
import multiprocessing 

class CompressionObject:
    def __init__(self, fileName, columnsToBreakUpOn, whichCompressionAlgToUse, parallelOption):
        self.fileName = fileName
        self.columnsToBreakUpOn = columnsToBreakUpOn
        self.whichCompressionAlgToUse = whichCompressionAlgToUse
        self.parallelOption = parallelOption

    def iterateCsvBreakUp(self):
        with open(self.fileName) as openFile:
            reader = csv.reader(openFile, delimiter = ',')
            for row in reader:
                print(row)
