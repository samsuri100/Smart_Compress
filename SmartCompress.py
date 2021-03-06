#!/usr/bin/python3
import os
import sys
import argparse
from mongo_library import Mongo
from decompression_library import Decompression
from compression_library import Compression, Table

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog = 'SmartCompress', \
                                     allow_abbrev = False, \
                                     description = "Intelligently compress a CSV file! Breaks a CSV file into \
                                                    segments, on specified column attributes, compresses each \
                                                    segment, and stores each in a Mongo Database collection, in \
                                                    parallel, for quick retrieval")
    parser.add_argument('-v', '--version', \
                        nargs = 1, \
                        type = str, \
                        choices = ['compress', 'decompress'], \
                        required = True, \
                        help = "['compress' to break up input file, compress segments, store in Mongo Database] ~ \
                                ['decompress' to retieve segments from Mongo Database, decompress, output to file]")
    parser.add_argument('-i', '--input', '-o', '--output', \
                        nargs = 1, \
                        type = str, \
                        metavar = 'FILE-NAME', \
                        required = True, \
                        help = '[-v, --version = compress: input file to break up, compress, and store] ~ \
                                [-v, --version = decompress: output file name to store retrieved, uncompressed, combined file segments')
    parser.add_argument('-f', '--fields', \
                        nargs = '+', \
                        type = str, \
                        metavar = 'FIELD /or/ FIELD=VALUE', \
                        required = True, \
                        help = '[-v, --version = compress: columns attributes that the file will be broken on up, only provide FIELD names: FIELD FIELD...] ~ \
                                [-v, --version = decompress: columns attributes that the file segments will be retrieved and constructed on, provide FIELD \
                                names and lookup VALUE: FIELD=VALUE, FIELD=VALUE...]')
    parser.add_argument('-a', '--algorithm', \
                        nargs = 1, \
                        type = str, \
                        choices = ['gzip', 'bzip2', 'xz', 'zlib'], \
                        required = True, \
                        help = '[-v, --version = compress: compression algorithm to use] ~ \
                                [-v, --version = decompress: decompression algorithm to use]')
    parser.add_argument('-d', '--database', \
                        nargs = 1, \
                        type = str, \
                        required = True, \
                        help = '[-v, --version = compress: Mongo Database to write to] ~ \
                                [-v, --version = decompress: Mongo Database to retrieve from]')
    parser.add_argument('-c', '--collection', \
                        nargs = 1, \
                        type = str, \
                        required = True, \
                        help = '[-v, --version = compress: collection within Mongo Database to write to] ~ \
                                [-v, --version = decompress: collection within Mongo Database to retrieve from]')
    parser.add_argument('-m', '--memory', \
                        nargs = 1, \
                        type = int, \
                        required = False, \
                        help = '[-v, --version = compress: default program loads entire CSV into memory. If CSV \
                                is larger than memory, or there is a danger of thrashing, provide integer to  \
                                serve as a segment "cap". This will write to the database every time this cap \
                                is reached for a particular set of fields, and then flush the data out of memory. \
                                Value should be very large for Fields with many common elements, very small for \
                                Fields with only a few common elements.]')
    #Using argparse as command line argument parser
    args = parser.parse_args()
       
    fieldsToBreakOrReconstructOn = args.fields
    whichCompressionAlgToUse = args.algorithm[0]
    mongoDbName = args.database[0]
    mongoCollectionName = args.collection[0]
    
    #For compression version, checking to see of optional argument, memory, was set, None by default
    if args.memory is not None:
        memoryCap = args.memory[0]
    else:
        memoryCap = args.memory

    #Checking to make sure field name are valid
    fieldsToBreakOrReconstructOn = Table.checkValidFieldNames(fieldsToBreakOrReconstructOn)

    #Checking to see if Mongo database and collection names are valid, if so, removing quotes around them
    mongoDbName, mongoCollectionName = Mongo.checkValidDbAndConnectionName(mongoDbName, mongoCollectionName)
    #See if the user wants to keep default connection string (localhost at port 27017) or supply their own inside a file
    connectionString = Mongo.askUserForConnectionString()
    #Checks to see if a connection can be made based on the connection string
    Mongo.checkForValidConnection(connectionString)
    mongoObject = Mongo(connectionString, mongoDbName, mongoCollectionName)
    
    compressOrDecompress = args.version[0]

    #Compression version:
    if compressOrDecompress == 'compress':
        print('Entering compression mode')
        inputFileName = args.input[0]
        #Checking to see if input file exists
        if not os.path.isfile(inputFileName):
            sys.exit('Please enter a valid input file, program terminating')

        comp = Compression(inputFileName, fieldsToBreakOrReconstructOn, whichCompressionAlgToUse, mongoObject)
        
        #Optional argument 'memory' was not set:
        if memoryCap is None:
            #Breaks up CSV into segments, loads entire CSV into memory
            comp.iterateCsvBreakUpOnAttributes()
            #Compresses and writes segments to Mongo DB in parallel
            comp.compressChunksInParallel()
        #Optional argument 'memory' was set:
        else:
            #Max length of each segment
            comp.memoryCap = memoryCap
            #Break up CSV into segments of up to max length, after segment is passed to 
            #compression process, segment is cleared from Smart Compress memory
            #Compression and writing happen in parallel
            comp.breakUpCsvAndCompressChunksMemorySensative()

    #Decompression version:
    else:
        print('Entering decompression mode')
        outputFileName = args.input[0]

        decomp = Decompression(outputFileName, fieldsToBreakOrReconstructOn, whichCompressionAlgToUse)
        #Checking to make sure user defined output file name is valid
        decomp.checkValidOutputFileName()
        #Sending query to Mongo Db on 'fields' argument
        decomp.segments = mongoObject.retrieveSegmentsFromDatabase(decomp.columnsToReconstructOn)
        #Decompressing and writing results in parallel
        decomp.decompressAndCombineInParallel()
