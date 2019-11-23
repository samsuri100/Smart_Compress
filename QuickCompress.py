#!/usr/bin/python3
import os
import sys
import argparse
from compression_library import Compression
from decompression_library import Decompression

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog = 'SmartCompress', \
                                     allow_abbrev = False, \
                                     description = "Intelligently compress a CSV file! Breaks a CSV file into \
                                                    segments, on specified column attributes, compresses each \
                                                    segment, and stores each in a Mongo Database collection, in \
                                                    parallel, for quick retrieval")
    parser.add_argument('-m', '--method', \
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
                        help = '[-m, --method = compress: input file to break up, compress, and store] ~ \
                                [-m, --method = decompress: output file name to store retrieved, uncompressed, combined file segments')
    parser.add_argument('-f', '--fields', \
                        nargs = '+', \
                        type = str, \
                        metavar = 'FIELD', \
                        required = True, \
                        help = '[-m, --method = compress: columns attributes that the file will be broken on up] ~ \
                                [-m, --method = decompress: columns attributes that the file segments will be retrieved and constructed on]')
    parser.add_argument('-a', '--algorithm', \
                        nargs = 1, \
                        type = str, \
                        choices = ['gzip', 'bzip2', 'xz', 'zip'], \
                        required = True, \
                        help = '[-m, --method = compress: compression algorithm to use] ~ \
                                [-m, --method = decompress: decompression algorithm to use]')
    parser.add_argument('-d', '--database', \
                        nargs = 1, \
                        type = str, \
                        required = True, \
                        help = '[-m, --method = compress: Mongo Database to write to] ~ \
                                [-m, --method = decompress: Mongo Database to retrieve from]')
    parser.add_argument('-c', '--collection', \
                        nargs = 1, \
                        type = str, \
                        required = True, \
                        help = '[-m, --method = compress: collection within Mongo Database to write to] ~ \
                                [-m, --method = decompress: collection within Mongo Database to retrieve from]')
    args = parser.parse_args()
 
    fieldsToBreakOrReconstructOn = args.fields
    whichCompressionAlgToUse = args.algorithm[0]
    mongoDbName = args.database[0]
    mongoCollectionName = args.collection[0]
    
    compressOrDecompress = args.method[0]

    if compressOrDecompress == 'compress':
        inputFileName = args.input[0]
        if not os.path.isfile(inputFileName):
            sys.exit('Please enter a valid input file, program terminating')

        comp = Compression(inputFileName, fieldsToBreakOrReconstructOn, whichCompressionAlgToUse, mongoDbName, mongoCollectionName)
        comp.iterateCsvBreakUpOnAttributes()
        comp.compressChunksInParallel()

    '''
    else:
        outputFileName = args.output[0]

        decomp = Decompression(outputFileName, fieldsToBreakOrReconstructOn, whichCompressionAlgToUse, mongoDbName, mongoCollectionName)
    '''
