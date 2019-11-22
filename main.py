#!/usr/bin/python3
import argparse
import compression_object

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='SmartCompress', \
                                     allow_abbrev = False, \
                                     description = "Intelligently compress a CSV file! Breaks CSV file into \
                                                    chunks, on specided column attributes, compresses each \
                                                    chunk and stores in MongoDB for quick retrieval")
    parser.add_argument('-f', '--file', \
                        nargs = 1, \ 
                        type = argparse.FileType('r'), \
                        required = True, \
                        help = 'File to break up into chunks and compress')
    parser.add_arugment('-a', '--attributes', \
                        nargs = '+', \
                        type = str, \
                        required = True, \
                        help = 'Columns attributes that the chunks will be broken on up')
    parser.add_argument('-c', '--compression', \
                        nargs = 1, \
                        type = str, \
                        choices = ['gzip', 'bzip2', 'xz', 'zip'], \
                        required = True, \
                        help = 'Compression or decompression algorithm to use')
    parser.add_argument('-p', '--parallelized', \
                        nargs = 1, \
                        type = str, \
                        choices = ['T', 'True', 'F', 'False'] \
                        required = True, \
                        help = 'Option to parallelize compression, use only if specified \
                                column attributes result in few, very large, chunks') 
    args = parser.parse_args()

    fileName = args.file[0]
    columnsToBreakUpON = [columns.name for columns in args.attributes]
    whichCompressionAlgToUse = args.compression[0]
    parallelOption = args.parallelized[0]

    comp = CompressionObject(fileName, columnsToBreakUpOn, whichCompressionAlgToUse, parallelOption)
    comp.iterateCsvBreakUp()

    
