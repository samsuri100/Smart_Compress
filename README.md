# Smart Compress: Compression for Big Data
Intelligently compress or decompress a massive CSV file into and out of a Mongo Database, in parallel, with the ability to decompress only part of the original file, based on attributes and lookup information you provide. 

Smart Compress is essentially a compression manager for big data files, specifically CSV files, where the user can choose to break up the CSV into different segments on any number of columns, where each segment is a unique combination of the values in those columns. For example, with the column ['STATE'], we have the segments [['CA', ROW-DATA], ['TX', ROW-DATA]...] and for the COLUMNS ['STATE', 'CITY'], we have the segments [['CA', 'San Francisco', ROW_DATA], ['TX', 'AUSTIN', ROW-DATA]...]. Whatever the field, and whatever the number, just 1 or multiple, you can use them. If the CSV file is too big to fit in memory, a memory flag can be appended, which takes an integer, and this integer will limit the size of each segment. Once this size is reached, that segment is then compressed in parallel, to the ongoing segmenting, by another process, but it is cleared from Smart Compress's memory, so memory thrashing will never occur. Smart Compress by default loads the entire CSV into memory before compressing, to ensure each segment contains all of the appropriate rows. Each segment is then compressed using either the GZIP, BZIP2, XZ (LZMA), or ZLIB compression algorithms, depending on user input, in parallel, and is then written to a Mongo database, also in parallel, where you can specify the database name, the collection name, and the connection string to connect to the database. 

For decompression, you can again the specify the connection string, the database name, and the collection name, so that Smart Compress can connect to your Mongo Database instance, whether local or remote, and have access to the stored segments. The user also specifies the fields (columns) they are interested in and the values that they want present in each field. For example: ['STATE'=CA] or ['STATE'='TX', 'CITY'='Austin', 'OCCUPATION='Software Engineer']. The fields are queried in Mongo Database, and the resulting data is then decompressed using either the GZIP, BZIP2, XZ (LZMA), or ZLIB compression algorithms, according to using input, in parallel, and then the output is written to an output file, specified by the user, in parallel with decompression using threading. 

## Features:
-Specify connection string to your Mongo Database, so it can be local or remote  
  
-Specify database name and collection that you would like to write to or pull from  
  
-Can break up CSV on any number of columns, to allow for easy lookup later  
  
-Pick your choice of compression algorithm: GZIP, BZIP2, XZ (LZMA), or ZLIB  
  
-Compression and decompression is done in parallel for each segment  
  
-Decompression and writing output are done in parallel, so decompression doesn't delay writing  
  
-Memory sensitive option for CSV files that are too big to fit in memory  
  
## Examples:
Given the file testInput.csv, a subset of a Federal Election Commission data-set on 2016 individual donors:

|FIELD1   |CMTE_ID|AMNDT_IND|RPT_TP|TRANSACTION_PGI|IMAGE_NUM|TRANSACTION_TP|ENTITY_TP|NAME       |CITY        |STATE|ZIP_CODE |EMPLOYER        |OCCUPATION            |TRANSACTION_DT|TRANSACTION_AMT|OTHER_ID|TRAN_ID             |FILE_NUM|MEMO_CD|MEMO_TEXT|SUB_ID               |
|---------|-------|---------|------|---------------|---------|--------------|---------|-----------|------------|-----|---------|----------------|----------------------|--------------|---------------|--------|--------------------|--------|-------|---------|---------------------|
|C00088591|N      |M3       |P     |15970306895    |15       |IND           |BURCH    | MARY K.   |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP PROGRAM MANAGEMENT |2132015       |500            |        |2A8EE0688413416FA735|998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970306960    |15       |IND           |KOUNTZ   | DONALD E. |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|DIR PROGRAMS          |2132015       |200            |        |20150211113220-479  |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970306960    |15       |IND           |KOUNTZ   | DONALD E. |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|DIR PROGRAMS          |2272015       |200            |        |20150225112333-476  |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970306915    |15       |IND           |DOSHI    | NIMISH M. |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP AND CFO            |2132015       |200            |        |20150309_2943       |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970306915    |15       |IND           |DOSHI    | NIMISH M. |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP AND CFO            |2272015       |200            |        |20150224153748-2525 |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970306992    |15       |IND           |NASTASE  | DAVID W.  |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP AND CIO            |2132015       |200            |        |20150309_695        |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970306993    |15       |IND           |NASTASE  | DAVID W.  |FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP AND CIO            |2272015       |200            |        |20150224153748-603  |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970307024    |15       |IND           |SCHMIDT  | GREGORY A.|FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP AND GENERAL MANAGER|2132015       |220            |        |20150309_811        |998834  |       |         |4.03202015124089E+018|
|C00088591|N      |M3       |P     |15970307024    |15       |IND           |SCHMIDT  | GREGORY A.|FALLS CHURCH|VA   |220424511|NORTHROP GRUMMAN|VP AND GENERAL MANAGER|2272015       |220            |        |20150224153748-701  |998834  |       |         |4.03202015124089E+018|

If we run Smart Compress on it, in compression version, breaking it up by the 'OCCUPATION' field:
```
python SmartCompress.py -v compress -i testInput.csv -a gzip -f '"OCCUPATION"' -d '"FecTestData"' -c '"Indiv2016Test"'
```

We get the response:
```
Program by default connects to Mongo Database @ localhost, port 27017
Would you like to change this? (Y/N)
N
Established connection to Mongo Database
Entering compression mode
Breaking up CSV into segments, based on field/s: ['OCCUPATION']
Compressing segments in parallel using 'gzip' compression algorithm
Segment successfully written to Mongo Database: ['VP AND CFO']
Segment successfully written to Mongo Database: ['VP AND CIO']
Segment successfully written to Mongo Database: ['VP PROGRAM MANAGEMENT']
Segment successfully written to Mongo Database: ['DIR PROGRAMS']
Segment successfully written to Mongo Database: ['VP AND GENERAL MANAGER']
```

And in Mongo DB, it looks like:
```
{ "_id" : ObjectId("5ddf3324b7cb6eec812114da"), "OCCUPATION" : "VP PROGRAM MANAGEMENT", "compressedObject" : BinData(0,"H4sIACQz310C/zWPwQ6DIAyGX4Ubl2UpUBSO1aEuEyRMTRbj+7/GrMYDX9u0/f+ybbIFAOesV/IhZGJEw8wMZX0NBirn7VUy3+nFoVlKO3AiIpWf+Dw572gcv6Id7t5KTK0BNVp1eUxlHsqURV+WGOm0XLPIZeoLxUMsUR9iSDM3QCuj4fK1ABxOQXIhHGc5VAZV1VFtzhHvnTN4T/FDOPZZQWnkf1Ya5b7/Afekrmz4AAAA") }
{ "_id" : ObjectId("5ddf3324c68e2097762114da"), "OCCUPATION" : "DIR PROGRAMS", "compressedObject" : BinData(0,"H4sIACQz310C/82PQQ6DIBBFr8KOjW2GARSWRFo1FTCoXdR4/2uU0XTZpMuS8JkPw/y8beMtABijreAV45EkSNKJRGjbgITa1nBa0iF6Oh5pjcuLKuZTdKNntyu5uxvHmbX9mtue/NORIoJCpcUZk/LS5zSxLq8huCPVD5lNOXXZhZk8oJAIZyLCEX+WQpensmQZeVGNpVtrjZHq00NbQflNzQIV8Rlh+V6xf+bF5isvlkEopSy89Y+8CHzf32Pbr7PgAQAA") }
{ "_id" : ObjectId("5ddf3324faa0c2c8092114da"), "OCCUPATION" : "VP AND CIO", "compressedObject" : BinData(0,"H4sIACQz310C/72Puw7CMAxFfyVbFkCO8x6jBGilvpQ+GKqK//8L6kaMSJ0YfOwrO7q568ojADinveAXxjtCK4kDQWhvQYLxHosk1l06bsM4hfFOI0thqRN73Ug8QtOMLFZzjhXpJRARQaHSotj0eapyP7Bnnts2HK7LwEKXWKx7UoBCIhQ/BKBWRqH3//i38cfKe+ek+m6pFOzv6EygKrkM3y7sdE7575xof+VEVEJLq9zVgDyb1vJt+wBQXRdG1gEAAA==") }
{ "_id" : ObjectId("5ddf3324b3214930e52114da"), "OCCUPATION" : "VP AND CFO", "compressedObject" : BinData(0,"H4sIACQz310C/82PwQ6DIAyGX4UbF7eUUhSORuMgETA6vRiz93+LWcmOS3bcoV/7h5K//77LDgCsNU7JSsjEiJo5MZRxDWionTJFMkPqufV58YEHkUIMixfxzmpox3ERnV/nzrPeWiYiEJJRxSXPTz/nSTzmNcb2Mt0m0aZedENmBag0QrFDAG5lVOY8x73Q0XWkc9Zq+jxzEZwfeU8hca7aanlU4m9zYvMtJyIpoxuyNzRofk1L8jjeeFyicNYBAAA=") }
{ "_id" : ObjectId("5ddf3324f85b8dee4b2114da"), "OCCUPATION" : "VP AND GENERAL MANAGER", "compressedObject" : BinData(0,"H4sIACQz310C/9VPyw6DIBD8FW5cbLMsWOBI1IqJoMFq0hjT//+LspIem/TaA7M7+xpm33kDAMbUVvCK8UgQJOFMIGqrQYIGVIUSDrGlsDQ+DO2DUtanrp/Sk7kr0bsbx4U1fk2NJ745QkRQqGpRhKb08Gma8+Yagjt1t5m52LK+i11yI8tV13eJOoBCIhT1fIbCmeZS/p19mXLUWmOk+nTpKch7NCZQZZc3yINHxf7FNepvrhGVqKVW5qLhZ+/Ij+MNCiPV2/IBAAA=") }
```

If we run Smart Compress on it, in decompression version, searching for the value, 'VP AND CIO' in the 'OCCUPATION' field:
```
 python SmartCompress.py -v decompress -o '"output.csv"' -a gzip -f '"OCCUPATION=VP AND CIO"' -d '"FecTestData"' -c '"Indiv2016Test"'
```

We get the response:
```
Program by default connects to Mongo Database @ localhost, port 27017
Would you like to change this? (Y/N)
N
Established connection to Mongo Database
Entering decompression mode
Successfully retrieved query results on: Indiv2016Test, from Mongo Database: FecTestData
Decompressing in parallel using 'gzip' algorithm
Writing uncompressed output to file 'output.csv' in parallel with decompression
Decompressing segment: [OCCUPATION: VP AND CIO]
Writing segment [OCCUPATION: VP AND CIO] to file 'output.csv'
```

And our output file, 'output.csv', looks like:

|         |   |   |   |           |   |   |       |         |            |   |         |                |          |       |   |   |                  |      |   |   |                     |
|---------|---|---|---|-----------|---|---|-------|---------|------------|---|---------|----------------|----------|-------|---|---|------------------|------|---|---|---------------------|
|C00088591|N  |M3 |P  |15970306992|15 |IND|NASTASE| DAVID W.|FALLS CHURCH|VA |220424511|NORTHROP GRUMMAN|VP AND CIO|2132015|200|   |20150309_695      |998834|   |   |4.03202015124089E+018|
|C00088591|N  |M3 |P  |15970306993|15 |IND|NASTASE| DAVID W.|FALLS CHURCH|VA |220424511|NORTHROP GRUMMAN|VP AND CIO|2272015|200|   |20150224153748-603|998834|   |   |4.03202015124089E+018|

## Planned Future Updates:
-Will have the output csv, for the decompression version, have a header row with the original column names  
  
-What happens if you forgot the compression algorithm you used originally, and you need to decompress something?  
  
-Benchmarks for Smart Compress versus simple compress and decompression
