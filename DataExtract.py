'''

Author: Lakshmi Arbatti
Project: Buck_USF Project
Created on 11/16/14. Reads files from the directories and converts Variant Call Format files into txt and csv
Version update: 11/20/14 - Handled huge files by breaking them into chunks

'''

import re
import os
from os.path import isfile, join
from os import listdir
from datetime import datetime
import csv


"""
Reading file 1GB at a time
"""
def readInChunks(fileObj, chunkSize=100000000):
    while True:
        data = fileObj.read(chunkSize)
        if not data:
            break
        yield data

def readTitleInfo(dataList,lSeek):
    global titleInfo
    titleInfo.append(re.split('[' '\s\n;]',dataList[lSeek]))
    titleInfo.append(('\n').strip("['']"))
    lSeek+=1
    return lSeek, titleInfo

def readWriteHeaderInfo(dataList,txtFile):
    lSeek = 0
    with open(txtFile,'w') as txt:
            for line in dataList:
                if line.startswith('##'):
                    txt.writelines("%s\n" %line)
                    lSeek+=1
    return lSeek

def getFileNames():
    myPath = os.getcwd()
    onlyFiles = [ f for f in listdir(myPath) if isfile(join(myPath,f))]
    return onlyFiles


if __name__ == "__main__":
    start_time_complete = datetime.now()        

    #Get the list of files in the current working directory
    onlyFiles = getFileNames()

    for fileName in onlyFiles:
        start_time_file = datetime.now()
        if fileName.endswith('.vcf'):
            iteration = 0
            firstRead = True

            ##
            dataNew  = []
            del dataNew[:]
            dataList = []
            del dataList[:]

            ##
            print "Converting vcf file....", fileName
            noExtFile = fileName.strip('.vcf')
            txtFile = noExtFile+'.txt'
            titleInfo = []
            print "File size", os.stat(fileName).st_size
            with open(fileName) as vcfFile:
                
                for dataChunk in readInChunks(vcfFile):
                    lSeek = 0
                    iteration+=1
                    dataList = dataChunk.split('\n')
                    #If reading the first chunk of data which contains the meta information and the header
                    if firstRead:        
                        #Write the header information into a txt file for future reference
                        lSeek = readWriteHeaderInfo(dataList,txtFile)
                        #Extract the title info to add to each .csv file
                        data = dataChunk.replace(',','&')
                        lSeek,titleInfo = readTitleInfo(dataList,lSeek)
                        firstRead = False

                        
                    #Replacing , with & in the data section so that it formats properly in the .csv file
                    dataNew = dataChunk.replace(',','&')
                    dataList = dataNew.split('\n')
                    dataDump = dataList[lSeek:]
                    data = []
                    for line in dataDump:
                        if None == line:
                            print "Data Error: No line to read"
                            break
                        data.append(re.split(r'[;\s\n]',line.strip("'")))
##                    print type(data), type(data[0]),type(data[0][0]),data[0][0]
                    csvFile = noExtFile + str(iteration)
                    csvFile = csvFile+'.csv'
                    with open(csvFile,'w') as outFile:                 
                        titleInfoStr = ','.join(map(str, titleInfo[0]))
                        titleInfoStr = titleInfoStr.strip("['")
                        titleInfoStr = titleInfoStr.strip("']\n")
                        outFile.write(titleInfoStr.strip("'")+'\n')
                        startLine = False
                        for line in data:
                            if startLine:
                                lineStr = ','.join(map(str, line))
                                lineStr = lineStr.strip("['")
                                lineStr = lineStr.strip("']\n")
                                outFile.writelines(lineStr.strip("'")+'\n')
                            else:
                                startLine = True
                    end_time_file = datetime.now()
                    print "Finished writing file...", csvFile
                    print('Duration taken to write the file was : {}'.format(end_time_file - start_time_file))

    end_time_complete = datetime.now()
    print ('Duration taken to convert all files: {}'.format(end_time_complete - start_time_complete))
