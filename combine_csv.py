import sys
import os
import pandas as pd
from pandas import read_csv


class Combiner:

    def __new__(cls, *args, **kwargs):
        if cls is Combiner:
            raise TypeError(f"only children of '{cls.__name__}' may be instantiated")
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, errorFileName='errorLog.txt'):
        self.__errorFileName = errorFileName

    def cleanLog(self):
        if os.path.exists(self.__errorFileName):
            os.remove(self.__errorFileName)

    def logError(self, message):
        mode = 'w'
        if os.path.exists(self.__errorFileName):
            mode = 'a'
        with open(self.__errorFileName, mode, encoding='utf-8') as errorFile:
            errorFile.write(message)
            errorFile.write('\n')
            errorFile.close()

    #takes path as an input 
    #return boolean output
    #checks the validity of parameters
    def validatefilepath(self, path_file):
        # to check the existence of directory
        if not os.path.exists(path_file):
            self.logError("Error: Please verify your input at - " + path_file + ". Directory doesnt exist")
            return False
        # to check if there is an empty file
        if os.stat(path_file).st_size == 0:
            self.logError("Error:Please verify your input at - " + path_file + ". File is empty.")
            return False
        return True

    #divide the file in read in chunks
    #this will help us read large files smoothly
    def readfilechunks(self, path_file, chunk_size):
        list_chunks = []
        if chunk_size <= 0:
            self.logError("Error: Size of chunk should be greater than 1.")
            return None
        for chunk in read_csv(path_file, chunksize=chunk_size):
            list_chunks.append(chunk)
        return list_chunks

    def combine_files(self, files: [], chunk_size=10 ** 4):
        pass

# using FACTORY design pattern for extensiblity and reusablity
class CombinerFactory:
    @staticmethod
    def createCombiner():
        return combine_CSV()


# create class with parameters from class arg

class combine_CSV(Combiner):
    #reads the file in chunks and add the colomn filename
    def __read_chunks__(self, path_file, chunk_size):
        file_name = os.path.basename(path_file)
        chunks = self.readfilechunks(path_file, chunk_size)
        for chunk in chunks:
            chunk['filename'] = file_name
        return chunks


   #combines the csvs
   #takes files amd chunksize as parameter
   #retuns the combined csv
    def combine_files(self, files: [], chunk_size=10 ** 5):
        # we will read the data as chunks to deal with huge data
        chunk_list = []
        for filePath in files:
            if self.validatefilepath(filePath):
                chunk_list.extend(self.__read_chunks__(filePath, chunk_size))
        if len(chunk_list) == 0:
            self.logError("Error: Please verify all the inputs.")
        # merging the chunks
        for index in range(len(chunk_list)):
            if index == 0:
                print(chunk_list[index].to_csv(index=False, line_terminator='\n', chunksize=chunk_size), end='')
            else:
                print(chunk_list[index].to_csv(index=False, header=False, line_terminator='\n', chunksize=chunk_size),
                      end='')


if __name__ == '__main__':
    args = sys.argv
    file_paths = []
    if len(sys.argv) > 1:
        file_paths = args[1:]
    combiner = CombinerFactory.createCombiner()
    combiner.cleanLog()
    combiner.combine_files(file_paths)
