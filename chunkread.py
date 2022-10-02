#!/usr/bin/env python3
'''
This is an code snippet on howto read an large file
in chunks and process them with the help of
multiprocessing 
'''

import multiprocessing as mp
import os

class FileReader:
    def __init__(self, file_name=None):
        self.file_name = file_name
        self.cores = mp.cpu_count()

        self.Start()

    # Process the chunk
    def process(self, line):
        print(line)

    def process_wrapper(self, chunkStart, chunkSize):
        with open(self.file_name) as f:
            f.seek(chunkStart)
            lines = f.read(chunkSize).splitlines()
            for line in lines:
                self.process(line)

    # Create chunks
    def chunkify(self, fname,size=256):
        fileEnd = os.path.getsize(fname)
        with open(fname,'rb') as f:
            chunkEnd = f.tell()
            while True:
                chunkStart = chunkEnd
                f.seek(size, 1)
                f.readline()
                chunkEnd = f.tell()
                yield chunkStart, chunkEnd - chunkStart
                if chunkEnd > fileEnd:
                    break

    def Start(self):
        # init objects
        pool = mp.Pool(self.cores)
        jobs = []

        # create jobs
        for chunkStart, chunkSize in self.chunkify(self.file_name):
            jobs.append( pool.apply_async(self.process_wrapper,(chunkStart,chunkSize)))

        # wait for all jobs to finish
        for job in jobs:
            job.get()

        # clean up
        pool.close()

if __name__ == '__main__':
    # Trying to process file
    try:
        file_name = "/home/chris/src/python/chunk-read/src/files/lichess_db_standard_rated_2013-08.pgn"
        FileReader(file_name)
    except IOError as ioe:
        print("Error during processing file: ", ioe)
