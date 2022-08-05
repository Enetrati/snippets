#!/usr/bin/env python3

import multiprocessing as mp
import os

class Lichess:
    def __init__(self, file_name=None):
        self.file_name = file_name
        #self.cores = mp.cpu_count()
        self.cores = 4

        self.Start()

    def process(self, line):
        #l = line.replace('[', ' ').replace(']', ' ')
        #if l == "Event":
        print(line)

    def process_wrapper(self, chunkStart, chunkSize):
        with open(self.file_name) as f:
            f.seek(chunkStart)
            lines = f.read(chunkSize).splitlines()
            for line in lines:
                self.process(line)

    def chunkify(self, fname,size=2 * 128):
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
        #init objects
        pool = mp.Pool(self.cores)
        jobs = []

        #create jobs
        for chunkStart, chunkSize in self.chunkify(self.file_name):
            jobs.append( pool.apply_async(self.process_wrapper,(chunkStart,chunkSize)) )

        #wait for all jobs to finish
        for job in jobs:
            job.get()

        #clean up
        pool.close()

def main():
    try:
        file_name = "/home/chris/src/python/chunk-read/src/files/lichess_db_standard_rated_2013-08.pgn"
        l = Lichess(file_name)
    except IOError as ioe:
        print("Error during processing file: ", ioe)

if __name__ == '__main__':
    main()
