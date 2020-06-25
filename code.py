from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time 
import itertools
import threading
from typing import List
import queue
import sys
import re
#import psutil ==> uncomment to measure the system perfomance
#import os

#global dictionary for keeping track of word count
d = {} 
executor = ThreadPoolExecutor(max_workers =5)
#function to count the number of words for a given chunk of data
def count_words(lines):
    for line in lines:
        words=re.findall(r'\w+[a-zA-Z]',line)
        for word in words:
            d[word] = d.get(word, 0) + 1
#function to read file in chunks and invoke thread
def read_file(filename,k: int,chunk_size=35000): 
    futures = []
    with open(filename) as f:
        while True:
            lines = list(itertools.islice(f, chunk_size))
            if not lines:
                break
            futures.append(executor.submit(count_words,lines))
            for fut in concurrent.futures.as_completed(futures):
                fut.result()
    ret = sorted(d, key=lambda word: (-d[word], word))
    print(ret[0:int(k)])
if __name__ == '__main__':
    read_file(sys.argv[1],sys.argv[2])#reading filename and K count from user
    #Printing the memory usage and cpu time
    #uncomment to measure the system perfomance
    #process=psutil.Process(os.getpid())
    #print('CPU : ',process.cpu_times())
    #mem = process.memory_percent()
    #mem1 = process.memory_info()
    #print('Memory: ',mem, 'mem1 : ',mem1)
