#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 18:37:06 2019

"""

import time
import threading
import multiprocessing
from functools import partial


class myThread (threading.Thread):
    def __init__(self, function):
        threading.Thread.__init__(self)
        self.function = function
        
    def run(self):
        self.function()
            


class processParallel():

    def __init__(self, work_queue, processingFunction, params = {}, data = [], no_threads = 1, verbose = 3, processOnceFunction = None, object_above = None):
        self.exit_flag = [0]
        self.processingFunction = processingFunction
        self.processOnceFunction = processOnceFunction
        self.object_above = object_above
        self.work_queue = work_queue
        self.threads = []
        self.verbose = verbose
        self.locks_dict = {"work_queue_lock": threading.Lock(), "data_lock": threading.Lock(), "print_lock": threading.Lock()}
        self.data = data
        self.params = params

        if(no_threads < 0):
            raise Exception("Number of threads cannot take negative value.")
            
        if(verbose not in [0, 1, 2, 3]):
            raise Exception("Verbose parameter takes integer values between 0 and 3.")
        
        total_num_threads = multiprocessing.cpu_count()
        if(no_threads == 0 or no_threads > total_num_threads):
            no_threads = total_num_threads
        
        self.run_event = threading.Event()
        self.run_event.set()
        
        threadID = 1
        for i in range(no_threads):
            self.threads.append(myThread( partial(self.processingLoop, threadID) ))
            threadID += 1

    def processingLoop(self, threadID):
            
            if(self.processOnceFunction is not None):
                self.processOnceFunction(self)
        
            while not self.exit_flag[0]:

                
                self.locks_dict["work_queue_lock"].acquire()

                if self.work_queue.empty():
                    self.exit_flag[0] = 1

                if not self.work_queue.empty():
                    work_piece = self.work_queue.get()
                    self.locks_dict["work_queue_lock"].release()

                    if(self.verbose >= 2):
                        self.locks_dict["print_lock"].acquire()
                        print("Thread %s processing %s" % (threading.current_thread(), work_piece), flush=True)
                        self.locks_dict["print_lock"].release()


#                    try:
                    self.processingFunction(self, work_piece)
#                    except Exception as e:
#                        self.locks_dict["work_queue_lock"].acquire()
#                        self.work_queue.put(work_piece)
#                        self.exit_flag[0] = 0
#                        self.locks_dict["work_queue_lock"].release()
#                        if(self.verbose >= 2):
#                                self.locks_dict["print_lock"].acquire()
#                                print("\n", flush=True)
#                                print(">>> Exception. " + work_piece + " will be reacquired", flush=True)
#                                print(str(e))
#                                print("\n", flush=True)
#                                self.locks_dict["print_lock"].release()                                
                        
                    
                    if(self.verbose >=1):
                        self.locks_dict["print_lock"].acquire()
                        print("Thread %s finished processing %s" % (threading.current_thread(), work_piece), flush=True)
                        self.locks_dict["print_lock"].release()
                        


 
                else:
                    self.locks_dict["work_queue_lock"].release()

                ## If processingFunction is fast the value here ought to be smaller. 
                ## It saves you from hanging when for example quickly using print("something", flush = True)
                time.sleep(0.005)




    def process(self):
        for thread in self.threads:
            thread.start()

        for thread in self.threads:
            thread.join()
    
    def join(self):
        self.run_event.clear()
        for thread in self.threads:
            thread.join()
