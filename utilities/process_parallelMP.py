#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 20:35:04 2020

@author: lvardges
"""


import multiprocessing as mp
from functools import partial

class processParallelMP():
    """
    The class applies its member function processingFunction to items_list elements in parallel.
    
    INPUT: 
        processingFunction: function
            Has to follow the format processingFunction(self, **processingFunction_kwargs)
            
        items_list: list
            List of items to be processed. 
        
        processingFunction_kwargs: dict
            These keywordargs will be applied to processingFunction via functools.partial
            
        no_threads: int
            Defines how many processes to use. Default value 0 means is all available. 
            
        verbose: int between 0 and 3
            Parameter for control of level of details of printed output messages. Can be used in processingFunction via self.verbose.
            
    OUTPUT:
        None
        
        
    USEFUL MEMBERS:
        locks_dict: dict of locks
            Contains data_lock for locking data elements accessed by multiple processes and print_lock for locking printout messages.
    """
    def __init__(self, processingFunction, items_list, processingFunction_kwargs = {}, no_threads = 0, ordered_results = True, verbose = 3):

        m = mp.Manager()
        self.locks_dict = {"data_lock": m.Lock(), "print_lock": m.Lock()} 
        self.processingFunction = processingFunction
        self.items_list = items_list
        self.verbose = verbose
        self.processingFunction_kwargs = processingFunction_kwargs
        self.no_threads = no_threads
        self.ordered_results = ordered_results
        
        if(no_threads < 0):
            raise Exception("Number of threads cannot take negative value.")
            
        if(verbose not in [0, 1, 2, 3]):
            raise Exception("Verbose parameter takes integer values between 0 and 3.")
        
        total_num_threads = mp.cpu_count()
        if(no_threads == 0 or no_threads > total_num_threads):
            self.no_threads = total_num_threads
        

      
        
    def process(self):

        self.processingFunction = partial(self.processingFunction, self, **self.processingFunction_kwargs)



        pool = mp.Pool(processes = self.no_threads)
        if(self.ordered_results == True):
            data = pool.map(self.processingFunction, self.items_list)
        else:
            data = pool.imap_unordered(self.processingFunction, self.items_list)
        
        pool.close()
        pool.join()

        return data



