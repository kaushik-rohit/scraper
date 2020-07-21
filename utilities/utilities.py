#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 18:39:21 2019


"""

import os
import pandas as pd
from collections import defaultdict
from itertools import chain
import datetime
import functools
import pickle
import time
import numpy as np


import traceback

from multiprocessing import cpu_count, Pool

def trace_unhandled_exceptions(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print('Exception in '+func.__name__)
            traceback.print_exc()
    return wrapped_func



def pdMapSeriesMP(series, dictionary, map_splits = 1, no_threads = None):
    

    data_splits = np.array_split(series, map_splits)

    
    def createMapFunction(dictionary):
        ### without global we got pickle problem in Pool
        global map_function
        def map_function(a, dictionary):
            return pd.Series.map(a, dictionary)
        map_fun = functools.partial(map_function, dictionary = dictionary)
        return map_fun
    ### we have to call this function here as calling it within pool.map below creates problem with map_function in the processes
    mf = createMapFunction(dictionary)

    if(no_threads is None):
        no_threads = cpu_count()

    pool = Pool(no_threads)
    for i, data_split in enumerate(data_splits):
        data_subsplits = np.array_split(data_split, no_threads)
        data = pd.concat(pool.map(mf, data_subsplits))
        data_splits[i] = data
    data = pd.concat(data_splits)
    pool.close()
    pool.join()
    return data  



def pdFunctionDataframeMP(dataframe, function, args = {}, map_splits = 1):
    data_splits = np.array_split(dataframe, map_splits)
    
    function = functools.partial(function, **args)
    
    cores = cpu_count()
    pool = Pool(cores)
    for i, data_split in enumerate(data_splits):
        data_subsplits = np.array_split(data_split, cores)
        data = pd.concat(pool.map(function, data_subsplits))
        data_splits[i] = data
    data = pd.concat(data_splits)
    pool.close()
    pool.join()
    return data   


##Takes dwo default dicts of int type and adds their values by keys
def addDefaultDictionaries(a,b):
    tempDict = defaultdict(int)
    for k, v in chain(a.items(), b.items()):
        tempDict[k] += v
    return tempDict

# Given path has to end with "/" e.g. dir/dir/dir/ as this function removes everyting after last "/"
def ensureDir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def pdPrint(df):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)
        
def isNumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
    
def getDefaultDictionaryTotalCount(default_dictionary):
    return sum(default_dictionary.values())
    
# This function has an upper limit on max secounds of sth like 30 days
def convertTime(seconds):
    d = datetime.datetime(1,1,1) + datetime.timedelta(seconds = seconds)
    return [d.day-1, d.hour, d.minute, d.second]


def timeIt_old(func):
    @functools.wraps(func) ### This is so that the name of wrapped function will be the original name and not wrapper_timeIt
    def wrapper_timeIt(*args, **kwargs):
        startTime = time.time()
        val = func(*args, **kwargs)
        duration = convertTime(time.time() - startTime)
        print("\n")
        print("Calculations took: " + str(duration[0]) + " days " + str(duration[1]) + " hours " + str(duration[2]) + " minutes " + str(duration[3])  + " seconds")
        return val
    return wrapper_timeIt

def timeIt(func):
    @functools.wraps(func)
    def wrapper_timeIt(*args, **kwargs):
        startTime = datetime.datetime.now()
        val = func(*args, **kwargs)
        duration = datetime.datetime.now() - startTime
        print("\n")
        print("Calculations took: " + str(duration))
        return val
    return wrapper_timeIt

def pickleSave(lambda_path):
    def decorator_pickleSave(func):
        @functools.wraps(func)
        def wrapper_pickleSave(self, *args, **kwargs):
            path = lambda_path(self)
            if(os.path.isfile(path) == 0):
                ensureDir(path)
                value = func(self, *args, **kwargs)
                
                with open(path, 'wb') as file:
                    pickle.dump(value, file)
            else:
                with open(path, 'rb') as file:
                    value = pickle.load(file)
                
            return value
        return wrapper_pickleSave
    return decorator_pickleSave



def CSVSave(lambda_path):
    def decorator_CSVSave(func):
        @functools.wraps(func)
        def wrapper_CSVSave(self, *args, **kwargs):
            path = lambda_path(self)
            if(os.path.isfile(path) == 0):
                ensureDir(path)
                value = func(self, *args, **kwargs)
                

                if("[" in path and "]" in path):
                    items = path.split("[")[1].split("]")[0].split(", ")
                    items = list(np.array([item.split(",") for item in items]).ravel())
                    
                    if(len(value) != len(items)):
                        raise("@CSVSave: Number of .csv names does not match number of returned arrays.")
                        
                    for i in range(len(value)):
                        value[i].to_csv("/".join(path.split("/")[:-1]) + "/" + items[i] + ".csv", index = False)
                else:
                    value.to_csv(path, index = False)
                
            else:
                value = pd.read_csv(path)
                
            return value
        return wrapper_CSVSave
    return decorator_CSVSave
