#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 18:35:22 2019


"""

import datetime
from bidict import bidict
import numpy as np
import functools

## Takes dates given in format    'd/dd-month_name-yyyy'    or    'month_name-yyyy'    or    'yyyy'
## where d/dd in {one or two digit number representing a day} with single digit numbers 1, 2, ..., 9
## month_name in {jan, feb, apr, ..., dec} three letter abbreviation of english month name
## yyyy in {four digit number representing year} eg. 2017

def getFullDates(start_date, end_date, vector = 1):
    

    #Takes a date given in    'd/dd-month_name-yyyy'    format
    #returns a date given in    '[d/dd, m/mm, yyyy]'    format

    def convertDate(date):
        months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
                              '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
                              '11': "nov", '12': "dec"}
        months = bidict(months_num_to_name)
        
        date = date.split("-")
        
        day = date[0]
        month = months.inv[date[1]]
        year = date[2]    
        
        return [day, month, year]

    #Takes a date given in    'month_name-yyyy'    format
    #Returns a date given in    'dd'    format (only double digit days are final days)

    def getEndDay(month_name, year):
        months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
              '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
              '11': "nov", '12': "dec"}
        months = bidict(months_num_to_name)
        
        month_no = months.inv[month_name]
        
        for i in range(31, 27, -1):
            try:
                datetime.datetime(year=int(year),month=int(month_no),day=int(i))
                final_day = i
                break
            except ValueError:
                continue

        return str(final_day)

    ## Takes a date given in    'd/dd-month_name-yyyy'    or    'month_name-yyyy'    or    'yyyy'    format
    ## returns a date given in    'd/dd-month_name-yyyy'    format
    def completeDate(date, start_or_end):
        if(start_or_end == "start"):
            if(len(date) == 10 or len(date) == 11):
                return date
            elif(len(date) == 8):
                return "1-" + date
            else:
                return "1-jan-" + date
            
        if(start_or_end == "end"):

                    
            
            if(len(date) == 10 or len(date) == 11):
                return date
            elif(len(date) == 8):
                end_day = getEndDay(date.split("-")[0], date.split("-")[1])
                
                return end_day + "-" + date
            
            else:
                end_day = getEndDay("dec", date)
                return end_day + "-dec-" + date           


    ## Takes a date given in    'd/dd-month_name-yyyy'    format
    ## and raises an error if the date is incorrect
    def checkFullDateCorrectness(date):
        date_conv = convertDate(date)
        try:
            ## Takes a date given in    year = yyyy, month = m/mm, day = d/dd    format
            datetime.datetime(year=int(date_conv[2]),month=int(date_conv[1]),day=int(date_conv[0]))
        except ValueError:
            raise ValueError("Incorrect date given.") 
            

    def checkDateCorrectness(date, start_or_end):

        if(len(date) == 4 or len(date) == 8):
            date = completeDate(date, start_or_end)
            checkFullDateCorrectness(date)
            return date
        
        elif(len(date) == 10 or len(date) == 11):
            checkFullDateCorrectness(date)
            return date
        else:
            raise Exception("Incorrect date given.")
            
    ## Check start date correctness
    checkDateCorrectness(start_date, "start")
            
    ## Check end date correctness
    checkDateCorrectness(end_date, "end")
    
    ## Complete the dates
    complete_start_date = completeDate(start_date, "start")
    complete_end_date = completeDate(end_date, "end")
    
    if(vector == 1):
        complete_start_date_converted = convertDate(complete_start_date)
        complete_end_date_converted = convertDate(complete_end_date)
        return [complete_start_date_converted, complete_end_date_converted]
    else:
        return [complete_start_date, complete_end_date]
    
    



## Takes a start_date and an end_date given in    'd/dd-month_name-yyyy'    format
## and raises an error if the start_date does not precede end_date         
def checkDatesPrecedence(start_date, end_date):
    [start_date_conv, end_date_conv] = getFullDates(start_date, end_date, 1)
    try:
        ## Takes a date given in    'year = yyyy, month = m/mm, day = d/dd'    format
        start = datetime.datetime(year=int(start_date_conv[2]),month=int(start_date_conv[1]),day=int(start_date_conv[0]))
        end = datetime.datetime(year=int(end_date_conv[2]),month=int(end_date_conv[1]),day=int(end_date_conv[0]))
        if(end < start):
            raise Exception("Start date must precede the end date.")
    except ValueError:
        raise ValueError("Incorrect date given.")   
    

    
def getMonths(start_date, end_date, month_names = True, months_with_zeros = False):
    
    [start_date_conv, end_date_conv] = getFullDates(start_date, end_date, 1)
    checkDatesPrecedence(start_date, end_date)
    
    
    months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
                          '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
                          '11': "nov", '12': "dec"}
    if(months_with_zeros == True):
        months_num_to_name = {'01': "jan", '02': "feb",'03': "mar", '04': 'apr', '05': "may", \
                      '06': "jun", '07': "jul", '08': "aug", '09': "sep", '10': "oct", \
                      '11': "nov", '12': "dec"}
            
    months = bidict(months_num_to_name)
    
    ## Takes dates in format    '[d/dd, m/mm, yyyy]'    returns days list in format    '[d/dd-month_name-yyyy]'
    def getDaysBetweenDates(start_date_conv, end_date_conv):
    
        def dateRange(date1, date2):
            for n in range(int ((date2 - date1).days)+1):
                yield date1 + datetime.timedelta(n)
 
        start_dt = datetime.date(int(start_date_conv[2]), int(start_date_conv[1]), int(start_date_conv[0]))
        end_dt = datetime.date(int(end_date_conv[2]), int(end_date_conv[1]), int(end_date_conv[0]))
        
        days_list = []
        
        for dt in dateRange(start_dt, end_dt):
            days_list.append(dt.strftime("%d-%b-%Y").lower())
    
        return days_list
    
    
    ## Takes dates in format    'd/dd-m/mm-yyyy'    returns months list in format    '[month_name-yyyy]'
    def getMonthsBetweenDates(start_date, end_date):
        days_list = getDaysBetweenDates(start_date, end_date)
        months_list = np.array([x[3:] for x in days_list])
        _, idx = np.unique(months_list, return_index = True)
        months_list = months_list[np.sort(idx)]
        return months_list
    
    months_list = getMonthsBetweenDates(start_date_conv, end_date_conv)
    
    if(month_names == False):
        months_list = [months.inv[month.split("-")[0]] + "-" + month.split("-")[1] for month in months_list]
    
    
    return months_list




# Checks if start_date precedes end_date and returns 1 if true 0 if false
def checkDatesPrecedence2(start_date, end_date):
    
    months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
                          '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
                          '11': "nov", '12': "dec"}
    months = bidict(months_num_to_name)
    
    start_date_conv = ["1", months.inv[start_date.split("-")[0]], start_date.split("-")[1]]
    end_date_conv = ["1", months.inv[end_date.split("-")[0]], end_date.split("-")[1]]

    try:
        ## Takes a date given in    'year = yyyy, month = m/mm, day = d/dd'    format
        start = datetime.datetime(year=int(start_date_conv[2]),month=int(start_date_conv[1]),day=int(start_date_conv[0]))
        end = datetime.datetime(year=int(end_date_conv[2]),month=int(end_date_conv[1]),day=int(end_date_conv[0]))
        if(start < end):
            return 1
        else:
            return 0
    except ValueError:
        raise ValueError("Incorrect date given.")    




# takes a dict_of_unified_months_dictionaries and returns a vector with earliest month and latest month
def findDateRange(months_dictionary, months_list):
    begin = ""
    end = ""

    
    present_months_list = []
    
    
    for month in months_list:
        if(bool(months_dictionary[month]) != 0):
            present_months_list.append(month)
        
    present_months_list = sorted(present_months_list, key = functools.cmp_to_key(checkDatesPrecedence2))
    
    begin = present_months_list[0]
    end = present_months_list[-1]

    return [begin, end]

def findDateRange2(months_dictionaries):
    
    begin = ""
    end = ""

    
    months_dictionaries_list = list(months_dictionaries.keys())

    
    months_list = [[s for s in month_dictionary_name.split(" ") if "-" in s][0] for month_dictionary_name in months_dictionaries_list]
    months_list = sorted(months_list, key = functools.cmp_to_key(checkDatesPrecedence2))
    
    begin = months_list[0]
    end = months_list[-1]

    return [begin, end]

