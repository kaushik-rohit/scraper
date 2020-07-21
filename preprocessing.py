#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 14:40:50 2018


"""

import os
import pandas as pd
from bidict import bidict
import numpy as np
import time
import queue
import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
from collections import defaultdict
import pickle

from utilities.date_operations import getMonths
from utilities.utilities import ensureDir, convertTime
from utilities.process_parallel import processParallel
from utilities.process_parallelMP import processParallelMP

from utilities.utilities import timeIt, trace_unhandled_exceptions

nltk.download('stopwords')
nltk.download('punkt')

PATH = os.path.abspath(os.path.dirname(__file__))
PHRASES_PATH = PATH + "/Calculations Data/Phrases/"



def getFiles(directory, start_date = None, end_date = None):
    """
    Takes a directory in    'string'    format, start_date and end_date in 'd/dd-month_name-yyyy'    or    'month_name-yyyy'    or    'yyyy' format
    Returns a list of paths in format    '[file_dir, ..., file_dir]'    to all files of '.csv' format in this directory and subdirectories between those dates.
    If one of the dates is not supplied it finds all .csv files in the directory and subdirectories.
    """
    if(start_date is not None and end_date is not None):
        months_list = getMonths(start_date, end_date)
    
    
    files_list = []
    
    if(os.path.exists(directory) == 0):
        raise Exception("The given directory does not exist.")
    
    for root, dirs, files in os.walk(directory):  
        for filename in files:
            if(filename[-3:]=="csv"):
                if(start_date is not None and end_date is not None):
                    if( len((set(filename.split(" ")) & set(months_list))) == 1 ):
                        files_list.append(os.path.join(root, filename))
                else:
                    files_list.append(os.path.join(root, filename))

    return files_list




def SourceNoToNameDict():
    """
    Opens sources.csv file (located in the same directory as this file) and extracts information about SourceId and Source Name pairs. 
    Created bidict with these pairs.
    """


    source_no_to_name_dictionary = pd.read_csv(PATH + "/sources.csv")

    source_no_to_name_dictionary = dict(zip(source_no_to_name_dictionary["Source Name"], source_no_to_name_dictionary["SourceId"]))

    source_no_to_name_dictionary = bidict(source_no_to_name_dictionary)
 
        
    return source_no_to_name_dictionary

### multiprocessing Pool class handles this function in parallel but does not raise exceptions ! This decorator handles that.
@trace_unhandled_exceptions
def preprocessFile(self, file_path):
    """
    Standardizes the dates and does standard preprocessing (removes stopwords etc.)
    """

    print("Preprocessing: " + file_path)

    months_dictionary = {"January": "jan", "February": "feb", "March": "mar", "April": "apr", \
                         "May": "may", "June": "jun", "July": "jul", "August": "aug", "September": "sep", "October": "oct", "November": "nov", "December": "dec"}
 
    
    months = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}  
    year_start = 1990
    year_end = 2025
    years = [str(year) for year in range(year_start, year_end)]
    months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
                              '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
                              '11': "nov", '12': "dec"}
    months_num_to_name = bidict(months_num_to_name)
    
    
    
    def NexisDateToStandard(nexis_date, months, years):
        

        nexis_date = nexis_date.replace(",", "")
        nexis_date = nexis_date.replace("\n", " ")
        nexis_date = nexis_date.replace(";", "")

        ### Previously returned nan may not be working correctly in every case. 
        ### Must be before if("Edition" ...) below as these dates contain "Edition" also
        if( "juillet" in set(nexis_date.split(" "))):
            nexis_date_temp = nexis_date.split(" ")
            month = "jul"
            day = nexis_date_temp[0]
            year = nexis_date_temp[2]
            
            ### there should not be a normal month name here
            if( len(set(nexis_date.split(" ")).intersection(months)) != 0 ):
                raise Exception("Found month other than juillet on: " + nexis_date)

            
            if(day not in set( [str(i) for i in range(32)[1:]]) ):
                raise Exception("Day is incorrect on " + nexis_date + "\n" + day)

                
            if(year not in years):
                raise Exception("Year is incorrect on " + nexis_date + "\n" + year)

            print("juillet found")
            return day + "-" + month + "-" + year

        
        if( "Edition" in set(nexis_date.split(" "))):
            nexis_date_temp = nexis_date.split(" ")
            month = nexis_date_temp[0]
            month = months_dictionary[month]
            day = nexis_date_temp[1]
            year = nexis_date_temp[2]
            
            if( len(set(nexis_date.split(" ")).intersection(months)) != 1 ):
                raise Exception("There are more than two months on " + nexis_date)

            
            if(day not in set( [str(i) for i in range(32)[1:]]) ):
                raise Exception("Day is incorrect on " + nexis_date + "\n" + day)

                
            if(year not in years):
                raise Exception("Year is incorrect on " + nexis_date + "\n" + year)

            
            return day + "-" + month + "-" + year
            
            

        
        if(len(nexis_date.split("-")) > 2):
            
            nexis_date = nexis_date.split("-")
            year = nexis_date[0].split(" ")[-1]
            month = nexis_date[1]
            day = nexis_date[0].split(" ")[0]
            day = set(nexis_date).intersection(set( [str(i) for i in range(32)[1:]] ).union(set(["01", "02", "03", "04", "05", "06", "07", "08", "09"])))
            if(len(day) != 1):
                print(day)
                raise Exception("Day is incorrect on " + " ".join(nexis_date))
            day = str(day.pop())
            if(day[0] == "0"):
                day = day[1:]
            
            
            month = set([month]).intersection(set(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]))
      
            if(len(month) != 1):
                print(month)
                raise Exception("Month is incorrect on " + " ".join(nexis_date))
            
            month = month.pop()
            if(month[0] == "0"):
                month = month[1]
                
            month = months_num_to_name[month]
            
            year = set(nexis_date).intersection(set(years))
            if(len(year) != 1):
                print(year)
                raise Exception("Year is incorrect on " + " ".join(nexis_date))
            year = year.pop()
            
            if(year not in years):
                raise Exception("Year is incorrect on " + " ".join(nexis_date) + "\n" + year)
            
            return day + "-" + month + "-" + year

        
        
        
        ### Source 8010
        source_number = file_path.replace(".", " ").split(" ")[-2]
        
        if(source_number == "8010" and ("-" in nexis_date.split(" ") or "/" in nexis_date.split(" "))): ## for 8010 dates are given in periods i.e. 1 April - 7 April
            
            nexis_date = nexis_date.split(" ")
            
            days_8010 = [nexis_date[1], nexis_date[5]]
            months_8010 = [nexis_date[0], nexis_date[4]]
            years_8010 = [nexis_date[2], nexis_date[6]]
            sign = nexis_date[3]
            
            if( len(set(days_8010).union(set( [str(i) for i in range(32)[1:]] ))) == 31 and len(set(months_8010).union(set(months))) == 12 and len(set(years_8010).union(set(years))) == len(years) and sign in ["-", "/"]):
                day = days_8010[0]
                month = months_8010[0]
                month = months_dictionary[month]
                year = years_8010[0]
                
            else:
                print(nexis_date)
                raise Exception("Date is incorrect on " + " ".join(nexis_date))        
        
            return day + "-" + month + "-" + year
        
        
        
        
        
        
        ### Default

        nexis_date = nexis_date.split(" ")
        
        

        day = set(nexis_date).intersection(    set( [str(i) for i in range(32)[1:]] ).union(set(["01", "02", "03", "04", "05", "06", "07", "08", "09"]))    )


        if((" ".join(nexis_date) == "November 4 2001 November 10 2001" and source_number == "138794") or \
           (" ".join(nexis_date) == "April 26 2002 Friday April 27 2002 Saturday" and source_number == "8200") or \
           (" ".join(nexis_date) == "October 7 2005 Friday October 8 2005 Saturday October 11 2005 Tuesday" and source_number == "145253")):
            day = np.array(list([int(i) for i in day])).min()
            day = set([str(day)])
            

        if(len(day) != 1):
            print(day)
            raise Exception("Day is incorrect on " + " ".join(nexis_date))
            
        day = day.pop()
    
        if(day[0] == "0"):
            day = day[1:]
                
        
        
        month = set(nexis_date).intersection(months)

        if(len(month) != 1):
            print(month)
            raise Exception("Month is incorrect on " + " ".join(nexis_date))
            
        month = month.pop()
        
        if(month not in months):
            raise Exception("Month is incorrect on " + " ".join(nexis_date) + "\n" + month)
                   


        year = set(nexis_date).intersection(set(years))
        if(len(year) != 1):
            print(year)
            raise Exception("Year is incorrect on " + " ".join(nexis_date))
        year = year.pop()
        
        if(year not in years):
            raise Exception("Year is incorrect on " + " ".join(nexis_date) + "\n" + year)          
        

#        print("Set " + " ".join(nexis_date) + " to " + day + "-" + months_dictionary[month] + "-" + year)
        return day + "-" + months_dictionary[month] + "-" + year
    
    

    if(file_path[-18:-4] == "no transcripts"): ## preprocess no transcript files
        pass
        
    else: ## preprocess transcript files




        if("BBC" in set(file_path.split("/"))):
            database = "BBC"
        elif("Nexis" in set(file_path.split("/"))):
            database = "Nexis"
        elif("Factiva" in set(file_path.split("/"))):
            database = "Factiva"
            
            
        preprocessed_file_path = file_path.replace("Raw Data", "Preprocessed Data")
        file_name = preprocessed_file_path.split("/")[-1]
        preprocessed_file_path = "/".join(preprocessed_file_path.split("/")[:-1]) + "/"
        
        


        if(os.path.isfile(preprocessed_file_path + file_name) == True):
            if(self.verbose >= 3):
                with self.locks_dict["print_lock"]:
                    print("File: " + preprocessed_file_path + file_name + " has already been preprocessed.")
            return
        
       


        ensureDir(preprocessed_file_path)
        file = pd.read_csv(file_path)


        source_no = file_path.split(" ")[-1].split(".")[0]


        if(database == "Factiva"):
            source = source_no.replace("+", " ")
        else:
            source = SourceNoToNameDict().inv[source_no]
         
 
    
        
        for row in range(file.shape[0]):
            file.at[row, "Source"] = source


#        file = file.drop(["Program Name"], axis = 1)
        file = file.drop(["Unnamed: 0"], axis = 1)


        file1 = file

        if("Program Transcript" in set(file1.columns)):
            file1.rename(columns={"Program Transcript": "Transcript"}, inplace=True)        
        
        file1["Raw Transcript"] = file1["Transcript"]
            

        for row in range(file1.shape[0]):
            


            if(pd.isnull(file1["Transcript"][row])):
                continue
            
            
            
            
            if(database == "Nexis"):
                file1.at[row, "Date"] = NexisDateToStandard(file1.at[row, "Date"], months, years)
            
            #To remove stop words
            stopW = stopwords.words('english')
            
            #To stem
            ps = PorterStemmer()

            #Tokenize
            
            clean_transcript = file1["Transcript"][row].lower()
            clean_transcript = word_tokenize(clean_transcript)
            #Remove digits
            clean_transcript = [i for i in clean_transcript if not re.match(r'\d+', i)]
            #Remove Stopwords and single characters
            clean_transcript = [i for i in clean_transcript if i not in stopW and len(i) > 1]
            #Stemming
            clean_transcript = [ps.stem(word) for word in clean_transcript]

                    
            #Convert back to a string
            clean_transcript = " ".join(clean_transcript)
            file1.at[row, "Transcript"] = clean_transcript
              
                    
        file1 = file1.replace("", np.nan)
        file1 = file1.dropna(subset = ["Transcript"])  
        

        file1.to_csv(preprocessed_file_path + file_name, index = False)

    
@trace_unhandled_exceptions
def markCopies(self, file_path):
    """
    Marks duplicate articles. They are very similar although not identical so their detection is a nontrivial task. 
    """
    
    print("Marking Copies: " + file_path)
    
    if(file_path[-18:-4] == "no transcripts"): ## preprocess no transcript files
        pass

        
    else: ## preprocess transcript files

        preprocessed_file_path = file_path.replace("Preprocessed Data", "Preprocessed Data No Copies")
        file_name = preprocessed_file_path.split("/")[-1]
        preprocessed_file_path = "/".join(preprocessed_file_path.split("/")[:-1]) + "/"
        

        
        if(os.path.isfile(preprocessed_file_path + file_name) == True):
            if(self.verbose >= 3):
                with self.locks_dict["print_lock"]:
                    print("File: " + preprocessed_file_path + file_name + " has already been preprocessed.")
            return
        
        with self.locks_dict["data_lock"]:
            ensureDir(preprocessed_file_path)

        

        file = pd.read_csv(file_path)

        # we do not need to remove any copied articles for BBC shows
        if("BBC" in preprocessed_file_path):
            file.to_csv(preprocessed_file_path + file_name, index = False)
            return



        is_copy = np.zeros_like(file["Date"], dtype = np.bool)
        group = np.zeros_like(file["Date"], dtype = np.uint32)

        
        group_no = 1
        
        for row in range(file.shape[0]):
            
            if(is_copy[row] == True):
                continue
            
            group[row] = group_no
            
            row_transcript = file.at[row, "Transcript"].split(" ")
            
            for row_below in range(row+1,file.shape[0]):
                
                
                transcript_below = file.at[row_below, "Transcript"].split(" ")
                
                len_diff = abs(len(row_transcript) - len(transcript_below))
                
                no_words_to_check = 30
                
                
                if(len_diff < 200):
                    if(len(row_transcript) > no_words_to_check and len(transcript_below) > no_words_to_check):
                        same = 0
                        for i in range(no_words_to_check):
                            if(row_transcript[i] == transcript_below[i]):
                                same += 1
                        if(same == no_words_to_check):
                            is_copy[row_below] = True
                            group[row_below] = group_no
                            
                    else:
                        if(transcript_below == row_transcript):
                            is_copy[row_below] = True
                            group[row_below] = group_no
                            
            group_no += 1

        file["Is Copy"] = is_copy
        file["Group"] = group
        
        
        
        print("Marked " + str(sum(is_copy)) + " copies.")
        file.to_csv(preprocessed_file_path + file_name, index = False)

@trace_unhandled_exceptions
def calculateFullNgrams(self, file_path, ngram, topic = ""):
    """
    Transforms a file to a bag-of-ngrams dictionary with ngram counts.
    """
    ngramU = ngram[0].upper() + ngram[1:]
    
    ### check if should return here
    if(file_path[-18:-4] == "no transcripts"): ## preprocess no transcript files
        pass
        
    else:
        if(os.path.isfile(PHRASES_PATH + "/" + ngramU + "/" + (topic + "/", "")[topic == ""] + file_path.split("/")[-1] + " " + ngramU + "Full.pkl") == 1):
            if(self.verbose >= 3):
                with self.locks_dict["print_lock"]:
                    print(PHRASES_PATH + "/" + ngramU + "/" + (topic + "/", "")[topic == ""] + file_path.split("/")[-1] + " " + ngramU + "Full.pkl has been found.")
            return
        

        file = pd.read_csv(file_path).fillna('')
 
    
        if(topic != ""):
            try:
                file = file.loc[file["Topic"] == int(topic), :]
            except KeyError:
                file = file.loc[file["topic"] == int(topic), :]

            file.reset_index(inplace = True)

        if(file.shape[0] == 0):
            return

        if("Sentence Preprocessed Transcripts" in set(file.columns)):
            file.rename(columns={"Sentence Preprocessed Transcripts": "Transcript"}, inplace=True) 

        dictionaryN = defaultdict(int)
        
        for row in range(file.shape[0]):  
            
            if(file["Transcript"][row] == ""):
                with self.locks_dict["print_lock"]:
                    print("Empty transcript in file " + file_path + " in row " + str(row))
                continue


            clean_transcript = file.at[row, "Transcript"].split(" ")
            no_words = len(clean_transcript)
            
            if(ngram == "unigrams"):
                for word in range(no_words):
                    tempN = clean_transcript[word]
                    dictionaryN[tempN] += 1                    
            elif(ngram == "bigrams"):
                for word in range(no_words - 1):
                    tempN = clean_transcript[word] + '.' + clean_transcript[word+1]
                    dictionaryN[tempN] += 1
            elif(ngram == "trigrams"):
                for word in range(no_words - 2):
                    tempN = clean_transcript[word] + '.' + clean_transcript[word+1] + '.' + clean_transcript[word+2]
                    dictionaryN[tempN] += 1                        

            

    
        with self.locks_dict["data_lock"]:
            ensureDir(PHRASES_PATH + ngramU + "/" + (topic + "/", "")[topic == ""])
        Ngrams_file = open(PHRASES_PATH + "/" + ngramU + "/" + (topic + "/", "")[topic == ""] + file_path.split("/")[-1] + " " + ngramU + "Full.pkl", 'wb')
        pickle.dump({file_path.split("/")[-1]: dictionaryN}, Ngrams_file)
        Ngrams_file.close()



@timeIt
def preprocessFilesDirectory(directory, start_date, end_date):
    """
    Applies processFile to all files (falling within dates range) in a directory.
    """
    print("Preprocessing.")
    files_list = getFiles(directory, start_date, end_date)

    parallel_processor = processParallelMP(preprocessFile, files_list, no_threads=8, ordered_results = False)
    parallel_processor.process()



@timeIt
def markCopiesDirectory(preprocessed_directory, start_date, end_date):
    """
    Applies markCopies to all files (falling within dates range) in a directory
    """
    print("Marking copies.")
    preprocessed_files_list = getFiles(preprocessed_directory, start_date, end_date)
    
    parallel_processor = processParallelMP(markCopies, preprocessed_files_list, ordered_results = False)
    parallel_processor.process()


 
@timeIt
def makeFullNgramsDirectory(preprocessed_directory_no_copies, start_date, end_date, ngram, topic = ""):
    """
    Applies calculateFullNgrams to all files (falling within dates range) in a directory
    """
    print("Calculating Full Ngrams.")
    preprocessed_files_list = getFiles(preprocessed_directory_no_copies, start_date, end_date)

    parallel_processor = processParallelMP(calculateFullNgrams, preprocessed_files_list, {"ngram": ngram, "topic": topic})
    parallel_processor.process()



def loadFullNgrams(dict_of_source_month_all_bigrams_dictionaries, file_paths_list, ngram, topic = ""):

    ngramU = ngram[0].upper() + ngram[1:]    
    
    for file_path in file_paths_list:
        
        if(os.path.isfile(PHRASES_PATH + "/" + ngramU + "/" + (topic + "/", "")[topic == ""] + file_path.split("/")[-1] + " " + ngramU + "Full.pkl") == 1):

            fileopen = open(PHRASES_PATH + "/" + ngramU + "/" + (topic + "/", "")[topic == ""] + file_path.split("/")[-1] + " " + ngramU + "Full.pkl", 'rb')
            dictionaryN = pickle.load(fileopen)
            dict_of_source_month_all_bigrams_dictionaries.update(dictionaryN)


######################################################################################################################
######################################################################################################################
######################################################################################################################
    
#    EXECUTABLE SECTION

######################################################################################################################
######################################################################################################################
######################################################################################################################



if(__name__ == "__main__"):

    start_date = "jan-1999"
    end_date = "jan-2025"
    
    ######## Preprocess files
    directory = "./Raw Data/"
#    preprocessFilesDirectory(directory, start_date, end_date)
    

    ######## Mark copies
    preprocessed_directory = "./Preprocessed Data/"
#    markCopiesDirectory(preprocessed_directory, start_date, end_date)
    
    
    ######## Make Full Bigrams
#    preprocessed_directory_no_copies = "./Preprocessed Data No Copies/"
    preprocessed_directory_no_copies = "./Preprocessed Data With Topics/"
    #makeFullNgramsDirectory(preprocessed_directory_no_copies, start_date, end_date, ngram = "unigrams")
#    makeFullNgramsDirectory(preprocessed_directory_no_copies, start_date, end_date, ngram = "bigrams")
    #makeFullNgramsDirectory(preprocessed_directory_no_copies, start_date, end_date, ngram = "trigrams")
    
    chosen_categories_dict = {"02": "Agriculture, animals, food and rural affairs", \
                 "03": "Asylum, immigration and nationality", "04": "Business, industry and consumers", "05": "Communities and families", \
                 "06": "Crime, civil law, justice and rights", "07": "Culture, media and sport", "08": "Defence", "09": "Economy and finance", \
                 "10": "Education", "11": "Employment and training", "12": "Energy and environment", "13": "European Union", \
                 "14": "Health services and medicine", "15": "Housing and planning", "16": "International affairs", \
                 "17": "Parliament, government and politics", "18": "Science and technology", "19": "Social security and pensions", \
                 "20": "Social services", "21": "Transport"}
    
    for topic in chosen_categories_dict.keys():
        makeFullNgramsDirectory(preprocessed_directory_no_copies, start_date, end_date, ngram = "bigrams", topic = topic)








