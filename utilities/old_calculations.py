#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 18:29:12 2019

"""

import numpy as np
import pandas as pd
import time
import os
import pickle
import defaultdict

from preprocessing import SourceNoToNameDict, getFiles, PATH

from date_operations import getMonths, findDateRange2
from utilities import ensureDir, convertTime


def calculatePartialBigrams(file_path, file_paths_queue, data, params, locks_dict, verbose, exit_flag):
    
    
    PHRASES_PATH = PATH + "/Preprocessed Data/Phrases/"
    weights_file_name = data[0]

    if(file_path[-18:-4] == "no transcripts"): ## preprocess no transcript files
        pass
        
    else:
        if(os.path.isfile(PHRASES_PATH + "/Bigrams/" + file_path.split("/")[-1] + " BigramsPartial_" + weights_file_name + "_.pkl") == 1):
            if(verbose >= 3):
                locks_dict["print_lock"].acquire()
                print(PHRASES_PATH + "/Bigrams/" + file_path.split("/")[-1] + " BigramsPartial_" + weights_file_name + "_.pkl has been found.")
                locks_dict["print_lock"].release()
            
            return
        
        fileopen = open(PHRASES_PATH + "/Bigrams/" + file_path.split("/")[-1] + " BigramsFull.pkl", 'rb')
        dictionaryBifull = pickle.load(fileopen)
 
        political_bigrams = pd.read_csv("./" + weights_file_name).fillna('')["Unnamed: 0"]
        political_bigrams = list(political_bigrams)
        
        
        keys = list(dictionaryBifull[list(dictionaryBifull.keys())[0]].keys())
        
        selected_political_bigrams = list(set(political_bigrams).intersection(keys))

        
        dictionaryBipartial = { political_bigram: dictionaryBifull[list(dictionaryBifull.keys())[0]][political_bigram] for political_bigram in selected_political_bigrams }
        dictionaryBipartial = defaultdict(int, dictionaryBipartial)

        
        ensureDir(PHRASES_PATH + "Bigrams/")
        bigrams_file = open(PHRASES_PATH + "/Bigrams/" + file_path.split("/")[-1] + " BigramsPartial_" + weights_file_name + "_.pkl", 'wb')

        pickle.dump({file_path.split("/")[-1]: dictionaryBipartial}, bigrams_file)
        bigrams_file.close()





def loadPartialBigrams(dict_of_months_dictionaries, weights_file_name, file_paths_list):
    
    PHRASES_PATH = PATH + "/Preprocessed Data/Phrases/"
    
    for file_path in file_paths_list:
        
        if(os.path.isfile(PHRASES_PATH + "/Bigrams/" + file_path.split("/")[-1] + " BigramsPartial_" + weights_file_name + "_.pkl") == 1):

            fileopen = open(PHRASES_PATH + "/Bigrams/" + file_path.split("/")[-1] + " BigramsPartial_" + weights_file_name + "_.pkl", 'rb')
            dictionaryBi = pickle.load(fileopen)
            dict_of_months_dictionaries.update(dictionaryBi)

# to complete
def getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict, calculate_period_score = 0, normalize_by_political_bigrams = 1):
    months_scores_dictionary = {}


    #calculates the score for every dictionary in dict_of_months_dictionaries
    if(calculate_period_score == 0):

        for month_dictionary_name in list(dict_of_months_dictionaries.keys()):
            month_dictionary = dict_of_months_dictionaries[month_dictionary_name]
            month_dictionary_score = 0.0
            total_political_bigrams_count = 0
            denominator = 0
            
            try:
                fileopen = open("./Preprocessed Data/Phrases/" + "/Bigrams/" + month_dictionary_name + " BigramsFull.pkl", 'rb')
                dictionaryBiFull = pickle.load(fileopen)
                dictionaryBiFull = dictionaryBiFull[list(dictionaryBiFull.keys())[0]]
            except Exception:
                raise("Could not open: " + "./Preprocessed Data/Phrases/" + "/Bigrams/" + month_dictionary_name + " BigramsFull.pkl")

            total_bigrams_count = sum(dictionaryBiFull.values())

            if(isinstance(list(political_bigrams_dict.values())[0], float) == True): # we process differently when we have one and two weights for a bigram

                for political_bigram, bigram_count in month_dictionary.items():
#                    if(np.abs(political_bigrams_dict[political_bigram] - 0.5) > 0.1):
                    total_political_bigrams_count += bigram_count
                    month_dictionary_score += political_bigrams_dict[political_bigram]*bigram_count


            else:

#                for political_bigram, bigram_count in month_dictionary.items():
#                    total_political_bigrams_count += bigram_count
                    
                total_political_bigrams_count = sum(month_dictionary.values())

                
#                for political_bigram in political_bigrams_dict.keys():
#                    try:
#                        bigram_count = month_dictionary[political_bigram]
#                    except Exception:
#                        bigram_count = 0
#
#
#                    if(bigram_count != 0):
#                        if(normalize_by_political_bigrams == 1):
#                            frequency = bigram_count/total_political_bigrams_count
#                        else:
#                            frequency = bigram_count/total_bigrams_count
#                    else:
#                        frequency = 0
#                        
#                    if(frequency != 0):
#                        month_dictionary_score += political_bigrams_dict[political_bigram][1]*(frequency - political_bigrams_dict[political_bigram][0])
#                        denominator +=  political_bigrams_dict[political_bigram][1]*political_bigrams_dict[political_bigram][1]
#                
                political_bigrams_dict_new = {}
                
                
                for political_bigram in political_bigrams_dict.keys():
                    try:
                        bigram_count = month_dictionary[political_bigram]
                    except Exception:
                        bigram_count = 0


                    if(bigram_count != 0):
                        if(normalize_by_political_bigrams == 1):
                            frequency = bigram_count/total_political_bigrams_count
                        else:
                            frequency = bigram_count/total_bigrams_count
                    else:
                        frequency = 0
                        
                    if(frequency <= 0.01 and frequency != 0):
                        political_bigrams_dict_new.update({political_bigram: bigram_count})

                
                
                total_political_bigrams_count_new = sum(political_bigrams_dict_new.values())
                
                
                
                for political_bigram_new in political_bigrams_dict_new.keys():

                    bigram_count = political_bigrams_dict_new[political_bigram_new]
                    frequency = bigram_count/total_political_bigrams_count_new

                    month_dictionary_score += political_bigrams_dict[political_bigram_new][1]*(frequency - political_bigrams_dict[political_bigram_new][0])
                    denominator +=  political_bigrams_dict[political_bigram_new][1]*political_bigrams_dict[political_bigram_new][1]
                            
                
                
                
                if(denominator != 0):
                    month_dictionary_score /= denominator
                else:
                    month_dictionary_score = -3

            if(isinstance(list(political_bigrams_dict.values())[0], float) == True):                                            
                if(total_political_bigrams_count != 0):
                    months_scores_dictionary.update({month_dictionary_name: {"score": month_dictionary_score/total_political_bigrams_count, "total_bigrams_count": total_bigrams_count, "total_political_bigrams_count": total_political_bigrams_count}})
                else: 
                    months_scores_dictionary.update({month_dictionary_name: {"score": -1, "total_bigrams_count": 0, "total_political_bigrams_count": 0}})
            else:
                months_scores_dictionary.update({month_dictionary_name: {"score": month_dictionary_score, "total_bigrams_count": total_bigrams_count, "total_political_bigrams_count": total_political_bigrams_count}})
        
        
        absent_months = [month for month in months_list if month not in [[mth for mth in dictionary_name.split(" ") if "-" in mth][0] for dictionary_name in list(months_scores_dictionary.keys())]]
        
        template = list(months_scores_dictionary.keys())[0].split(" ")
        idx = [i for i, s in enumerate(template) if "-" in s][0]
        
        for month in absent_months:
            template[idx] = month
            months_scores_dictionary.update({" ".join(template): {"score": -2, "total_bigrams_count": 0, "total_political_bigrams_count": 0}})

    #for every source calculates the one score (collapsing over months) for all dictionaries in dict_of_months_dictionaries  
    if(calculate_period_score == 1):
        source_names = np.unique([month_dictionary_name.split(" ")[-1][:-4] for month_dictionary_name in list(dict_of_months_dictionaries.keys())])
        
        
        for source in source_names:
            
            names_of_dictionaries_from_source = [source_dictionary for source_dictionary in list(dict_of_months_dictionaries.keys()) if source in source_dictionary]

            period_score = 0.0
            period_political_bigrams_count = 0

            for month_dictionary_name in names_of_dictionaries_from_source:
                month_dictionary = dict_of_months_dictionaries[month_dictionary_name]

                for political_bigram, bigram_count in month_dictionary.items():
                    period_political_bigrams_count += bigram_count
                    if(isinstance(list(political_bigrams_dict.values())[0], float) == True): # we process differently when we have one and two weights for a bigram
                        period_score += political_bigrams_dict[political_bigram]*bigram_count
                    
                                                       
            if(period_political_bigrams_count != 0):
                if(normalize_by_political_bigrams == 1):
                    start, end = findDateRange2(dict_of_months_dictionaries)
                    months_scores_dictionary.update({start + " to " + end: {"score": period_score/period_political_bigrams_count, "total_bigrams_count": 0, "total_political_bigrams_count": 0}})
                if(normalize_by_political_bigrams == 0):
                    # to implement 
                    pass
            else: 
                months_scores_dictionary.update({month_dictionary_name: {"score": -1, "total_bigrams_count": 0, "total_political_bigrams_count": 0}})  
        
    
    
    return months_scores_dictionary


######## Calculate total scores


def getResults(start_date, end_date, calculate_period_score = 0):
    
    startTime = time.time()
    
    
    
    political_bigrams = pd.read_csv("./weight_1000.csv").fillna('')["Unnamed: 0"]
    political_bigrams_weights = pd.read_csv("./weight_1000.csv").fillna('')["x"]
    political_bigrams_dict = dict(zip(political_bigrams, political_bigrams_weights))



    political_bigrams2 = pd.read_csv("./G-S_model_bi_party.csv").fillna('')["Unnamed: 0"].to_frame()
    political_bigrams_weights2 = pd.read_csv("./G-S_model_bi_party.csv", usecols = ["V1", "V2"]).fillna('')
    political_bigrams2 = political_bigrams2["Unnamed: 0"].values.tolist()
    political_bigrams_weights2 = [list(i) for i in zip(political_bigrams_weights2["V1"].values.tolist(), political_bigrams_weights2["V2"].values.tolist())]
    political_bigrams_dict2 = dict(zip(political_bigrams2, political_bigrams_weights2))



    political_bigrams3 = pd.read_csv("./G-S_model_bi_conShare.csv").fillna('')["Unnamed: 0"].to_frame()
    political_bigrams_weights3 = pd.read_csv("./G-S_model_bi_conShare.csv", usecols = ["V1", "V2"]).fillna('')
    political_bigrams3 = political_bigrams3["Unnamed: 0"].values.tolist()
    political_bigrams_weights3 = [list(i) for i in zip(political_bigrams_weights3["V1"].values.tolist(), political_bigrams_weights3["V2"].values.tolist())]
    political_bigrams_dict3 = dict(zip(political_bigrams3, political_bigrams_weights3))
 


    political_bigrams4 = pd.read_csv("./G-S_model_bi_conShare2.csv").fillna('')["Unnamed: 0"].to_frame()
    political_bigrams_weights4 = pd.read_csv("./G-S_model_bi_conShare2.csv", usecols = ["V1", "V2"]).fillna('')
    political_bigrams4 = political_bigrams4["Unnamed: 0"].values.tolist()
    political_bigrams_weights4 = [list(i) for i in zip(political_bigrams_weights4["V1"].values.tolist(), political_bigrams_weights4["V2"].values.tolist())]
    political_bigrams_dict4 = dict(zip(political_bigrams4, political_bigrams_weights4))

    
    
    preprocessed_directory = "./Preprocessed Data No Copies/"
    nexis_sources = os.listdir(preprocessed_directory + "Nexis/")
    bbc_sources = os.listdir(preprocessed_directory + "BBC/")
    
    source_dir_list = []
    

    for source in nexis_sources:
        source_dir_list.append(preprocessed_directory + "Nexis/" + source + "/")
    for source in bbc_sources:
        source_dir_list.append(preprocessed_directory + "BBC/" + source + "/")  
    
    source_to_name = SourceNoToNameDict()

    results_dict = {}
    
    def getCounts(preprocessed_files_list, months_score_dictionary_name_list, calculate_period_score):
        counts_dictionary = {}

        if(calculate_period_score == 0):
            for file_path in preprocessed_files_list:
                file = pd.read_csv(file_path).fillna('')
                name = file_path.split("/")[-1]
                counts_dictionary[name] = file.shape[0]
        if(calculate_period_score == 1):
            total_count = 0
            for file_path in preprocessed_files_list:
                file = pd.read_csv(file_path).fillna('')
                total_count += file.shape[0]
            name = months_score_dictionary_name_list[0] 
            counts_dictionary[name] = total_count


        return counts_dictionary
    
 
    for directory in source_dir_list:

        print("Processing calculations for: " + directory)


        source = directory.split("/")[-2]
        preprocessed_files_list = getFiles(directory, start_date, end_date)
        months_list = getMonths(start_date, end_date)



        dict_of_months_dictionaries = {}
        weights_file_name = "weight_1000.csv"
        loadPartialBigrams(dict_of_months_dictionaries, weights_file_name, preprocessed_files_list)

#        summ = sum(dict_of_months_dictionaries[" apr-2013 138620.csv"].values())
#        yy = [(x, y/summ) for x,y in dict_of_months_dictionaries[" apr-2013 138620.csv"].items()]

#        pd.DataFrame( data = dict(yy), index = ["frequency = counts/total_political_bigrams"] ).transpose().to_csv("./ apr-2013 138620 political_bigram frequencies.csv") 
       

        # weight_1000.csv
        months_score_dictionary = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict, calculate_period_score, normalize_by_political_bigrams = 1)



        dict_of_months_dictionaries = {}
        weights_file_name = "G-S_model_bi_party.csv"
        loadPartialBigrams(dict_of_months_dictionaries, weights_file_name, preprocessed_files_list)

#        summ = sum(dict_of_months_dictionaries[" apr-2013 138620.csv"].values())
#        yy = [(x, y/summ) for x,y in dict_of_months_dictionaries[" apr-2013 138620.csv"].items()]
#
#        pd.DataFrame( data = dict(yy), index = ["frequency = counts/total_political_bigrams"] ).transpose().to_csv("./ apr-2013 138620 political_bigram frequencies GS.csv") 
       

#        time.sleep(99999)

        # G-S_model_bi_party.csv
        months_score_dictionary2 = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict2, calculate_period_score, normalize_by_political_bigrams = 1)
        months_score_dictionary3 = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict2, calculate_period_score, normalize_by_political_bigrams = 0)

        # G-S_model_bi_conShare.csv
        months_score_dictionary4 = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict3, calculate_period_score, normalize_by_political_bigrams = 1)
        months_score_dictionary5 = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict3, calculate_period_score, normalize_by_political_bigrams = 0)

        # G-S_model_bi_conShare2.csv
        months_score_dictionary6 = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict4, calculate_period_score, normalize_by_political_bigrams = 1)
        months_score_dictionary7 = getScoreDictionary(months_list, dict_of_months_dictionaries, political_bigrams_dict4, calculate_period_score, normalize_by_political_bigrams = 0)


        

        months_score_dictionary_name_list = list(months_score_dictionary.keys())



        counts_dictionary = getCounts(preprocessed_files_list, months_score_dictionary_name_list, calculate_period_score)




        for i in range(len(list(months_score_dictionary.keys()))):
            
            name = months_score_dictionary_name_list[i]
            
            try:
                temp = counts_dictionary[name]
            except Exception:
                counts_dictionary[name] = 0

            months_score_dictionary[source_to_name.inv[source] + " (" + source + ")" + ": " + name] = \
            [months_score_dictionary[name]["score"], months_score_dictionary[name]["total_bigrams_count"], months_score_dictionary.pop(name)["total_political_bigrams_count"], \
             months_score_dictionary2[name]["score"], months_score_dictionary2[name]["total_bigrams_count"], months_score_dictionary2.pop(name)["total_political_bigrams_count"], \
             months_score_dictionary3[name]["score"], months_score_dictionary3[name]["total_bigrams_count"], months_score_dictionary3.pop(name)["total_political_bigrams_count"], \
             months_score_dictionary4[name]["score"], months_score_dictionary4[name]["total_bigrams_count"], months_score_dictionary4.pop(name)["total_political_bigrams_count"], \
             months_score_dictionary5[name]["score"], months_score_dictionary5[name]["total_bigrams_count"], months_score_dictionary5.pop(name)["total_political_bigrams_count"], \
             months_score_dictionary6[name]["score"], months_score_dictionary6[name]["total_bigrams_count"], months_score_dictionary6.pop(name)["total_political_bigrams_count"], \
             months_score_dictionary7[name]["score"], months_score_dictionary7[name]["total_bigrams_count"], months_score_dictionary7.pop(name)["total_political_bigrams_count"], \
             counts_dictionary.pop(name)]
                                   

        
        results_dict.update(months_score_dictionary)


    
    file_names1 = ["weight_1000.csv"]
    file_names2 = ["G-S_model_bi_party.csv", "G-S_model_bi_conShare.csv", "G-S_model_bi_conShare2.csv"]
    file_names1_ext = [file_name for file_name in file_names1 for i in range(3)]
    file_names2_ext = [file_name for file_name in file_names2 for i in range(6)]
    
    file_names1_ext = ["Objectivity Value1 " + file_name + " normalize_political_bigrams = 1" for file_name in file_names1_ext]
    file_names2_ext = ["Objectivity Value2 " + file_name + " normalize_political_bigrams = 1" if idx%6 in [0, 1, 2] else "Objectivity Value2 " + file_name + " normalize_political_bigrams = 0" for idx, file_name in enumerate(file_names2_ext)]
    
    idx = file_names1_ext + file_names2_ext
    idx = [file_name + " Score" if idxx%3 == 0 else file_name + " total_bigrams_count" if idxx%3 == 1 else file_name + " total_political_bigrams_count" for idxx, file_name in enumerate(idx)]

        
        
    pd.DataFrame( data = results_dict, index = idx + ["Number of Articles"] ).transpose().to_csv("./results_.csv") 
        
        
        
        
    
    duration = convertTime(time.time() - startTime)
    print("\n")
    print("Calculating total scores took: " + str(duration[0]) + " days " + str(duration[1]) + " hours " + str(duration[2]) + " minutes " + str(duration[3])  + " seconds")
    
    
######################################################################################################################

######## Make Partial Bigrams

#data[0] = "G-S_model_bi_party.csv"
#preprocessed_directory = "./Preprocessed Data No Copies/"
#preprocessed_files_list = getFiles(preprocessed_directory, start_date, end_date)
#preprocessed_files_no_copies_queue = queue.Queue()
#preprocessed_files_no_copies_queue.queue = queue.deque(preprocessed_files_list)
#
#
#
#startTime = time.time()
#parallel_processor = processParallel(preprocessed_files_no_copies_queue, calculatePartialBigrams, params, data, no_threads=1, verbose=3)
#parallel_processor.process()
#duration = time.time() - startTime
#duration = convertTime(duration)
#
#print("\n")
#print("Making Partial Bigrams took: " + str(duration[0]) + " days " + str(duration[1]) + " hours " + str(duration[2]) + " minutes " + str(duration[3])  + " seconds")
#
#
#data[0] = "weight_1000.csv"
#preprocessed_directory = "./Preprocessed Data No Copies/"
#preprocessed_files_list = getFiles(preprocessed_directory, start_date, end_date)
#preprocessed_files_no_copies_queue = queue.Queue()
#preprocessed_files_no_copies_queue.queue = queue.deque(preprocessed_files_list)
#
#
#
#startTime = time.time()
#parallel_processor = processParallel(preprocessed_files_no_copies_queue, calculatePartialBigrams, params, data, no_threads=1, verbose=3)
#parallel_processor.process()
#duration = time.time() - startTime
#duration = convertTime(duration)
#
#print("\n")
#print("Making Partial Bigrams took: " + str(duration[0]) + " days " + str(duration[1]) + " hours " + str(duration[2]) + " minutes " + str(duration[3])  + " seconds")


######################################################################################################################

#getResults(start_date, end_date, calculate_period_score = 0)

