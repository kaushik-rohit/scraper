#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 17:37:58 2020

"""


import psycopg2
from db.config import config

from datetime import datetime


#from collections import defaultdict
#
#start_date = "jan-2018"
#end_date = "dec-2018"
#list_of_sources = ["106"]
#from utilities.date_operations import getMonths, getFullDates
#start_date, end_date = getFullDates(start_date, end_date, vector = 0)



def selectTranscriptsPreprocessed(start_date, end_date, list_of_sources):
    """ SELECT multiple transcripts from the transcripts table  """
    
    start_date = datetime.strptime(start_date, "%d-%b-%Y").date()
    end_date = datetime.strptime(end_date, "%d-%b-%Y").date()
    
    
    
    if(len(list_of_sources) == 1):
        sql_query = f"""select array_agg(preprocessedtranscript) from transcriptspreprocessed
        where transcriptid in (select transcriptid from transcriptsinfo
        where date between '{start_date}' and '{end_date}'
        and sourceid = '{list_of_sources[0]}')"""
    else:
        sql_query = f"""select array_agg(preprocessedtranscript) from transcriptspreprocessed
        where transcriptid in (select transcriptid from transcriptsinfo
        where date between '{start_date}' and '{end_date}'
        and sourceid in {tuple(list_of_sources)})"""
    
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(sql_query)
        rows = cur.fetchall()[0][0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        return rows
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
            
#rows = selectTranscriptsPreprocessed(start_date, end_date, list_of_sources)
#print(rows)


def selectSourceIDs(database = None):
    """ SELECT sourceids from the sources table  """
    if(database is None):
        sql_query = """select array_agg(sourceid) from sources"""
    elif(database in ["BBC", "Factiva", "Nexis"]):
        sql_query = f"""select array_agg(sourceid) from sources where databasename = '{database}'"""
    else:
        raise Exception("Incorrect database name. Choose from: BBC, Factiva, Nexis")
    
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(sql_query)
        rows = cur.fetchall()[0][0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        return rows
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def selectTranscriptsInfoQuery(start_date, end_date, list_of_sources):
    """ SELECT multiple transcripts from the transcripts table  """
    
    start_date = datetime.strptime(start_date, "%d-%b-%Y").date()
    end_date = datetime.strptime(end_date, "%d-%b-%Y").date()
    
    
    if(len(list_of_sources) == 1):
        sql_query = f"""select array_agg(query) from transcriptsinfo
    where transcriptid in (select transcriptid from transcriptsinfo
        where date between '{start_date}' and '{end_date}'
        and sourceid = '{list_of_sources[0]}')"""
    else:
        sql_query = f"""select array_agg(query) from transcriptsinfo
    where transcriptid in (select transcriptid from transcriptsinfo
        where date between '{start_date}' and '{end_date}'
        and sourceid in {tuple(list_of_sources)})"""
    
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(sql_query)
        rows = cur.fetchall()[0][0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        return rows
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



def selectUnavailableLinks(start_date, end_date, list_of_sources):
    """ SELECT unavailable transcripts from the transcriptsinfo table  """
    
    start_date = datetime.strptime(start_date, "%d-%b-%Y").date()
    end_date = datetime.strptime(end_date, "%d-%b-%Y").date()
    
    if(type(list_of_sources) is str):
        list_of_sources = [list_of_sources]
    
    if(len(list_of_sources) == 1):
        sql_query = f"""select transcriptid, unavailablelink, unavailablereason from transcriptsinfo
    where transcriptid in (select transcriptid from transcriptsinfo
        where date between '{start_date}' and '{end_date}'
        and sourceid = '{list_of_sources[0]}'
        and hastranscript = False
        and requestedfix = 0)"""
    else:
        sql_query = f"""select transcriptid, unavailablelink, unavailablereason from transcriptsinfo
    where transcriptid in (select transcriptid from transcriptsinfo
        where date between '{start_date}' and '{end_date}'
        and sourceid in {tuple(list_of_sources)}
        and hastranscript = False
        and requestedfix = 0)"""
    
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(sql_query)
        rows = cur.fetchall()
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        return rows
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
