#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 22:20:00 2020

@author: revist
"""
 
import psycopg2
from db.config import config


def insertIntoTranscriptsInfo(tuples_list):
    """ insert multiple transcripts into the transcripts table  """
    sql = """INSERT INTO transcriptsinfo(query, sourceid, date, time, duration, transcripttitle, 
    iscopy, transcriptgroup, hastranscript, unavailablelink, unavailablereason) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING transcriptid"""
    
    conn = None
    transcriptids = []
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        for t in tuples_list:           
            cur.execute(sql,t)
            tid = cur.fetchone()[0]
            transcriptids.append(tid)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
    return transcriptids


def insertIntoTranscriptsPreprocessed(tuples_list):
    """ insert multiple transcripts into the transcripts table  """
    sql = "INSERT INTO transcriptspreprocessed(transcriptid, preprocessedtranscript) VALUES(%s, %s)"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
            
        cur.executemany(sql,tuples_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertIntoTranscriptsRaw(tuples_list):
    """ insert multiple transcripts into the transcripts table  """
    sql = "INSERT INTO transcriptsraw(transcriptid, rawtranscript) VALUES(%s, %s)"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
            
        cur.executemany(sql,tuples_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insertIntoSources(tuples_list):
    """ insert multiple sources into the sources table  """
    sql = "INSERT INTO sources(sourceid, sourcename, databasename) VALUES(%s, %s, %s)"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,tuples_list)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
            
def insertIntoTopics(tuples_list):
    """ insert multiple transcript topic info into the topics table  """
    sql = """INSERT INTO topics(transcriptid, topic, cossim2, cossim3, cossim4, cossim5, 
                                cossim6, cossim7, cossim8, cossim9, cossim10, cossim11,
                                cossim12, cossim13, cossim14, cossim15, cossim16, cossim17,
                                cossim18, cossim19, cossim20, cossim21) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,tuples_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()           
            
            