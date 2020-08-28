"""
    This file contains the main method and the loop responsible for running the crawler.
"""

# Library imports
from pymongo import MongoClient
import pymongo
import time
import concurrent.futures
import os

# Local file imports
from logger import logger
from cfg import config
from crawler import crawl
from dbManager import insertRootURL, insertNewURLs, returnExpiredLinks, returnUncrawledLinks

if __name__ == '__main__':
    logger.info('Hi! Lets begin.')
    logger.debug('Started the Web Crawler')

    # define all the global variables
    maxSevSelDelay = 1                          # time for which connection attempt will be made (default=30s), changed to 1s
    no_of_threads = config['No_of_threads']
    database_name = config['database_name']
    collection_name = config['collection_name']
    sleep_time = config['sleep_timer']
    download_dir_path = config['download_dir_path']

    logger.debug('Verifying if the file download path is valid')
    if os.path.isdir(download_dir_path) == False:
        logger.critical(
            'The download directory path given is invalid. Please check if the directory exists and try again.')
        exit()
    logger.debug('Directory path valid.')

    logger.debug('Attempting to connect to MongoDB')
    try:
        client = MongoClient(config['connection_uri'],
                             serverSelectionTimeoutMS=maxSevSelDelay)
        client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.critical(
            'Connection to MongoDB failed. Either the URI is faulty or the database server is down. Please retry.')
        exit()
    else:
        logger.debug('Connection to the database is successful.')

    db = client[database_name]
    collectionObject = db[collection_name]
    logger.debug('collectionObject created')

    logger.debug("Inserting the root URL({}) into the database".format(
        config['root_url']))
    insertRootURL(collectionObject)
    logger.debug('Root URL inserted')

    # The infinite loop
    while True:
        logger.debug('Starting cycle')
        
        uncrawledLinks = returnUncrawledLinks(collectionObject)     # Getting the new/uncrawled links
        expiredLinks = returnExpiredLinks(collectionObject)         # Getting the links not crawled for 'Recrawl_time_limit_hours'
        uncrawledLinks.extend(expiredLinks)

        if not uncrawledLinks:                                      # Checking if all links have been already crawled
            logger.info('All links crawled')
            logger.debug('Cycle completed.')
            time.sleep(sleep_time)
            continue

        distributorList = []
        for i in range(no_of_threads):
            distributorList.append([])
        for i in range(len(uncrawledLinks)):
            distributorList[(i % no_of_threads)].append(uncrawledLinks[i])  # Dividing the links among the threads
        
        return_values = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=no_of_threads) as executor:
            futures = []
            for i in range(no_of_threads):                          # Running multiple instances of crawl method concurrently
                futures.append(executor.submit(
                    crawl, collectionObject=collectionObject, toCrawlLinkList=distributorList[i]))
            for future in concurrent.futures.as_completed(futures): # Update results as and when the threads have completed execution
                return_values.extend(future.result())

        for urlList in return_values:
            if not urlList:
                continue
            sourceURL = urlList[0]
            del urlList[0]
            dbStatus = insertNewURLs(collectionObject, urlList, sourceURL)  # Inserting the new URLs into the database
            if dbStatus == -1:
                logger.info('Maximum limit reached')
                break

        logger.debug('Cycle completed.')
        
        time.sleep(sleep_time)
