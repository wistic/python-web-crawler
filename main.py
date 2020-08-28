from pymongo import MongoClient
import pymongo
import time
import concurrent.futures
import os

from logger import logger
from cfg import config
from crawler import crawl
from dbManager import insertRootURL, insertNewURLs, returnExpiredLinks, returnUncrawledLinks

if __name__ == '__main__':
    logger.info('Hi! Lets begin.')

    logger.debug('Started the Web Crawler')

    # define all the global variables
    maxSevSelDelay = 1
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

    # The infinite loop
    while True:
        logger.debug('Starting cycle')
        # Getting the new/uncrawled links
        uncrawledLinks = returnUncrawledLinks(collectionObject)
        # Getting the links not crawled for a specific amount of time (24 hrs in this case)
        expiredLinks = returnExpiredLinks(collectionObject)
        # Combining the lists
        uncrawledLinks.extend(expiredLinks)
        if not uncrawledLinks:
            logger.info('All links crawled')
            logger.debug('Cycle completed.')
            time.sleep(sleep_time)
            continue
        distributorList = []
        for i in range(no_of_threads):
            distributorList.append([])
        for i in range(len(uncrawledLinks)):
            distributorList[(i % no_of_threads)].append(uncrawledLinks[i])
        return_values = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=no_of_threads) as executor:
            futures = []
            for i in range(no_of_threads):
                futures.append(executor.submit(
                    crawl, collectionObject=collectionObject, toCrawlLinkList=distributorList[i]))
            for future in concurrent.futures.as_completed(futures):
                return_values.extend(future.result())
        for urlList in return_values:
            if not urlList:
                continue
            sourceURL = urlList[0]
            del urlList[0]
            dbStatus = insertNewURLs(collectionObject, urlList, sourceURL)
            if dbStatus == -1:
                logger.info('Maximum limit reached')
                break
        logger.debug('Cycle completed.')
        time.sleep(sleep_time)
