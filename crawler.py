"""
    This file contains the function for the crawling process.

    Methods
    -------
    crawl(collectionObject, toCrawlLinkList)
        Crawls a web page and returns valid hyperlinks found in the crawled pages.
"""

# Library imports
import requests
import datetime
import traceback

# Local file imports
from cfg import config
from logger import logger
from dbManager import updateEntry
from fileManager import saveFile
from linkProcessor import returnValidLinks


def crawl(collectionObject, toCrawlLinkList):
    """
    Crawls a web page and returns valid hyperlinks found in the crawled pages.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.
    toCrawlLinkList : list
        List of links which are to be crawled by this instance.

    Returns
    -------
    newFoundLinks
        a list of links which have been scraped off the crawled pages
    """

    newFoundLinks = []
    headers = {
        'User-Agent': config['user_agent'],
    }

    for link in toCrawlLinkList:
        try:
            logger.debug("Making HTTP GET request: " + link)
            request = requests.get(link, headers=headers, stream=True)
        except:
            logger.warning("Failed to get HTML source from " +
                           link+". Check Internet connection")
            continue
        else:
            logger.debug("Got response: " + link)
        
        contentType = request.headers['content-type'].split(';', 1)[0]
        try:
            contentLength = request.headers['content-length']
        except KeyError:
            contentLength = str(len(request.text))

        urldata = {
            "Link": link,
            "lastCrawlDt": datetime.datetime.now(),
            "responseStatus": request.status_code,
            "contentType": contentType,
            "contentLength": contentLength
        }

        updateStatus = updateEntry(collectionObject, urldata)
        filename = ""
        if updateStatus == "-2":
            logger.warning("Link {} is missing from the database".format(link))
            continue
        else:
            filename = updateStatus
        saveFile(filename, urldata, request)

        if request.status_code == 200 and contentType == 'text/html':
            validLinks = returnValidLinks(link, request.text)
        newFoundLinks.append(validLinks)

    return newFoundLinks
