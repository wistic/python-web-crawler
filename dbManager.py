"""
    This file contains the functions for the querying and updating the database.

    Methods
    -------
    isDBwithinLimit(collectionObject)
        Checks if the number of documents in the database is within 'Max_links_limit'.

    insertRootURL(collectionObject):
        Used to insert the root URL into the database.

    returnUncrawledLinks(collectionObject):
        Used to get the links which have not been crawled yet.

    returnExpiredLinks(collectionObject):
        Used to get the links which have not been crawled in the last 'Recrawl_time_limit_hours' hours.

    insertNewURLs(collectionObject, newURLList, sourceURL)
        Used to insert new URLS into the database.
    
    updateEntry(collectionObject, urldata)
        Used to update the data of the crawled links into the database.
"""

# Library imports
import datetime
from datetime import timedelta
from bson import ObjectId

# Local file imports
from logger import logger
from cfg import config

def isDBwithinLimit(collectionObject):
    """
    Checks if the number of documents in the database is within 'Max_links_limit'.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.

    Returns
    -------
    True/False
        depending on whether it satisfies the condition or not
    """
    count = collectionObject.count_documents({})
    if count >= config["Max_links_limit"]:
        return False
    return True

def insertRootURL(collectionObject):
    """
    Used to insert the root URL into the database.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.
    """
    url = config["root_url"]
    if url[-1] == "/":
        url = url[0:(len(url)-1)]
    document = {"Link": url,
                "sourceLink": "Manual",
                "isCrawled": "False",
                "createdAt": datetime.datetime.now()}
    collectionObject.insert_one(document)

def returnUncrawledLinks(collectionObject):
    """
    Used to get the links which have not been crawled yet.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.
    
    Returns
    -------
    uncrawledList : list
        List of links which have not been crawled yet.
    """
    uncrawledList = []
    for document in collectionObject.find({"isCrawled": "False"}):
        uncrawledList.append(document["Link"])
    return uncrawledList

def returnExpiredLinks(collectionObject):
    """
    Used to get the links which have not been crawled in the last 'Recrawl_time_limit_hours' hours.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.
    
    Returns
    -------
    expiredLinks : list
        List of links which have not been crawled in the last 'Recrawl_time_limit_hours' hours.
    """
    timestamp = datetime.datetime.now()
    tdelta = timedelta(hours=-config["Recrawl_time_limit_hours"])
    recrawlTimestamp = timestamp + tdelta
    expiredLinks = []
    for document in collectionObject.find({"$and": [{"lastCrawlDt": {"$lte": recrawlTimestamp}}, {"isCrawled": "True"}]}):
        expiredLinks.append(document["Link"])
    return expiredLinks

def insertNewURLs(collectionObject, newURLList, sourceURL):
    """
    Used to get the links which have not been crawled in the last 'Recrawl_time_limit_hours' hours.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.
    
    Returns
    -------
    -1/0
        depending on whether the insertion is successful
    """
    for link in newURLList:
        document = {"Link": link,
                    "sourceLink": sourceURL,
                    "isCrawled": "False",
                    "createdAt": datetime.datetime.now()}
        if isDBwithinLimit(collectionObject) == False:
            logger.debug("Storage limit of {} links crossed".format(
                config["Max_links_limit"]))
            return -1  # if database goes out of limit
        elif collectionObject.find_one({"Link": link}) is not None:
            logger.debug(
                "Link already exists in the database -> {}".format(link))
        else:
            collectionObject.insert_one(document)
    return 0

def updateEntry(collectionObject, urldata):
    """
    Used to update the data of the crawled links into the database.

    Parameters
    ----------
    collectionObject : pymongo.collection.Collection
        Object which represents a collection in the database.
    urldata: dict
        A dictionary containing updated data of a link after crawling it.

    Returns
    -------
    path : str
        Name of the file in which the link's extracted text/binary data is stored.
    """

    document = collectionObject.find_one({"Link": urldata["Link"]})
    if document is None:
        logger.warning("Link missing from the database.")
        return "-2"  # Represents a missing link
    else:
        filter = {"Link": urldata["Link"]}
        path = str(document["_id"]) + "." + \
            urldata["contentType"].split('/', 1)[1]
        if urldata["responseStatus"] != 200:
            newvalues = {
                "$set": {
                    "isCrawled": "True",
                    "lastCrawlDt": urldata["lastCrawlDt"],
                    "responseStatus": urldata["responseStatus"],
                    "contentType": urldata["contentType"],
                    "contentLength": urldata["contentLength"],
                    "filePath": ""
                }
            }
            collectionObject.update_one(filter, newvalues)
        else:
            newvalues = {
                "$set": {
                    "isCrawled": "True",
                    "lastCrawlDt": urldata["lastCrawlDt"],
                    "responseStatus": urldata["responseStatus"],
                    "contentType": urldata["contentType"],
                    "contentLength": urldata["contentLength"],
                    "filePath": path
                }
            }
            collectionObject.update_one(filter, newvalues)
        return path
