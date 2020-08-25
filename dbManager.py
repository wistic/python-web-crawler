import datetime
from datetime import timedelta
from bson import ObjectId

from logger import logger
from cfg import config

# Function to check whether the database is within limits


def isDBwithinLimit(collectionObject):
    count = collectionObject.count_documents({})
    if count >= config["Max_links_limit"]:
        return False
    return True

# Function to insert the root URL


def insertRootURL(collectionObject):
    url = config["root_url"]
    if url[-1] == "/":
        url = url[0:(len(url)-1)]
    document = {"Link": url,
                "sourceLink": "Manual",
                "isCrawled": "False",
                "createdAt": datetime.datetime.now()}
    collectionObject.insert_one(document)
    logger.debug('Root URL inserted')


# Function to return a list of links which haven't been crawled yet
def returnUncrawledLinks(collectionObject):
    uncrawledList = []
    for document in collectionObject.find({"isCrawled": "False"}):
        uncrawledList.append(document["Link"])
    return uncrawledList


# Function to return links which have not been crawled for Recrawl_time_limit_hours
def returnExpiredLinks(collectionObject):
    timestamp = datetime.datetime.now()
    tdelta = timedelta(hours=-config["Recrawl_time_limit_hours"])
    recrawlTimestamp = timestamp + tdelta
    expiredLinks = []
    for document in collectionObject.find({"$and": [{"lastCrawlDt": {"$lte": recrawlTimestamp}}, {"isCrawled": "True"}]}):
        expiredLinks.append(document["Link"])
    return expiredLinks


# Function to insert newly scraped valid URLs
def insertNewURLs(collectionObject, newURLList, sourceURL):
    for link in newURLList:
        document = {"Link": link,
                    "sourceLink": sourceURL,
                    "isCrawled": "False",
                    "createdAt": datetime.datetime.now()}
        if isDBwithinLimit(collectionObject) == False:
            logger.debug("Storage limit of {} links crossed".format(
                config["Max_links_limit"]))
            return -1  # for reference purpose returning -1
        elif collectionObject.find_one({"Link": link}) is not None:
            logger.debug(
                "Link already exists in the database -> {}".format(link))
        else:
            collectionObject.insert_one(document)
    return 0


# Function to update the scraped URL
def updateEntry(collectionObject, urldata):
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
