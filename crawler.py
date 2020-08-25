import requests
import datetime
import traceback

from cfg import config
from logger import logger
from dbManager import updateEntry
from fileManager import saveFile
from linkProcessor import returnValidLinks


def crawl(collectionObject, toCrawlLinkList):
    newFoundLinks = []
    headers = {
        'User-Agent': config['user_agent'],
    }
    for link in toCrawlLinkList:
        try:
            logger.debug("Making HTTP GET request: " + link)
            request = requests.get(link, headers=headers, stream=True)
            logger.debug("Got response, content length = " +
                         str(len(request.text)))
        except:
            logger.warning("Failed to get HTML source from " +
                           link+". Check Internet connection")
            traceback.print_exc()
            continue
        contentType = request.headers['content-type'].split(';', 1)[0]
        urldata = {
            "Link": link,
            "lastCrawlDt": datetime.datetime.now(),
            "responseStatus": request.status_code,
            "contentType": contentType,
            "contentLength": request.headers['content-length'] if bool(request.headers['content-length']) else str(len(request.text))
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
