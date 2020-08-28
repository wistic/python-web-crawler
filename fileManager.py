"""
    This file contains the function for the dealing with saving the files locally.

    Methods
    -------
    saveFile(filename, urldata, responseObject)
        Save or delete a file from 'download_dir_path' after crawling a link.
"""

# Library imports
import os

# Local file imports
from logger import logger
from cfg import config

def saveFile(filename, urldata, responseObject):
    """
    Save or delete a file from 'download_dir_path' after crawling a link.

    Parameters
    ----------
    filename : str
        Name of the file to be saved or deleted.
    urldata: dict
        A dictionary containing updated data of a link after crawling it.
    responseObject : requests.models.Response
        Object which represents the response of a HTTP GET request.

    Returns
    -------
    0/1
        depending on whether the file is saved or deleted.
    """

    download_dir_path = config["download_dir_path"]

    if download_dir_path[-1] == "/":
        filePath = download_dir_path + filename
    else:
        filePath = download_dir_path + "/" + filename
    
    if urldata["responseStatus"] != 200:
        try:
            os.remove(filePath)
        except FileNotFoundError:
            pass
        return 0
    else:
        if urldata["contentType"].split("/", 1)[0] == "text":
            with open(filePath, "w") as file:       # Open in text mode
                file.write(responseObject.text)
        else:
            with open(filePath, "wb") as file:      # Open in binary mode
                for chunk in responseObject.iter_content(chunk_size=128):
                    file.write(chunk)
        return 1