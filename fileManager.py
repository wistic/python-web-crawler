import os

from logger import logger
from cfg import config

def saveFile(filename, urldata, responseObject):
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
            with open(filePath, "w") as file:
                file.write(responseObject.text)
        else:
            with open(filePath, "wb") as file:
                for chunk in responseObject.iter_content(chunk_size=128):
                    file.write(chunk)
        return 1