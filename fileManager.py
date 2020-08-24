import os

from logger import logger


def saveFile(filename, urldata):
    filePath = str(os.path.abspath(os.getcwd()))+"/html-files/"+filename
    if urldata["responseStatus"]!="200":
        with open(filePath,"w+") as file:
            pass
        file.close()
        os.remove(filePath)
        