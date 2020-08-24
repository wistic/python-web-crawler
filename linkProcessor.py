from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from logger import logger


def returnValidLinks(sourceLink, receivedHTML):
    allLinks = []
    soup = BeautifulSoup(receivedHTML, 'html.parser')
    for link in soup.find_all('a'):
        allLinks.append(link.get('href'))
    absLinks = []
    for link in allLinks:
        absLinks.append(urljoin(sourceLink, link))
    absLinks = list(dict.fromkeys(absLinks))
    validLinks = []
    for link in absLinks:
        if bool(urlparse(link).netloc) and bool(urlparse(link).scheme):
            if urlparse(link).scheme == 'https' or urlparse(link).scheme == 'http':
                validLinks.append(link)
            else:
                pass
    trimmedLinks = []
    for link in validLinks:
        if link[-1] == "/":
            trimmedLinks.append(link[0:(len(link)-1)])
        else:
            trimmedLinks.append(link)
    return trimmedLinks
