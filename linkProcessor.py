"""
    This file contains the function for parsing the HTML source received from a valid link
    and extract all the crawlable links from it.

    Methods
    -------
    returnValidLinks(sourceLink, receivedHTML)
        Return a list of hyperlinks extracted from the source file and can be crawled.
"""

# Library imports
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Local file imports
from logger import logger

def returnValidLinks(sourceLink, receivedHTML):
    """
    Return a list of hyperlinks extracted from the source file and can be crawled.

    Parameters
    ----------
    sourceLink : str
        Link of the source whose HTML file is provided to scrape.
    receivedHTML: str
        HTML file to scrape.

    Returns
    -------
    trimmedValidLinks : list
        List of links which are valid and can be further crawled.
    """
    allLinks = []
    soup = BeautifulSoup(receivedHTML, 'html.parser')
    for link in soup.find_all('a'):
        allLinks.append(link.get('href'))
    absLinks = []
    for link in allLinks:
        absLinks.append(urljoin(sourceLink, link))
    absLinks = list(dict.fromkeys(absLinks))  # to remove duplicate entries
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
    trimmedLinks = list(dict.fromkeys(trimmedLinks))
    trimmedValidLinks = []
    trimmedValidLinks.append(sourceLink)
    trimmedValidLinks.extend(trimmedLinks)
    return trimmedValidLinks
