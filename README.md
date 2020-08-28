## Python Web Crawler

This repository contains code for crawling the Internet, dowloading and extracting all the links off those web pages.

The program makes web requests starting from a root URL which has been manually inserted into the database, downloads it and saves it locally. Then it extracts
http links from the downloaded source and saves them to the database. And the process repeats ...

The supported database is [MongoDB](https://www.mongodb.com/)

### To run locally:

- Install requirements
```
  pip3 install -r requirements.txt
```

- Run the application
```
  python3 main.py
```

And wait until you are out of disk space :grin:
