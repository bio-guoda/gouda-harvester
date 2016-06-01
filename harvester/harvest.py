import os
import unicodecsv as csv
import requests
import idigbio
import pycron
import logging

from cStringIO import StringIO

logging.root.setLevel(logging.DEBUG)
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
file_handler = logging.FileHandler("error.log")
file_handler.setLevel(logging.WARNING)

logger = logging.root

PUSH_ALL = False

HARVEST_LIST_URLS = (
    "https://raw.githubusercontent.com/godfoder/gimmefreshdata.github.io/patch-1/_data/sources.csv",
)

def main():
    api = idigbio.json(
        env="prod",
        user=os.environ.get("IDB_API_UUID"),
        password=os.environ.get("IDB_API_KEY")
    )

    for url in HARVEST_LIST_URLS:
        try:
            r = requests.get(url)
            r.raise_for_status()

            header = None
            for line in csv.reader(StringIO(r.text)):
                if header is None:
                    header = line
                else:
                    line_dict = dict(zip(header,line))

                    mime = "text/csv"
                    if line_dict["archive_format"] == "DwCA":
                        mime = "application/zip"

                    cron_line = line_dict["cron"]
                    if PUSH_ALL or (len(cron_line) > 0 and pycron.is_now(cron_line)):
                        logger.info(line_dict)
                        try:
                            api_r = api.addurl(line_dict["archive_url"], media_type="guoda", mime_type=mime)
                            logger.info(api_r)
                        except:
                            print api_r.content
                            logger.exception("Error Pushing URL")
                        
                    else:
                        logger.debug("Skipping %s", line_dict["name"])

        except:
            logger.exception("Error Fetching %s", url)

if __name__ == '__main__':
    main()
