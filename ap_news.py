import os
import psycopg2
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import time
from dateutil.parser import parse
import logging
import json

# Set the path to the logs directory
logs_dir = os.path.join(os.getcwd(), "src/logs")

# Create the logs directory if it doesn't already exist
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s,%(msecs)03d: %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "ap_news_scraper.log")),
        logging.StreamHandler(),
    ],
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


class APNews:
    # constant
    DOMAIN = "https://apnews.com"

    @staticmethod
    def __get_most_recent_timestamp(source_url):
        indexed_at = None
        conn = None
        try:
            conn = psycopg2.connect(
                host=os.getenv("DATABASE_HOST"),
                database="verceldb",
                user="default",
                password=os.getenv("DATABASE_PASSWORD"),
            )
            cursor = conn.cursor()

            query = f'SELECT "indexedAt" FROM "Source" WHERE url = \'https://apnews.com/hub/{source_url}\';'
            cursor.execute(query)
            result = cursor.fetchone()

            if result is not None:
                indexed_at = result[0]
                print(f"indexedAt value for Source Apnews {source_url} : {indexed_at}")

                current_time = datetime.now()
                update_query = f'UPDATE "Source" SET "indexedAt" = %s WHERE url = \'https://apnews.com/hub/{source_url}\';'
                cursor.execute(update_query, (current_time,))
                conn.commit()

                print(
                    f"indexedAt value for Source Apnews {source_url} has been updated to current date and time."
                )
            else:
                print("No record found with the specified URL.")

            return indexed_at

        except psycopg2.Error as error:
            print(f"Error: {error}")
            return None

        finally:
            if conn:
                cursor.close()
                conn.close()

    def __init__(self, *args):
        if len(args) == 1:
            self.url = args[0]
        elif len(args) == 0:
            pass
        # creating a logging object
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.WARNING)
        # setting connection pool to stop debug level messages
        urllib3_logger = logging.getLogger("urllib3.connectionpool")
        urllib3_logger.setLevel(logging.WARNING)
        # setting variables to log at a discord channel
        self.webhook_url = "https://discord.com/api/webhooks/1103565837954199583/zSibDE8CuB4gNp9JbPubDRYpmKZPW0cnQJlr1e-wCvNmXOFoyki6Fdik1L9PdUY3sAES"

    # mines all urls from ap news main page
    def __scrape_news_urls(self, source):
        # mines all url's regardless of date (i.e at the moment)
        html = self.__send_request(self.url)
        if html is not None:  # handle exception if request was failed
            urls = html.find_all("a", attrs={"data-key": "card-headline"})
            news_urls = [url.attrs["href"] for url in urls]
            return list(set(news_urls))
        else:
            """logging.warning(
                f"No URLs are found to scrape from {source} - {self.url}"
            )"""  # instead of printig, we should log
            self.__log_to_discord(
                f"No URLs are found to scrape from {source} - {self.url}"
            )
            return None

    # mines urls after compariosion with the last index time from DB
    def __scrape_updated_urls(self, source):
        # uses source url and identifies updated urls
        # return the list of them
        html = self.__send_request(self.url)
        if html is not None:
            divs = html.findAll("div", class_="CardHeadline")
            urls_with_dates = [
                [
                    each_div.find("a", attrs={"data-key": "card-headline"}).attrs[
                        "href"
                    ],
                    datetime.strptime(
                        each_div.find("span", attrs={"data-key": "timestamp"}).attrs[
                            "data-source"
                        ],
                        "%Y-%m-%dT%H:%M:%SZ",
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                ]
                for each_div in divs
            ]

            cut_off_date = datetime.strptime(
                datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), "%Y-%m-%dT%H:%M:%SZ"
            )
            """ self.__get_most_recent_timestamp(
                self.url
            )"""  # datetime.strptime('2023-03-17T00:00:00Z',"%Y-%m-%dT%H:%M:%SZ") #tested
            # generate a list of urls that needs re scraping as per date
            self.__log_to_discord(f'last indexed date at DB: {cut_off_date} for {self.url}',color=65280)
            to_be_scraped_urls = [
                each_article[0]  # index-1 contains url for respective dates
                for each_article in urls_with_dates
                if parse(each_article[1])
                > parse(cut_off_date.strftime("%Y-%m-%d %H:%M:%S"))
            ]

            return list(set(to_be_scraped_urls))

        else:
            self.__log_to_discord(
                f"No URLs are found to scrape from {source} - {self.url}"
            )
            return None

    def __scrape_content(self, urls, source):
        # recives a list of urls and tries to scrape
        if len(urls) != 0:  # hav to handle a exception here too
            data_frame = pd.DataFrame(columns=["source", "url", "title", "content"])
            for url in urls:
                # scraped urls are missing domain name, so adding that before making request
                current_url = "".join([self.DOMAIN, url])
                html = self.__send_request(current_url)
                if html is None:
                    self.__log_to_discord(f"Bad url {current_url} for making request")
                    continue

                current_title = html.find("title")
                current_content = html.find_all("p")
                current_content = [
                    para.text.rstrip("  No posts Ready for more?")
                    for para in current_content
                    if para.text is not None
                ]
                current_content = " ".join(current_content)
                if len(current_content) == 0:
                    """logging.debug(
                        f"No content found at: {current_url}"
                    )"""
                    self.__log_to_discord(f"No content found at: {current_url}")
                    # let's skip the urls that aren't having content and log them for future debugging
                    continue
                new_data_frame = pd.DataFrame(
                    {
                        "source": [source],
                        "url": [current_url],
                        "title": [current_title],
                        "content": [current_content],
                    }
                )

                data_frame = pd.concat(
                    [data_frame, new_data_frame], axis=0, ignore_index=True
                )

            return data_frame

    # fine & exception hadled

    def __send_request(self, url, retries=3):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as exception:
            if retries > 0:
                # logging.debug(f'Error: While scraping {url} {exception}. Retrying in 5 seconds...')
                time.sleep(5)
                return self.__send_request(url, retries - 1)
            else:
                """self.logger.setLevel(logging.WARNING)
                self.logger.warning(
                    f"Problem with scraping [{url}]: {exception} After 3 retries, No retries left. Check URL passed!"
                )"""
                self.__log_to_discord(
                    f"problem with scraping [{url}]: {exception} After 3 retries, No retries left. Check URL passed!"
                )
                return None

    # logs the errors to discord channel

    def __log_to_discord(self, message, color=16711680, retries=3):  # by-default red
        payload = self.__create_payload(message, color)
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as exception:
            if retries > 0:
                # logging.debug(f'Error: While scraping {url} {exception}. Retrying in 5 seconds...')
                time.sleep(5)
                return self.__log_to_discord(message, retries - 1)
            else:
                """self.logger.setLevel(logging.WARNING)
                self.logger.warning(
                    f"Error Logging to discord: {exception} After 3 retries, No retries left. Check/Debug URL passed!"
                )"""
                # logs to local file after 3 failed calls
                self.logger.error(
                    f"Error Logging to discord: {exception} After 3 retries, No retries left. Check/Debug URL passed!"
                )
                return None

    def __create_payload(self, message, color=16711680):
        return {
            "content": "",
            "embeds": [
                {"title": "ap-news scraper", "description": message, "color": color}
            ],
        }

    def scrape(self):
        self.__log_to_discord("initiating ap-news scraper", color=65280)
        # currently modified to scrape only updated urls
        obj1 = APNews("https://apnews.com/hub/cryptocurrency")
        obj2 = APNews("https://apnews.com/hub/blockchain")

        # working super fine
        # df1_urls = obj1.__scrape_news_urls("Apnews Cryptocurrency")
        # df2_urls = obj2.__scrape_news_urls("Apnews Blockchain")

        df1_urls = obj1.__scrape_updated_urls("Apnews Cryptocurrency")
        df2_urls = obj2.__scrape_updated_urls("Apnews Blockchain")

        if df1_urls is not None:
            df1 = obj1.__scrape_content(df1_urls, "Apnews Cryptocurrency")
        else:
            df1 = None
        if df2_urls is not None:
            df2 = obj2.__scrape_content(df2_urls, "Apnews Blockchain")
        else:
            df2 = None

        dfs_to_concat = []

        if df1 is not None:
            dfs_to_concat.append(df1)
        else:
            # self.logger.info(f'No updates found at Apnews Cryptocurrency')
            self.__log_to_discord(
                f"No updates found at Apnews Cryptocurrency", color=16753920
            )  # orange

        if df2 is not None:
            dfs_to_concat.append(df2)
        else:
            # self.logger.info(f'No updates found at Apnews Blockchain')
            self.__log_to_discord(
                f"No updates found at Apnews Blockchain", color=16753920
            )

        if len(dfs_to_concat) > 0:
            df = pd.concat(dfs_to_concat, ignore_index=True)
            df = df.drop_duplicates(subset="url", keep="first")
            df.reset_index(drop=True, inplace=True)
            self.__log_to_discord(
                f"scraping successful... {df.shape[0]} urls are updated!", color=65280
            )  # green
            return df
        else:
            # self.logger.log(f'Neither of sites have updated/ new content')
            self.__log_to_discord(
                "Neither of sites have updated/ new content", color=16753920
            )  # orange


obj = APNews()  # working
print(
    obj.scrape()
)  # working invokes __scrape_ap_news,__scrape_updated_urls, __scrape_content

# end = time.time()
# print(end - start)
