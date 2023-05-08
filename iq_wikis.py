import os
import psycopg2
import requests
from datetime import timezone, datetime, timedelta
import pandas as pd
from dateutil import parser
import time
import json
import random
import logging


try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


class IQWiki:
    @staticmethod
    def __get_most_recent_timestamp():
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

            query = 'SELECT "indexedAt" FROM "Source" WHERE url = \'https://graph.everipedia.org/graphql\';'
            cursor.execute(query)
            result = cursor.fetchone()

            if result is not None:
                indexed_at = result[0]
                print(f"indexedAt value for Source IQ Wiki: {indexed_at}")

                current_time = datetime.now()
                update_query = 'UPDATE "Source" SET "indexedAt" = %s WHERE url = \'https://graph.everipedia.org/graphql\';'
                cursor.execute(update_query, (current_time,))
                conn.commit()

                print(
                    "indexedAt value for Source IQ Wiki has been updated to current date and time."
                )
            else:
                print("No record found with the specified IQ Wiki URL.")
            return indexed_at

        except psycopg2.Error as error:
            print(f"Error: {error}")
            return None

        finally:
            if conn:
                cursor.close()
                conn.close()

    def __init__(self):
        self.url = "https://graph.everipedia.org/graphql"

        self.query_new_wikis = """
        {
            activities(lang: "en") {
                datetime
                content {
                    id
                    title
                    content
                }
            }
        }
        """
        # creating a logging object
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.WARNING)
        # setting connection pool to stop debug level messages
        urllib3_logger = logging.getLogger("urllib3.connectionpool")
        urllib3_logger.setLevel(logging.WARNING)
        # setting variables to log at a discord channel

        self.webhook_url = os.getenv("WEBHOOK_URL")

    def __scrape_new_urls(self, cut_off_date):
        response = requests.post(url=self.url, json={"query": self.query_new_wikis})

        if response.status_code == 200:
            data = response.json()
            activities = data["data"]["activities"]

            new_wikis = {}
            for activity in activities:
                activity_date_time = parser.parse(activity["datetime"])
                activity_date_time = activity_date_time.astimezone(
                    timezone.utc
                ).replace(tzinfo=None)
                if activity_date_time > cut_off_date:
                    contents = activity["content"]
                    for content in contents:
                        wiki_id = content["id"]
                        if (
                            wiki_id not in new_wikis
                            or activity_date_time > new_wikis[wiki_id]["datetime"]
                        ):
                            new_wikis[wiki_id] = {
                                "datetime": activity_date_time,
                                "title": content["title"],
                                "content": content["content"],
                            }

            return new_wikis
        else:
            self.__log_to_discord(f"failed to establish connection to {response.url}")
            print(f"Error: {response.status_code}")
            print(response.text)
            return {}

    def __scrape_new_urls_today(self):
        # cut_off_date = self.__get_most_recent_timestamp()
        start_date = datetime(2022, 7, 1, tzinfo=timezone.utc)
        end_date = datetime(2023, 5, 1, tzinfo=timezone.utc)

        random_dt = self.__random_date(start_date, end_date)
        random_dt_str = random_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        cut_off_date = datetime.strptime(random_dt_str, "%Y-%m-%dT%H:%M:%SZ")
        self.__log_to_discord(
            f"last indexed date at DB: {cut_off_date} for [{self.url}]",
            color=16776960,
        )
        new_wikis = self.__scrape_new_urls(cut_off_date)

        reframed_data_frame = pd.DataFrame(
            columns=["source", "url", "title", "content"]
        )

        for wiki_id, wiki_data in (
            new_wikis.items() if isinstance(new_wikis, dict) else new_wikis.iterrows()
        ):
            if isinstance(new_wikis, pd.DataFrame):
                wiki_id = wiki_data["wikiid"]
                wiki_data = wiki_data.drop("wikiid")

            source = "IQ Wiki"
            url = f"https://iq.wiki/wiki/{wiki_id}"
            title = wiki_data["title"]
            content = wiki_data["content"]
            temp_data_frame = pd.DataFrame(
                {
                    "source": [source],
                    "url": [url],
                    "title": [title],
                    "content": [content],
                }
            )
            reframed_data_frame = pd.concat(
                [reframed_data_frame, temp_data_frame], ignore_index=True
            )
        self.__log_to_discord(
            "following urls are scraped for updation:\n"
            + "\n".join(reframed_data_frame["url"][:]),
            color=16776960,
        )
        return reframed_data_frame

    def __scrape_all_urls(self):
        data_frame = pd.DataFrame(columns=["id", "title", "content"])
        offset = 0
        limit = 50
        has_more_data = True

        while has_more_data:
            query = f"""
            {{
                wikis(limit: {limit}, offset: {offset}) {{
                    id
                    title
                    content
                }}
            }}
            """

            response = requests.post(url=self.url, json={"query": query})

            freshDf = pd.DataFrame(response.json()["data"]["wikis"])

            if len(freshDf) < limit:
                has_more_data = False

            data_frame = pd.concat([data_frame, freshDf], axis=0, ignore_index=True)

            offset += limit

        data_frame = data_frame.drop_duplicates(subset=["id"])

        # wikis = self.scrape_all_urls()
        data_frame.rename(columns={data_frame.columns[0]: "wikiid"}, inplace=True)

        reframed_data_frame = pd.DataFrame(
            columns=["source", "url", "title", "content"]
        )

        for wiki_id, wiki_data in (
            data_frame.items()
            if isinstance(data_frame, dict)
            else data_frame.iterrows()
        ):
            if isinstance(data_frame, pd.DataFrame):
                wiki_id = wiki_data["wikiid"]
                wiki_data = wiki_data.drop("wikiid")

            source = "IQ Wiki"
            url = f"https://iq.wiki/wiki/{wiki_id}"
            title = wiki_data["title"]
            content = wiki_data["content"]
            temp_data_frame = pd.DataFrame(
                {
                    "source": [source],
                    "url": [url],
                    "title": [title],
                    "content": [content],
                }
            )
            reframed_data_frame = pd.concat(
                [reframed_data_frame, temp_data_frame], ignore_index=True
            )

        return reframed_data_frame

    """start here for logging common file"""

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
        if isinstance(message, list):
            message = "following urls are scraped for updation:\n" + "\n\n".join(
                message
            )
            return {
                "content": "",
                "embeds": [
                    {
                        "title": "[IQ Wiki scraper]",
                        "description": message,
                        "color": color,
                    }
                ],
            }
        else:
            return {
                "content": "",
                "embeds": [
                    {
                        "title": "[IQ Wiki scraper]",
                        "description": message,
                        "color": color,
                    }
                ],
            }

    """end here for logging commons"""

    def __random_date(self, start, end):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)

    """ end here for logging + testing with random date"""

    def scrape(self):
        try:
            self.__log_to_discord("initiating IQ Wiki scraper", color=65280)
            data_frame = self.__scrape_new_urls_today()
            if data_frame is not None:
                self.__log_to_discord(
                    f"scraping successful... {data_frame.shape[0]} urls are updated!",
                    color=65280,
                )
                return data_frame
        finally:
            self.__log_to_discord("finished scraping IQ Wiki!!", color=65280)


obj = IQWiki()
print(obj.scrape())
# print(obj.scrape_all_urls())
