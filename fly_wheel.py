import os
import psycopg2
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date, timezone, timedelta
import time
from dateutil.parser import parse
import random

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


class FlyWheel:
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

            query = 'SELECT "indexedAt" FROM "Source" WHERE url = \'https://flywheeloutput.com/sitemap.xml\';'
            cursor.execute(query)
            result = cursor.fetchone()

            if result is not None:
                indexed_at = result[0]
                print(f"indexedAt value for Source Flywheel: {indexed_at}")

                current_time = datetime.now()
                update_query = 'UPDATE "Source" SET "indexedAt" = %s WHERE url = \'https://flywheeloutput.com/sitemap.xml\';'
                cursor.execute(update_query, (current_time,))
                conn.commit()

                print(
                    "indexedAt value for Source Flywheel has been updated to current date and time."
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

    def __init__(self):
        self.url = "https://flywheeloutput.com/"
        self.sitemap_url = "https://flywheeloutput.com/sitemap.xml"
        self.webhook_url = os.getenv("WEBHOOK_URL")
        # self.discord = Discord()
        # self.discord.log_to_discord('initiating flywheel scraper', color=65280)

    """probably this part of code is not nucessaer, since we know sitemap url, if it's chaging in dynamic sense
        may be then for finding where will he helpful
    """

    def __sitemap_exists(self, base_url,retries=3):
        with open("".join([os.path.dirname(__file__), "/sitemaps.json"]), "r") as file:
            sitemap_url_list = json.load(file)["sitemap_types"]

        for url in sitemap_url_list:
            new_url = "".join([base_url + url])
            try:
                response = requests.get(new_url)
                response.raise_for_status()
                return (base_url.rstrip("/"), url, base_url.rstrip("/") + url)
            except requests.exceptions.RequestException as exception:
                if retries > 0:
                    time.sleep(5)
                    self.__sitemap_exists(base_url,retries-1)
                else:
                    self.__log_to_discord(
                        f"problem with scraping [{url}]: {exception} After 3 retries, No retries left. Check URL passed!"
                    )
                    return None
        else:
            self.__log_to_discord(f"Sitemap does not exist for [{base_url}]")
            return None

    def __scrape_content(self, urls, source):
        # recives a list of urls and tries to scrape
        if len(urls) != 0:
            data_frame = pd.DataFrame(columns=["source", "url", "title", "content"])
            for current_url in urls:
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
        else:
            self.__log_to_discord(f"Scraper recieved 0 urls to scrape from {source}")
            return None

    def __scrape_updated_urls(self, sitemap_url,retries=3):
        # let it be since here we are parsing xml page
        try:
            response = requests.get(sitemap_url)
            response.raise_for_status()
            data = BeautifulSoup(response.text, "xml")

            url_set = data.find_all("url")

            """
            Ex: <url>
                <loc>https://flywheeloutput.com/p/everything-you-need-to-know-about</loc>
                <lastmod>2023-04-20</lastmod>
                <changefreq>monthly</changefreq>
            </url>
            """
            url_dates = [
                [
                    datetime.fromisoformat(
                        url_set_item.find("lastmod").string
                    ).strftime("%Y-%m-%d"),
                    url_set_item.find("loc").string,
                ]
                for url_set_item in url_set
                if url_set_item.find("lastmod") is not None
            ]

            # most_recent_timestamp = self.__get_most_recent_timestamp()
            # cut_off_date = most_recent_timestamp.date()
            # cut_off_date = cut_off_date.strftime("%Y-%m-%d")

            start_date = datetime(2023, 4, 1, tzinfo=timezone.utc)
            end_date = datetime(2023, 5, 1, tzinfo=timezone.utc)

            random_dt = self.__random_date(start_date, end_date)
            random_dt_str = random_dt.strftime("%Y-%m-%d")

            cut_off_date = random_dt_str
            self.__log_to_discord(
                f"last indexed date at DB: {cut_off_date} for [{self.url}]",
                color=16776960,
            )
            to_be_scraped_urls = [
                each_article[1]
                for each_article in url_dates
                if parse(each_article[0]) > parse(random_dt_str)
            ]
            self.__log_to_discord(to_be_scraped_urls, color=16776960)
            return list(set(to_be_scraped_urls))

        except requests.exceptions.RequestException as exception:
            if retries > 0:
                time.sleep(5)
                self.__sitemap_exists(sitemap_url, retries-1)
            else:
                self.__log_to_discord(
                    f"No URLs found at sitemap {sitemap_url}")
                self.__log_to_discord(
                    f"problem with scraping [{sitemap_url}]: {exception} After 3 retries, No retries left. Check URL passed!"
                )
                return None

    def __random_date(self, start, end):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)

    def __scrape_all_urls(self, sitemap_url,retries=3):
        try:
            response = requests.get(sitemap_url)
            response.raise_for_status()
            data = BeautifulSoup(response.text, "xml")

            url_set = data.find_all("url")

            """
            Ex: <url>
                <loc>https://flywheeloutput.com/p/everything-you-need-to-know-about</loc>
                <lastmod>2023-04-20</lastmod>
                <changefreq>monthly</changefreq>
            </url>
            """
            url_dates = [
                [
                    datetime.fromisoformat(
                        url_set_item.find("lastmod").string
                    ).strftime("%Y-%m-%d"),
                    url_set_item.find("loc").string,
                ]
                for url_set_item in url_set
                if url_set_item.find("lastmod") is not None
            ]

            current_date = date.today().strftime("%Y-%m-%d")

            to_be_scraped_urls = [
                each_article[1]
                for each_article in url_dates
                if (parse(current_date) - parse(each_article[0])).days >= 1
            ]

            return to_be_scraped_urls
        except requests.exceptions.RequestException as exception:
            if retries > 0:
                time.sleep(5)
                self.__sitemap_exists(sitemap_url, retries-1)
            else:
                self.__log_to_discord(
                    f"No URLs found at sitemap {sitemap_url}")
                self.__log_to_discord(
                    f"problem with scraping [{sitemap_url}]: {exception} After 3 retries, No retries left. Check URL passed!"
                )
                return None


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
                        "title": "[Flywheel scraper]",
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
                        "title": "[Flywheel scraper]",
                        "description": message,
                        "color": color,
                    }
                ],
            }

    def scrape_fly_wheel(self):
        try:
            self.__log_to_discord("initiating flywheel scraper", color=65280)
            possibility = self.__sitemap_exists(self.url)
            if possibility is not None:
                urls = self.__scrape_updated_urls(possibility[2])
                if urls is not None:
                    data_frame = self.__scrape_content(urls, source="Flyhweel")
                    if data_frame is not None:
                        self.__log_to_discord(
                            f"scraping successful... {data_frame.shape[0]} urls are updated!",
                            color=65280,
                        )
                        return data_frame
                    else:
                        return None
                else:
                    return None
            else:
                return None
        finally:
            self.__log_to_discord("finished scraping Flywheel!!", color=65280)


obj = FlyWheel()
print(obj.scrape_fly_wheel())
