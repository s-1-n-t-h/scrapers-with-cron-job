import os
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import time
from dateutil.parser import parse
import random

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


class FlyWheel:
    #__INTERNAL_ACTIVITY_WHOOK_URL = os.getenv("INTERNAL_ACTIVITY_WHOOK_URL")
    __BRAIN_DAO_ALARMS_WHOOK_URL = os.getenv("WEBHOOK_URL")

    def __init__(self):
        self.URL = "https://flywheeloutput.com/"
        self.SITEMAP_URL = "https://flywheeloutput.com/sitemap.xml"

    """probably this part of code is not necessary since we know sitemap url, if it's chaging in dynamic sense
        may be then for finding where will he helpful
    """

    def __scrape_content(self, urls, source="Flywheel"):
        data_frame = pd.DataFrame(columns=["source", "url", "title", "content"])
        for current_url in urls:
            html = self.__send_request(current_url)
            if html is None:
                self.__log_to_discord(
                    f"⛔️ Bad URL for making request - {current_url} ⛔️"
                )
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
                self.__log_to_discord(f"⛔️ No content found at: {current_url}\n 😿")
                continue
            else:
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

    def __scrape_updated_urls(self, cut_off_date):
        # let it be since here we are parsing xml page

        xml = self.__send_request(self.SITEMAP_URL, parser="xml")
        if xml is not None:
            url_set = xml.find_all("url")

            """
            Ex: <url>
                <loc>https://flywheeloutput.com/p/everything-you-need-to-know-about</loc>
                <lastmod>2023-04-20</lastmod>
                <changefreq>monthly</changefreq>
            </url>
            """
            urls_with_dates = [
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
            """
            start_date = datetime(2023, 4, 1, tzinfo=timezone.utc)
            end_date = datetime(2023, 5, 1, tzinfo=timezone.utc)

            random_dt = self.__random_date(start_date, end_date)
            random_dt_str = random_dt.strftime("%Y-%m-%d")
            """
            self.__log_to_discord(
                f"Scraped Flywheel last on: {cut_off_date} 🗓️",
                color=16776960,
            )
            cut_off_date = cut_off_date.date()
            cut_off_date = cut_off_date.strftime("%Y-%m-%d")
            to_be_scraped_urls = [
                each_article[1]
                for each_article in urls_with_dates
                if parse(each_article[0]) > parse(cut_off_date)
            ]

            if to_be_scraped_urls is not None:
                self.__log_to_discord(to_be_scraped_urls, color=16776960)  # yellow
                return list(set(to_be_scraped_urls))
            else:
                return None
        else:
            return None

    def random_date(self, start, end):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)

    def __scrape_all_urls(self):
        xml = self.__send_request(self.SITEMAP_URL, parser="xml")
        if xml is not None:
            url_set = xml.find_all("url")
            """
            Ex: <url>
                <loc>https://flywheeloutput.com/p/everything-you-need-to-know-about</loc>
                <lastmod>2023-04-20</lastmod>
                <changefreq>monthly</changefreq>
            </url>
            """
            urls_with_dates = [
                [
                    datetime.fromisoformat(
                        url_set_item.find("lastmod").string
                    ).strftime("%Y-%m-%d"),
                    url_set_item.find("loc").string,
                ]
                for url_set_item in url_set
                if url_set_item.find("lastmod") is not None
            ]

            to_be_scraped_urls = [each_article[1] for each_article in urls_with_dates]
            if to_be_scraped_urls is not None:
                self.__log_to_discord(to_be_scraped_urls, color=16776960)
                return list(set(to_be_scraped_urls))
            else:
                return None
        else:
            return None

    def __send_request(self, url, retries=3, parser="html.parser"):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, parser)
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
                    f"❌ problem with scraping [{url}]: {exception} After 3 retries, No retries left. Check URL passed! ❌"
                )
                return None

    def __log_to_discord(self, message, color=16711680, retries=3):  # by-default red
        payload = self.__create_payload(message, color)
        try:
            response = requests.post(
                self.__BRAIN_DAO_ALARMS_WHOOK_URL,
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
                    f"❌ Error Logging to discord: {exception} After 3 retries, No retries left. Check/Debug URL passed! ❌"
                )"""
                # logs to local file after 3 failed calls
                self.logger.error(
                    f"❌ Error Logging to discord: {exception} After 3 retries, No retries left. Check/Debug URL passed! ❌"
                )
                return None

    def __create_payload(self, message, color=16711680):
        if isinstance(message, list):
            message = "Following 🔗s are scraped from Flywheel:\n\n" + "\n\n".join(
                message
            )
            return {
                "content": "",
                "embeds": [
                    {
                        "title": "Flywheel scraper",
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
                        "title": "Flywheel scraper",
                        "description": message,
                        "color": color,
                    }
                ],
            }

    def scrape(self, cut_off_date):
        try:
            self.__log_to_discord("🏁 Initiating Flywheel Scraper 🔧", color=65280)
            # better send cuttof date as string from db
            # i guess it's done like this since we dk how each scraper is expecting it's date format to be in
            updated_urls = self.__scrape_updated_urls(cut_off_date)
            if updated_urls is None:
                self.__log_to_discord(
                    "🚫 No pages in FLywheel substack are found to have updates! 🚫"
                )
                return None
            else:
                df = self.__scrape_content(updated_urls)
                if df is not None:
                    self.__log_to_discord(
                        f" Total pages scraped = {df.shape[0]} 🚀",
                        color=65280,
                    )
                    return df
                else:
                    self.__log_to_discord(
                        "The pages updated into Flywheel after last cron doesnt have content!🙄",
                        color=16753920,
                    )
                    return None

        except Exception as e:
            self.__log_to_discord(f"❌ Error during Flywheel Scraper ❌\n{e}")
            print(f"❌ Error during Flywheel Scraper ❌\n{e}")

        finally:
            self.__log_to_discord(
                "Scraping Successful ✅\n\nExit Flywheel Scraping 🏁", color=65280
            )


obj = FlyWheel()
start_date = datetime(2023, 4, 1, tzinfo=timezone.utc)
end_date = datetime(2023, 5, 1, tzinfo=timezone.utc)

random_dt = obj.random_date(start_date, end_date)
random_dt_str = random_dt.strftime("%Y-%m-%d")
print(obj.scrape(random_dt))
