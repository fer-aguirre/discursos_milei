import csv, datetime, logging, asyncio, requests
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv(filename):
    """
    Reads the CSV file and returns the 'url' column as a list.
    """
    try:
        df = pd.read_csv(filename)
        return df["url"].tolist()
    except FileNotFoundError:
        logging.error(f"File {filename} not found.")
        return []
    except pd.errors.EmptyDataError:
        logging.warning(f"File {filename} is empty.")
        return []
    except Exception as e:
        logging.error(f"Error reading {filename}: {e}")
        return []


def append_to_csv(data, filename):
    """
    Appends the data to a CSV file.
    """
    try:
        with open(filename, "a", newline="") as csvfile:
            fieldnames = ["title", "content", "date", "url"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for row in data:
                writer.writerow(row)
    except IOError as e:
        logging.error(f"Error writing to {filename}: {e}")


def get_discursos_urls(base_url, keyword):
    """
    Fetches URLs containing the keyword from the base URL.
    """
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as e:
        logging.error(f"Error fetching {base_url}: {e}")
        return []
    article_selector = "html body div#jm-allpage.nofluid div#jm-mainpage div#jm-mainpage-in div#jm-main.lcr.scheme1.nocolumns.clearfix div#jm-maincontent main.home-special.home-mid div.container section div.row.row-extra.row-news.row-clear-4 div.blog div.contentboxes div.box.col-sm-6.col-md-3 div.item"
    articles = soup.select(article_selector)

    discursos_urls = [
        f"{base_url}{a.get('href')}"
        for article in articles
        for a in article.find_all("a")
        if keyword in a.get("href").lower()
    ]
    return discursos_urls


async def get_content(url):
    """
    Asynchronously fetches and parses HTML content from the URL.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                html = await response.text()
                return BeautifulSoup(html, "html.parser")
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching {url}: {e}")
        return None


def get_title(soup):
    """
    Extracts the title from the parsed HTML content.
    """
    title = soup.find("h2")
    return title.text if title else None


def get_article_content(soup):
    """
    Extracts the article content from the parsed HTML content.
    """
    article = soup.find("article")
    return (
        "".join([p.text for p in article.find_all("p") if p.find("strong") is None])
        if article
        else None
    )


def get_date(soup):
    """
    Extracts and formats the date from the parsed HTML content.
    """
    month_map = {
        "enero": "01",
        "febrero": "02",
        "marzo": "03",
        "abril": "04",
        "mayo": "05",
        "junio": "06",
        "julio": "07",
        "agosto": "08",
        "septiembre": "09",
        "octubre": "10",
        "noviembre": "11",
        "diciembre": "12",
    }

    date = soup.find("time")
    if date:
        date = date.text.replace("\r", "").replace("\n", "")
        date = " ".join(date.split()[1:])
        for month_name, month_number in month_map.items():
            date = date.replace(month_name, month_number)
        return datetime.datetime.strptime(date, "%d de %m de %Y").strftime("%Y-%m-%d")
    else:
        return None


async def create_data(discursos_urls):
    """
    Creates a list of dictionaries from the list of URLs.
    """
    data = []
    tasks = [get_content(url) for url in discursos_urls]
    soups = await asyncio.gather(*tasks)
    for url, soup in zip(discursos_urls, soups):
        if soup is None:
            continue
        title = get_title(soup)
        content = get_article_content(soup)
        date = get_date(soup)
        data.append({"title": title, "content": content, "date": date, "url": url})
    return data


def write_to_csv(data, filename):
    """
    Writes the data to a CSV file.
    """
    try:
        with open(filename, "w", newline="") as csvfile:
            fieldnames = ["title", "content", "date", "url"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in data:
                writer.writerow(row)
    except IOError as e:
        logging.error(f"Error writing to {filename}: {e}")


def main():
    asyncio.run(main_async())

async def main_async():
    base_url = "https://www.casarosada.gob.ar/informacion/discursos/"
    keyword = "milei"
    filename = "./data/discursos_milei.csv"

    discursos_urls = get_discursos_urls(base_url, keyword)
    existing_urls = read_csv(filename)

    new_urls = [url for url in discursos_urls if url not in existing_urls]

    if new_urls:
        data = await create_data(new_urls)
        append_to_csv(data, filename)
    else:
        logging.info("No new URLs found.")


if __name__ == "__main__":
    main()
