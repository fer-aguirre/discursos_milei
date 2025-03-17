from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import logging
import datetime
import pandas as pd
import numpy as np
import click
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

def check_csv_exists_and_fetch_missing(keyword, base_url):
    """
    Check if a CSV file exists for the given keyword, read it, and fetch missing URLs.

    Args:
        keyword (str): The keyword used to name the CSV file.
        base_url (str): The base URL to search for articles.

    Returns:
        list: A list of new URLs not present in the existing CSV.
    """

    csv_file = os.path.join(".", "data", f"discursos_{keyword}.csv")
    existing_urls = []

    if os.path.exists(csv_file):
        existing_data = pd.read_csv(csv_file)
        existing_urls = existing_data['url'].tolist()

    current_urls = get_discursos_urls(base_url, keyword)
    new_urls = list(set(current_urls).difference(existing_urls))

    return new_urls


def get_discursos_urls(base_url, keyword):
    """
    Fetch URLs from a given base URL that contain a specified keyword.

    Args:
        base_url (str): The base URL to search for articles.
        keyword (str): The keyword to filter URLs.

    Returns:
        list: A list of URLs containing the keyword.

    Logs an error message if the request to the base URL fails.
"""
    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as e:
        logging.error(f"Error fetching {base_url}: {e}")
        return []
    article_selector = "html body div#jm-allpage.nofluid div#jm-mainpage div#jm-mainpage-in div#jm-main.lcr.scheme1.nocolumns.clearfix div#jm-maincontent main.home-special.home-mid div.container section div.row.row-extra.row-news.row-clear-4 div.blog div.contentboxes div.box.col-sm-6.col-md-3 div.item"
    articles = soup.select(article_selector)

    discursos_urls = [
        urljoin(base_url, a.get('href'))
        for article in articles
        for a in article.find_all("a")
        if keyword in a.get("href").lower()
    ]
    return discursos_urls


# Define a dictionary to map Spanish month names to numbers
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


def get_content(url, session):
    """
    Fetches and parses the HTML content of a given URL using a provided session.

    Args:
        url (str): The URL to fetch the content from.
        session (requests.Session): The session object to use for making the request.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML content if successful.
        None: If an error occurs during the request.

    Logs:
        Logs an error message if the request fails.
    """
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        if response.status_code != 200:
            logging.error(f"Failed to fetch the URL: {url} (Status code: {response.status_code})")
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        return soup
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None


def get_title(soup):
    """
    Extracts the text of the first <h2> element from a BeautifulSoup object.

    Parameters:
        soup (BeautifulSoup): A BeautifulSoup object representing the HTML document.

    Returns:
        str: The text content of the first <h2> element, or NaN if no <h2> is found.
    """
    h2_tag = soup.find("h2")
    return h2_tag.text if h2_tag else np.nan

def get_article_content(soup):
    """
    Extracts and returns the text content of an article from a BeautifulSoup object.

    This function searches for the first <article> tag within the provided BeautifulSoup
    object and concatenates the text from all <p> tags that do not contain a <strong> tag.
    If no <article> tag is found, it returns np.nan.

    Parameters:
        soup (BeautifulSoup): A BeautifulSoup object representing the HTML content.

    Returns:
        str or float: The concatenated text of the article's paragraphs, or np.nan if no
        article is found.
    """
    article = soup.find("article")
    return "".join([p.text for p in article.find_all("p") if p.find("strong") is None]) if article else np.nan

def get_date(soup):
    """
    Extracts and formats a date from an HTML document.

    This function retrieves a date string from a BeautifulSoup object, removes any
    day of the week, replaces Spanish month names with their numerical equivalents,
    and converts the date into the 'YYYY-MM-DD' format.

    Parameters:
        soup (BeautifulSoup): A BeautifulSoup object containing the HTML to parse.

    Returns:
        str: The formatted date string in 'YYYY-MM-DD' format.
    """
    time_tag = soup.find("time")
    date = time_tag.text.replace("\r", "").replace("\n", "") if time_tag else np.nan
    # Remove the day of the week from the date string
    date = " ".join(date.split()[1:])
    # Replace the Spanish month name with its corresponding number
    for month_name, month_number in month_map.items():
        date = date.replace(month_name, month_number)
    # Convert the date string to a date object and format it in 'YYYY-MM-DD'
    try:
        return datetime.datetime.strptime(date, "%d de %m de %Y").strftime("%Y-%m-%d")
    except ValueError as e:
        logging.error(f"Date parsing error: {e}")
        return np.nan


def create_dataframe(discursos_urls):
    """
    Creates a DataFrame from a list of URLs by extracting titles, content, and dates.

    This function iterates over a list of URLs, fetching and parsing the HTML content
    of each URL using a session. It extracts the title, article content, and date from
    each page and compiles these into a DataFrame.

    Args:
        discursos_urls (list of str): A list of URLs to process.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted titles, content, dates, and URLs.
    """
    data = {"title": [], "content": [], "date": [], "url": []}
    with requests.Session() as session:
        for url in discursos_urls:
            soup = get_content(url, session)
            if soup:
                title = get_title(soup)
                content = get_article_content(soup)
                date = get_date(soup)
                data["title"].append(title)
                data["content"].append(content)
                data["date"].append(date)
                data["url"].append(url)
    return pd.DataFrame(data)

@click.command()
@click.option('--keyword', default="milei", help='The keyword to search for in URLs.')
def main(keyword):
    """
    Command-line interface for collecting and saving speech data.

    This script fetches URLs containing speeches from a specified base URL using a keyword,
    extracts relevant data from each URL, and saves the data to a CSV file.

    Args:
        keyword (str): The keyword to search for in URLs. Defaults to "milei".

    The resulting CSV file is named using the keyword and contains the extracted data.
    """
    # Example usage
    csv_file = os.path.join(".", "data", f"discursos_{keyword}.csv")
    base_url = "https://www.casarosada.gob.ar/informacion/discursos/"
    new_urls = check_csv_exists_and_fetch_missing(keyword, base_url)
    
    if new_urls:
        click.echo("New data found. Fetching new data...")
        new_data = create_dataframe(new_urls)
        try:
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)
            if os.path.exists(csv_file):
                existing_data = pd.read_csv(csv_file)
                combined_data = pd.concat([existing_data, new_data], ignore_index=True)
                combined_data.sort_values(by='date', ascending=False, inplace=True)
                combined_data.to_csv(csv_file, index=False)
            else:
                new_data.to_csv(csv_file, index=False)
            click.echo(f"Data updated and saved to {csv_file}")
        except (pd.errors.EmptyDataError, FileNotFoundError) as e:
            logging.error(f"Error reading CSV file: {e}")
        except IOError as e:
            logging.error(f"Error writing to CSV file: {e}")
        
    else:
        discursos_urls = get_discursos_urls(base_url, keyword)
        df = create_dataframe(discursos_urls)
        df.to_csv(csv_file, index=False)
        click.echo(f"Data collected and saved to {csv_file}")

if __name__ == "__main__":
    main()