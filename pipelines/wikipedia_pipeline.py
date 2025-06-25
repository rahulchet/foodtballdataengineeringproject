from fileinput import filename

import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import re
import json

from pandas import DataFrame
from sqlalchemy.sql.sqltypes import NULLTYPE


def get_wikipedia_page(url):
    print("getting wiikipedia URl", url)

    try:
        page = requests.get(url)
        page.raise_for_status()
        return page.text
    except Exception as e:
        print(e)
        return None


get_wikipedia_page(r"https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]

    table_rows = table.find_all("tr")
    return table_rows


def extract_wikipedia_data(**kwargs):
    logger = logging.getLogger("airflow.task")
    url = kwargs['url']
    logger.info(f"extracting wikipedia data from {url}")

    html = get_wikipedia_page(url)
    if html is None:
        raise Exception(f"wikipedia page not found at {url}")
    table_rows = get_wikipedia_data(html)
    logger.info(f"extracted {len(table_rows)} rows")
    expected_keys = {"Rank", "Stadium", "Capacity", "Region", "Country", "City", "Image", "Home_Team"}
    data = []

    data = []

    for i in range(1, len(table_rows)):
        # print(rows[i])
        tds = table_rows[i].find_all("td")
        values = {
            "Rank": i,
            "Stadium": tds[0].find("a").get("title") if tds[0].find("a") else tds[0].text.strip(),
            "Capacity": re.sub(r"\[.*?\]", "", tds[1].text.strip("\n")),
            "Region": tds[2].text.strip(),
            "Country": tds[3].find("a").get("title") if tds[3].find("a") else tds[3].text.strip(),
            "City": tds[4].find("a").get("title") if tds[4].find("a") else tds[4].text.strip(),
            "Image": "https:" + tds[5].find("img").get("src") if tds[5].find("img") else None,
            "Home_Team": tds[6].text.strip()
        }

        data.append(values)

    # df = pd.DataFrame(data)

    # df.to_csv(r"C:\Users\Lenovo\Downloads\file.csv", index=False)
    json_rows = json.dumps(data, indent=4)

    kwargs["ti"].xcom_push(key="data", value=json_rows)

    return  "ok"


def transform_wikidata_data(**kwargs):
    ti =kwargs["ti"]
    data = ti.xcom_pull("extract_data_from_wiki",key="data")

    data= json.loads(data)
    stadiums_df=pd.DataFrame(data)

    stadiums_df["Image"] = stadiums_df["Image"].apply(lambda x: x if x not in [None,"",NULLTYPE] else "No_Image" )

    stadiums_df=stadiums_df.drop_duplicates()

    kwargs["ti"].xcom_push(key="data", value=stadiums_df.to_json())

    return "ok"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    ti = kwargs['ti']

    data=ti.xcom_pull("transform_data_from_wiki",key="data",)

    data=json.loads(data)
    data=pd.DataFrame(data)

    filename= ("stadium_cleaned_data"+ datetime.now().strftime("%Y%m%d-%H%M%S") +".csv")

    #data.to_csv('data/'+ filename, index=False)

    ##

    data.to_csv('abfs://footballengineeringcont@footballengineeringdata.dfs.core.windows.net/data/'+ filename,
                storage_options={
                    'AZURE_KEY = "your-very-long-secret-key"
                },
                index=False)







#if __name__ == "__main__":
    #url = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
    #html = get_wikipedia_page(url)
    #data = get_wikipedia_data(html)
    #finaldata =extract_wikipedia_data(url=url, data=data)
    # print(f"Extracted {len(rows)} rows")
    # print(rows[:3])  # Print sample rows
    # print(data)
    #df = pd.DataFrame(finaldata)
    # print(df.head())
    #df.to_csv(r"C:\Users\Lenovo\Documents\project_trainings\FootballDataengineering\data\outputdata.csv",index=False)
