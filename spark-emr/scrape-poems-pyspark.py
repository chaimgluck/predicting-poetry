# Imports.
from bs4 import BeautifulSoup
import pandas as pd
import requests
from contextlib import closing
import re
from time import time
import logging
import sys
from io import BytesIO
import boto3
from pyspark import SparkContext

# S3 bucket to save output.
bucket = sys.argv[1]

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

# This page contains links to each poet's bio page.
all_poets_page = requests.get('http://www.famouspoetsandpoems.com/poets.html')
all_poets_page = BeautifulSoup(all_poets_page.text, 'lxml')

def grab_poet_info(all_poets_page):
    '''Input - page containing all poet info
    Output - list of all poets and their info'''
    poets = list()
    for tag in all_poets_page.findAll('td'):
        if '(' in tag.get_text():
            poets.append(tag.get_text().strip())
    poets = [x.strip() for x in poets]
    poets = poets[3:]
    poets = poets[::2]
    return poets

def extract_poet_info(poet_string):
    '''Extract name, number of poems, and years of poet's life from string'''
    poet_name = re.findall('^[^\(]+', poet_string)[0].strip()
    number_of_poems = re.findall('\((.*?)\)', poet_string)[0]
    poet_years = re.findall('\((.*?)\)', poet_string)[1]
    return poet_name, number_of_poems, poet_years

def extract_poet_links(all_poets_page):
    '''Extract links to each poet's page from page which lists them all'''
    poet_links = list()
    for tag in all_poets_page.findAll('td'):
        try:
            link = tag.find('a')['href']
            if '/poets/' in link:
                poet_links.append(link)
        except:
            pass
    poet_links = list(set(poet_links))
    base = 'http://www.famouspoetsandpoems.com'
    poet_pages = [base + poet + '/poems' for poet in poet_links]
    return poet_pages

def get_poems(poet_page):
    '''Extract all links to individual poem pages from pages of poets. Takes about 4.5 minutes.'''
    poet_page = requests.get(poet_page)
    bib_soup = BeautifulSoup(poet_page.text, 'lxml')

    raw_poem_links = list()
    for poems in bib_soup.findAll('td'):
        try:
            poem = poems.find('a')['href']
            if '/poems/' in poem:
                raw_poem_links.append(poem)
        except:
            pass
    raw_poem_links = list(set(raw_poem_links))
    poem_links = ['http://www.famouspoetsandpoems.com' + poem for poem in raw_poem_links]
    return poem_links

def scrape_poem(url):
    '''Extract one poem, and its poet, from its page'''
    with closing(requests.get(url, stream=True)) as resp:
        page = resp.text
    soup = BeautifulSoup(page, 'lxml')
    poem = soup.find('div', style="padding-left:14px;padding-top:20px;font-family:Arial;font-size:13px;")
    poem = str(poem)
    poem = BeautifulSoup(poem.replace('<br/>', ' ')).get_text().strip()
    for tag in soup('span'):
        if 'by' in tag.get_text():
            poet = tag.get_text().strip()
        else:
            poet = ''
    return (poem, poet)

def df_to_s3(df, bucket, filepath):
    s3_resource = boto3.resource('s3')
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding = 'utf-8')
    s3_resource.Object(bucket, filepath).put(Body=csv_buffer.getvalue())

if __name__ == '__main__':
    sc = SparkContext(appName="PoemScraper")
    logging.info('Scraping has commenced!')
    poets = grab_poet_info(all_poets_page)
    poets_info = map(extract_poet_info, poets)
    poets_df = pd.DataFrame(poets_info, columns=['name', 'number', 'years'])
    poet_info_file = 'poet_info.csv'
    df_to_s3(poets_df, bucket, poet_info_file)
    logging.info('Wrote poet info to file {}'.format(poet_info_file))

    all_poet_pages = extract_poet_links(all_poets_page)
    logging.info('Scraping poems.')
    poets_rdd = sc.parallelize(all_poet_pages, 30)
    poets_rdd = poets_rdd.flatMap(get_poems)
    poets_rdd = poets_rdd.repartition(400)
    poets_rdd = poets_rdd.map(scrape_poem)
    result = poets_rdd.collect()
    num_poems = len(result)
    logging.info('Poem scraping complete. {} poems scraped'.format(str(num_poems)))
    df = pd.DataFrame(result)
    all_poems_file = 'clean_poems.csv'
    df_to_s3(df, bucket, all_poems_file)
    logging.info('Wrote all poems to file {}'.format(all_poems_file))
    logging.info('Application complete')
