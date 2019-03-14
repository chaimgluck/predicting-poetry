# Imports.
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
from time import time

# Url contains list of all poets on site.
all_poets_page = requests.get('http://www.famouspoetsandpoems.com/poets.html')
all_poets_page = BeautifulSoup(all_poets_page.text, 'lxml')

def grab_poets_and_years(all_poets_page):
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

poets = grab_poets_and_years(all_poets_page)
poets_info = map(extract_poet_info, poets)
poets_df = pd.DataFrame(poets_info, columns=['name', 'number', 'years'])
poets_df.to_csv('poets_years.csv', index=False)

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

all_poet_pages = extract_poet_links(all_poets_page)
all_poem_links = map(get_poems, all_poet_pages)
all_poem_links_list = [item for sublist in all_poem_links for item in sublist]

# This would take 3-4 hours.
start = time()
poems_soup = []
for link in tqdm(all_poem_links_list, desc='Scrape all poems'):
    url = requests.get(link)
    soup = BeautifulSoup(url.text, 'lxml')
    poem = soup.find('div', style="padding-left:14px;padding-top:20px;font-family:Arial;font-size:13px;")
    for tag in soup('span'):
        if 'by' in tag.get_text():
            poet = tag.get_text().strip()
    poems_soup.append([poem, poet])
print((time() - start) / 60)

## Creating a DataFrame from all the poems and poets.
df_all = pd.DataFrame(poems_soup)
df_all.to_csv('all_poets.csv')