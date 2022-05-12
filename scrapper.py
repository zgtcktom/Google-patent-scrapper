import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from fake_useragent import UserAgent

import threading
from multiprocessing.pool import ThreadPool

from optparse import OptionParser
parser = OptionParser()
parser.add_option('-p', '--pool_size', type='int', default=16, help='multithread pool size')
options, args = parser.parse_args()

pool = ThreadPool(options.pool_size)

lock = threading.Lock()

dir_path = 'data'
out_dir = 'out'
debug = False
col_name = 'abstract'

if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    
ua = UserAgent()

def request_page(url):
    headers = {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'
    }
    
    response = requests.get(url, headers=headers)
    
    return response

def extract_page(text):
    soup = BeautifulSoup(text, 'lxml')
    element = soup.find('abstract', {})
    
    if element is not None:
        return element.text.strip()
    
    return None
    
def extract(url, retry=False):
    with lock:
        global count
        if not retry:
            count = count + 1
        print(f'{count} / {len(df)}: requesting {url}')

        if debug:
            with open('result.html', 'w', encoding='utf-8') as file:
                file.write(response.text)
    
    response = request_page(url)
    text = extract_page(response.text)

    while text is None:
        print('retrying')
        return extract(url, retry=True)
        
    return text
    
for name, path in ((name, os.path.join(dir_path, name)) for name in os.listdir(dir_path)):
    print(f'reading csv file: {name}')
    
    df = pd.read_csv(path, skiprows=1)
    
    count = 0

    results = pool.map(extract, list(df['result link']))

    df[col_name] = results
    
    save_path = os.path.join(out_dir, name)
    print(f'saving to: {save_path}')
    df.to_csv(save_path, encoding='utf_8_sig')
    
    