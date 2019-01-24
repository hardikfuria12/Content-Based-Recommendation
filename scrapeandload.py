import requests
import json
import os
from bs4 import BeautifulSoup
import re

cwd = str(os.getcwd())
#print(cwd)
MAIN_LINK = 'https://en.wikibooks.org'
from elasticsearch import Elasticsearch
import json
import os
ES_CLUSTER = 'http://localhost:9200/'
ES_INDEX = 'hardikfuria'
ES_TYPE = 'doc'
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def main():
    print('Scrapping Scripting is running...')
    book_link = 'https://en.wikibooks.org/wiki/Java_Programming'
    req_inst = requests.get(book_link)
    scrapper = BeautifulSoup(req_inst.content, features="lxml")
    initDoc = scrapper.find('span', class_='mw-headline').find_parent()
    initDoc = initDoc.find_next_sibling()
    while (initDoc):
        for i in initDoc.find_all("a", href=re.compile("/wiki/Java_Programming/")):
            link = MAIN_LINK + i.get("href")
            description_page(link)
        initDoc = initDoc.find_next_sibling()
    uploadtoES()

def description_page(link):
    req_inst = requests.get(link)
    scrapper = BeautifulSoup(req_inst.content, features="lxml")
    scrapped_data = {}
    table_holder = scrapper.find('table', class_='wikitable')
    if (table_holder):
        table_holder = table_holder.find_next_sibling()
    heading = 'introduction_' + link.split("/")[-1]
    scrapped_data['heading'] = heading
    scrapped_data['content'] = ""
    while (table_holder and table_holder.name != 'h2'):
        if len(table_holder.text) > 10:
            scrapped_data['content'] = scrapped_data['content'] + ' ' + table_holder.text
            # print(table_holder.text)
        table_holder = table_holder.find_next_sibling()
    json_opfile = "KnowledgeDataset/" + heading + ".json"
    print(json_opfile)
    with open(json_opfile, 'w') as f:
        json.dump(scrapped_data, f)
    while (table_holder):
        if (table_holder.name == 'h2'):
            scrapped_data = {}
            heading = table_holder.find('span', class_='mw-headline').text
            scrapped_data['heading'] = heading
            scrapped_data['content'] = ""
            # print(table_holder.find('span', class_='mw-headline').text)
            content = table_holder.find_next_sibling()
            while (content and content.name != 'h2'):
                if (content.attrs.get('class') and content.attrs.get('class')[0] == 'collapsible'):
                    content = content.find_next_sibling()
                    continue
                if (len(content.text) > 5):
                    scrapped_data['content'] += '\n' + content.text
                    # print(content.text)
                content = content.find_next_sibling()
            if (content and content.attrs.get('class') and content.attrs.get('class')[0] == 'collapsible'):
                content = content.find_next_sibling()
                continue
            table_holder = content
            json_opfile = cwd + "\\KnowledgeDataset\\" + link.split("/")[-1] + "_" + heading.replace('/', '_') + ".json"
            json_opfile = json_opfile.replace('?', '')
            json_opfile = json_opfile.replace('<', '')
            json_opfile = json_opfile.replace('>', '')

            print(json_opfile)

            with open(json_opfile, 'w') as f:
                json.dump(scrapped_data, f)
def uploadtoES():
    print('Uploading documents to Elastic Search')
    i=1
    for filename in os.listdir('KnowledgeDataset'):
        print(filename)
        filename = str(os.getcwd()) + '\KnowledgeDataset\\' + filename
        if filename.endswith('.json'):
            jsonFile = open(filename)
            jsonContent = jsonFile.read()
            es.index(index='try1', ignore=400, doc_type='content', id=i, body=json.loads(jsonContent))
            i = i + 1
            # with open(filename) as open_file:
            # json_docs.append(json.load(open_file))

    # es.bulk(ES_INDEX, ES_TYPE, json_docs)

    print(i)


if __name__ == "__main__":
    print("Welcome to Stack over flow scarpping script")
    main()

# description_page('https://en.wikibooks.org/wiki/Java_Programming/Loop_blocks')
