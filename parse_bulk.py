#!/usr/bin/python

import sys
import re
from datetime import datetime
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions, helpers
import logging
import xml.etree.ElementTree as ET

logging.basicConfig(filename='parse.log',level=logging.INFO)
es = Elasticsearch()  # use default of localhost, port 9200

with open(sys.argv[0], 'r', encoding="utf8") as f:
        data=f.read()

recs = data.split("<PubmedArticle>");
# drop preamble
recs.pop(0)

articles = []

def parse_mesh(heading_list):
        return_lst = []

        for item in heading_list:
                item = "<MeshHeading>" + item + "</MeshHeading>"
                root = ET.fromstring(item)

                descriptor = root.find("DescriptorName").text.strip()
                qualifier = list(map(lambda k: k.text.strip(), root.findall("QualifierName")))

                if qualifier:
                        for qualifier in qualifier:
                                temp = f"{descriptor} ({qualifier})"

                                return_lst.append(temp)
                else:
                        return_lst.append(descriptor)
        
        return return_lst

for r in recs:
        pmid = re.findall('<PMID Version="1">(.*?)</PMID>', r)
        if pmid:
                pmid = pmid[0]
        else:
                pmid = ""
                        
        title = re.findall('<ArticleTitle>(.*?)</ArticleTitle>', r)
        if title:
                title = title[0]
        else:
                title = ""
                
        abstract = re.findall('<Abstract>([\s\S]*?)</Abstract>', r)

        if abstract:
                abstract = re.sub("\n\s*", "", abstract[0])
                abstract = re.sub('<AbstractText Label="(.*?)".*?>', "\\1: ", abstract)
                abstract = re.sub('</AbstractText>', "", abstract)
        else:
                abstract = ""

        type = re.findall("<PublicationType UI=.*?>(.*?)</PublicationType>", r)
        if type:
                type = str(type)
        else:
                type = str([])

        mesh_heading_list = re.findall("<MeshHeading>(.*?)</MeshHeading>", r, flags=re.DOTALL)
        if mesh_heading_list:
                mesh_heading_list = parse_mesh(mesh_heading_list)
        else:
                mesh_heading_list = []

        articles.append(
                {'_index': 'medline', 
                '_type': 'article', 
                "_op_type": 'index', 
                '_source': {"pmid": pmid, 
                            "title": title, 
                            "abstract": abstract, 
                            "timestamp": datetime.now().isoformat(), 
                            "type": type, 
                            "mesh": mesh_heading_list}})

res = helpers.bulk(es, articles)#, raise_on_exception=False)
logging.info(datetime.now().isoformat() + " imported " + str(res[0]) + " records from " + sys.argv[1])
