from bs4 import BeautifulSoup
import urllib.request
from urllib.error import HTTPError
import pymongo 
from pprint import pprint
import re
import json
import math
import os
import sys


## python service_url_scraper.py USCACISBB https://services3.arcgis.com/hMpg7vsYb74pEKjX/ArcGIS/rest/services 'Santa Barbara' 'City'


print(sys.argv[1])
service_uid = sys.argv[1]

if(len(sys.argv) > 2):
	supp_service_link = sys.argv[2]
	supp_name = sys.argv[3]
	supp_type = sys.argv[4]

def get_soup(link):
    fp = urllib.request.urlopen(link)
    mybytes = fp.read()
    mystr = mybytes.decode("utf8")
    fp.close()
    soup = BeautifulSoup(mystr, 'html.parser')
    return soup



def traverse(root_link, all_layers):
    try: 
        soup = get_soup(root_link)
        if 'layer' in soup.find('title').text.lower():
            return
    except HTTPError as err:
        if err.code == 404:
            return
    uls = soup.find_all('ul')
    for ul in uls:
        nested_uls_no = len(ul.find_all('ul'))
        if nested_uls_no == 0:
            ps = ul.previous_sibling
            name = ps.name
            link_type = ''
            while name != 'b':
                ps_temp = ps
                ps = ps.previous_sibling
                if ps is None:
                    ps = ps_temp.parent
                name = ps.name
            if 'folder' in ps.text.lower():
                link_type = 'folder'
            if 'service' in ps.text.lower():
                link_type = 'service'
            if 'table' in ps.text.lower():
                link_type = 'table'
            if 'layer' in ps.text.lower():
                link_type = 'layer'
            lis = ul.find_all('li')
            for li in lis:
                link = li.find('a')
                if link is None:
                    return
                link_name = li.text
                part1 = root_link.split('//')[0]
                part2 = root_link.split('//')[1].split('/')[0]
                traversal_link = part1 + '//' + part2 + li.find('a').get('href')
                print(link_name)
                if link_type == 'layer':
                    layer_object = {}
                    layer_object['link'] = traversal_link
                    layer_object['name'] = link_name.strip()
                    all_layers.append(layer_object)
                traverse(traversal_link, all_layers)
    return all_layers


def get_data(layer_link):
    data = None
    with urllib.request.urlopen(layer_link) as url:
        data = json.loads(url.read().decode())
    return data


def extract_data(layer_link, table_name):
        
    print(layer_link)
    
    id_link = (layer_link + "/query" + "?where=1%3D1&returnIdsOnly=true&f=json")
    id_data = get_data(id_link)
    
    if id_data['objectIds']:
        start = math.floor(min(id_data['objectIds'])/1000) * 1000
        end = math.ceil(max(id_data['objectIds'])/1000) * 1000
    else:
        start = 0
        end = 0
    print(start)
    print(end)
    
    for i in range(start, end, 1000):
        from_ = i
        to_ = i + 1000
        link = f"""{layer_link}/query?where={id_data['objectIdFieldName']}+%3E%3D+{from_}+and+
        {id_data['objectIdFieldName']}+%3C+{to_}&time=&geometry=&geometryType=esriGeometryEnvelope&
        inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&
        returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&
        featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&
        geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&
        returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&
        returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&
        cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&
        having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&
        returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=json&token=""".replace("\n", "").replace(" ", "")
        print(link)
        x = urllib.request.urlretrieve(link, "test.geojson")
        print(x)
        query = f'ogr2ogr -f "PostgreSQL" PG:"host=raw-data.c91397hbzfpf.us-east-1.rds.amazonaws.com user=postgres dbname=db1 password=arcgisvault" test.geojson -nln "{table_name}" -nlt {geom_t}'
        print(query)
        y = os.system(query)
        print(y)




### connection to mongo
client = pymongo.MongoClient("""mongodb://apoorv:M0ngoParce1n1@cluster0-shard-00-00.mpmef.mongodb.net:27017,
	cluster0-shard-00-01.mpmef.mongodb.net:27017,
	cluster0-shard-00-02.mpmef.mongodb.net:27017/myDB?ssl=true&replicaSet=atlas-7nzlgf-shard-0&authSource=admin&retryWrites=true&w=majority""")

db = client.arcgisLibrary
coll = db.directories
doc_object = None

doc_object = coll.find_one({ "_id": service_uid })


if doc_object is not None:
	supp_service_link = doc_object['root']
else:
	if(len(sys.argv) <= 2):
		raise 'Arcgis Link and Title are required'
	else:
		arcgis_link = {
		    '_id': service_uid,
		    'name': supp_name,
		    'jurisdiction': supp_type,
		    'root': supp_service_link
		}
		# print(arcgis_link)
		# db_arcgis = client.arcgisLibrary
		result = coll.insert_one(arcgis_link)
		doc_object = coll.find_one({ "_id": service_uid })
		supp_service_link = doc_object['root']



all_layers = traverse(supp_service_link, [])




for i in range(len(all_layers)):
    layer_link = all_layers[i]['link']
    soup = get_soup(layer_link)
    geom_elem = soup('b',text=re.compile(r'Geometry Type:'))[0]
    geom_type = geom_elem.next_sibling.strip()
    if geom_type == 'esriGeometryPolygon':
        geom_t = 'MULTIPOLYGON'
    if geom_type == 'esriGeometryPolyline':
        geom_t = 'MULTILINESTRING'
    if geom_type == 'esriGeometryPoint':
        geom_t = 'POINT'
    name = None
    mrc = None
    dfc = None
    description = None

    name_elems = soup('b',text=re.compile(r'(?i)\bname\b'))
    if len(name_elems) > 0:
    	name = name_elems[0].next_sibling.strip()

    mrc_elems = soup('b',text=re.compile(r'(?i)\bmax\b\s\brecord\b\s\bcount\b'))
    if len(mrc_elems) > 0:
    	mrc = int(mrc_elems[0].next_sibling.strip())

    df_elems = soup('b',text=re.compile(r'(?i)\bdisplay\b\s\bfield\b'))
    if len(df_elems) > 0:
    	dfc = df_elems[0].next_sibling.strip()

    desc_elems = soup('b',text=re.compile(r'(?i)\bdescription\b'))
    if len(desc_elems) > 0:
    	description = desc_elems[0].next_sibling.strip()


    layer_mng_obj = {}
    layer_mng_obj['_id'] = service_uid + '_' + str(i)
    layer_mng_obj['parent'] = service_uid
    layer_mng_obj['layer_name'] = all_layers[i]['name']
    layer_mng_obj['geom_t'] = geom_t
    layer_mng_obj['arcgeom_t'] = geom_type
    layer_mng_obj['essential'] = True
    layer_mng_obj['name'] = name
    layer_mng_obj['max_record_count'] = mrc
    layer_mng_obj['display_fields'] = dfc
    layer_mng_obj['description'] = description


    extract_data(layer_link, service_uid + '_' + str(i))

    result = coll.insert_one(layer_mng_obj)

