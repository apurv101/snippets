from bs4 import BeautifulSoup
import urllib.request
from urllib.error import HTTPError
# import pymongo 
from pprint import pprint
import re
import json
import math
import os
import sys







layer_link = sys.argv[1]
table_name = sys.argv[2]
format_ = sys.argv[3]
geom_t = sys.argv[4]




def get_soup(link):
    fp = urllib.request.urlopen(link)
    mybytes = fp.read()
    mystr = mybytes.decode("utf8")
    fp.close()
    soup = BeautifulSoup(mystr, 'html.parser')
    return soup





def get_data(layer_link):
    data = None
    with urllib.request.urlopen(layer_link) as url:
        data = json.loads(url.read().decode())
    return data


def extract_data(layer_link, table_name, format_, geom_t):
        
    print(layer_link)
    
    # id_link = (layer_link + "/query" + "?where=1%3D1&returnIdsOnly=true&f=json")
    # id_data = get_data(id_link)
    
    # if id_data['objectIds']:
    #     start = math.floor(min(id_data['objectIds'])/1000) * 1000
    #     end = math.ceil(max(id_data['objectIds'])/1000) * 1000
    # else:
    #     start = 0
    #     end = 0
    # print('TOTAL RANGE')
    # print(start)
    # print(end)

    start = 3405000
    end = 17000000

    # count = 
    
    for i in range(start, end, 1000):
        from_ = i
        to_ = i + 1000
        print('Starting from ', from_, ' to ', to_)
        link = f"""{layer_link}/query?where=OBJECTID+%3E%3D+{from_}+and+
        OBJECTID+%3C+{to_}&time=&geometry=&geometryType=esriGeometryEnvelope&
        inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&
        returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&
        featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&
        geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&
        returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&
        returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&
        cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&
        having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&
        returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f={format_}&token=""".replace("\n", "").replace(" ", "")

        trying = True

        while trying:
            try: 
                print(link)
                x = urllib.request.urlretrieve(link, f"{table_name}_test.geojson")
                print(x)
                trying = False
            except:
                trying = True


        query = f'ogr2ogr -f "PostgreSQL" PG:"host=raw-data.c91397hbzfpf.us-east-1.rds.amazonaws.com user=postgres dbname=db_karnataka password=arcgisvault" {table_name}_test.geojson -nln "{table_name}" -nlt {geom_t}'

        # query = f'ogr2ogr -f "PostgreSQL" PG:"dbname=test" {table_name}_test.geojson -nln "{table_name}" -nlt {geom_t}'
        print(query)
        y = os.system(query)
        print(y)

    return (start, end)





start, end, count = extract_data(layer_link, table_name, format_, geom_t)


# python3 service_url_custom.py 'https://kgis.ksrsac.in/kgismaps/rest/services/CadastralData_Admin/Dynamic_CadastralData_Admin/MapServer/0/query' 'state' geojson MULTIPOLYGON





