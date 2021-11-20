import requests
import pathlib
import pdal
import json
import pyproj
import pandas as pd
from shapely.geometry.polygon import Polygon
from sklearn.cluster import DBSCAN
import numpy as np




def pipeline_to_df(pipeline_):
    pipeline = pdal.Pipeline(json.dumps(pipeline_))
    pipeline.validate()
    pipeline.execute()
    arr = pipeline.arrays[0]
    description = arr.dtype.descr
    cols = [col for col, __ in description]
    df = pd.DataFrame({col: arr[col] for col in cols})
    df['X_0'] = df['X']
    df['Y_0'] = df['Y']
    df['Z_0'] = df['Z']
    df['X'] = df['X'] - df['X_0'].min()
    df['Y'] = df['Y'] - df['Y_0'].min()
    df['Z'] = df['Z'] - df['Z_0'].min()
    return df



def address_to_lazfile(address):
    f = open('all_workunits.txt')
    all_wu_data = f.read()
    f.close()
    workunits = [wu.strip().strip("/").strip("PRE").strip() for wu in all_wu_data.split("\n")]
    y = requests.post('https://protected-peak-85531.herokuapp.com/parcel_info', params={'address': address})
    parcel_info = y.json()
    z = requests.get('https://protected-peak-85531.herokuapp.com/get_point_cloud_workunits', params={'address': address})
    recieved_units = z.json()

    filename_original = "models/model_" + str(parcel_info["gisparcel_id"]) + '_original.laz'
    # filename_added_attributes = "models/model_" + str(parcel_info["gisparcel_id"]) + '_added_attributes.laz'
    # filename_buildings = "models/model_" + str(parcel_info["gisparcel_id"]) + '_buildings.laz'


    p = pathlib.Path(filename_original)

    if p.is_file():
        return filename_original

    f = open(filename_original, "w")
    f.close()
    matching_units = []
    for ru in recieved_units:
        for wu in workunits:
            if ru in wu:
                matching_units.append(wu)

    wu_link = matching_units[0]

    polygon_array = json.loads(parcel_info['polygon_details']['st_asgeojson'])['coordinates'][0][0]
    polygon_4326 = Polygon(polygon_array)
    polygon_4326_bounds = polygon_4326.bounds
    wsg84 = pyproj.Proj(init='epsg:4326')
    lambert = pyproj.Proj(init='epsg:3857')
    coords = [pyproj.transform(wsg84,lambert,x,y) for (x,y) in polygon_array]
    polygon = Polygon(coords)
    print(polygon.wkt)
    b = polygon.bounds
    cropper = {
    "pipeline": [
        {
            "bounds": str(([b[0], b[2]],[b[1], b[3]])),
            "filename": "ept://https://s3-us-west-2.amazonaws.com/usgs-lidar-public/" + wu_link,
            "type": "readers.ept",
            "tag": "readdata"
        },
        {   "type":"filters.crop",
            'polygon':polygon.wkt
        },
        {
            "filename": filename_original,
            "tag": "writerslas",
            "type": "writers.las"
        }
    ]}

    pipeline = pdal.Pipeline(json.dumps(cropper))
    pipeline.validate()
    n_points = pipeline.execute()
    return filename_original



def get_df_with_added_attributes(lazfile_path):
    reader = {
        "pipeline": [
            {
                "filename": lazfile_path,
                "type": "readers.las",
                "tag": "readdata"
            },
            {
                "type":"filters.smrf"
            },
            {
                "type":"filters.hag_nn"
            },
            {   "type":"filters.eigenvalues",
                "knn":16},
            {   "type":"filters.normal",
                "knn":16},
            {
                "type":"filters.approximatecoplanar",
                "knn":16
            },
            {
                "type":"filters.reciprocity",
                "knn":16
            }
        ]}
    return pipeline_to_df(reader)



def get_df_building_filtered(lazfile_path):
    reader = {
        "pipeline": [
            {
                "filename": lazfile_path,
                "type": "readers.las",
                "tag": "readdata"
            },
            {
                "type":"filters.smrf"
            },
            {
                "type":"filters.hag_nn"
            },
            {   "type":"filters.eigenvalues",
                "knn":16},
            {   "type":"filters.normal",
                "knn":16},
            {
                "type":"filters.approximatecoplanar",
                "knn":16
            },
            {
                "type":"filters.reciprocity",
                "knn":16
            },
            {
                "type":"filters.python",
                "add_dimension": "roof=int32",
                "script":"custom_filter.py",
                "function":"get_roof",
                "module":"anything"
            },
            
            {
                "type":"filters.range",
                "limits":"roof[1:1]"
            },
            {
                "type":"filters.outlier",
                "method":"statistical",
                "mean_k":200,
                "multiplier":0.8
            },
            {
                "type":"filters.range",
                "limits":"Classification![7:7]"
            },
            {
                "type":"filters.dbscan",
                "min_points":10,
                "eps":2.0,
                "dimensions":"X,Y,Z"
            },
        ]}
    return pipeline_to_df(reader)



def get_largest_building_df(lazfile_path):
    df = get_df_building_filtered(lazfile_path)
    df_n = df[df['Classification'] != 7]
    cluster_density = df_n['ClusterID'].value_counts()/df_n['ClusterID'].shape[0]
    if -1 in list(cluster_density.keys()):
        cluster_density.drop([-1], inplace=True)
    fcd = cluster_density.where(cluster_density > 0.1).dropna()
    print("Number of Structures: ", fcd.shape[0])
    largest_str_cluster = fcd.idxmax()
    other_str_clusters = [n for n in list(fcd.keys()) if n != largest_str_cluster]
    print("No of points:", df[df['ClusterID'] == largest_str_cluster].shape[0])
    print("Highest point of Building: ", df[df['ClusterID'] == largest_str_cluster]['HeightAboveGround'].max())
    print("Lowest point of Building: ", df[df['ClusterID'] == largest_str_cluster]['HeightAboveGround'].min())
    print("Avg height from gorund: ", df[df['ClusterID'] == largest_str_cluster]['HeightAboveGround'].mean())
    building_df = df[df['ClusterID'] == largest_str_cluster]
    return building_df


def get_separated_roof_labeled_building_data(building_df):
    df_copy = pd.DataFrame().reindex_like(building_df)
    dfs = []
    normals_to_points = building_df[['NormalX', 'NormalY', 'NormalZ']]
    clustering = DBSCAN(eps=0.01).fit(normals_to_points)
    building_df['labels_normal'] = clustering.labels_
    percentage_clusters = dict(building_df['labels_normal'].value_counts()/building_df.shape[0]*100)
    imp_points = {k:v for k,v in percentage_clusters.items() if v > 1 and k > -1}
    imp_clusters = imp_points.keys()
    i = 0
    for cluster_number in imp_clusters:
        df_cluster = building_df[building_df['labels_normal'] == cluster_number]
        Y = df_cluster[['X', 'Y']]
        spatial_clustering = DBSCAN(eps=3).fit(Y)
        df_cluster['labels_spatial'] = spatial_clustering.labels_
        for spatial_cluster in np.unique(spatial_clustering.labels_):
            df_spatial_cluster = df_cluster[df_cluster['labels_spatial'] == spatial_cluster]
            df_spatial_cluster['df_spatial_cluster'] = i
            building_df['labels_spatial'] = df_spatial_cluster['labels_spatial']
            dfs.append(df_spatial_cluster)
            i = i + 1
        final_df = pd.concat(dfs)
        return final_df
        

        

















