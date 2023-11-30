import boto3
import rasterio
import geopandas as gpd
import pandas as pd
import numpy as np
import os
import json
import argparse

from datetime import date, datetime, timedelta

from shapely.geometry import Point
from rasterstats import zonal_stats, point_query
s3_client = boto3.client('s3')
print('lol')


def query_dataframe(a):
    # s3://s3://mitrphol-farmfocus-test-553463144000/MasterPlot/file/
    # create sub_folder on sagemaker linux os
    # createFolder('/opt/ml/processing/blueprint/input')
    bucket = 'mitrphol-farmfocus-test-553463144000'
    prefix = 'MasterPlot/file/20200000_PLOT.geojson'
    source = f'/opt/ml/processing/blueprint/input/{a.YEAR}0000_PLOT.geojson'

    downlode_raw_tile(bucket, prefix, source)
    # downlode_raw_tile(bucket, prefix, source)
    print("download masterplot success")
    gdf = gpd.read_file(source)
    query_gdf = gdf['geometry']
    set_index_gdf = query_gdf.dropna().reset_index().drop(['index'], axis=1)
    geo = set_index_gdf['geometry'].set_crs('epsg:4326')
    geo48 = geo.to_crs('epsg:32648')
    # geo48 = geo.to_crs('epsg:32648')
    print("adjust dataframe success")

    return geo48, source


def tiles_of_list():
    lst_tiles = []
    s3_client = boto3.client("s3")
    for k in ['47', '48']:
        for t in ['P', 'Q']:
            response = s3_client.list_objects(
                Bucket='mitrphol-satellite-prod-553463144000',
                Prefix=f'satellite/tiles/{k}/{t}/',
                Delimiter='/',
            )

            # print(response)

            for o in response.get('CommonPrefixes'):
                lst_tiles.append(''.join(o.get('Prefix').split('/')[-4:]))

    print(lst_tiles)

    return lst_tiles


def dataframe48(pdf_48, dicts, source, tile):
    dicts[tile] = []  # create dictionary of each tile
    for loop in range(len(pdf_48.index)):
        geo_m = pdf_48.loc[loop]  # each polygon

        if isinstance(geo_m, type(None)):
            dicts[tile].append('None')

        elif isinstance(geo_m, type(pdf_48.loc[1])):
            geo_m_cen = geo_m.centroid
            check_query = point_query(geo_m_cen, source)

            if check_query[0] is None:  # not match
                dicts[tile].append(0)
                pass

            else:  # match
                dicts[tile].append(1)
    return dicts

# def dataframe48(pdf_48, dicts, source):
#     dicts[tile]  = [] # create dictionary of each tile
#     for loop in range(len(pdf_48.index)):
#         geo_m = pdf_48.loc[loop] # each polygon

#         if isinstance(geo_m, type(None)):
#             dicts[tile].append('None')

#         elif isinstance(geo_m, type(pdf_48.loc[1])):
#             geo_m_cen = geo_m.centroid
#             check_query =  point_query(geo_m_cen, source)

#             if check_query[0] is None: #not match
#                 dicts[tile].append('False')
#                 pass

#             else: # match
#                 dicts[tile].append('True')


def downlode_raw_tile(bucket='', prefix='', source=''):
    # s3_cilent.download_file(Bucket=bucket,Key=prefix,Filename=source)
    # https://mitrphol-farmfocus-test-553463144000.s3.ap-southeast-1.amazonaws.com/MasterPlot/file/20200000_PLOT.geojson
    s3_client.download_file(Bucket=bucket, Key=prefix, Filename=source)


def upload_dicts_on_aws(bucket='', prefix='', source=''):
    s3_client.upload_file(Filename=source, Bucket=bucket, Key=prefix)


def createFolder(sPathParentFolder):  # open path folder in computer cloud with linux
    try:
        os.makedirs(sPathParentFolder)
        print("Directory ", sPathParentFolder,  " Created ")
    except FileExistsError:
        print("Directory ", sPathParentFolder,  " already exists")


def write_and_uplode_file(dicts, a):
    bucket = 'mitrphol-farmfocus-test-553463144000'
    prefix = 'MasterPlot/prepare_of_files/dicts_file_tile48_2020_.json'
    dict_file = f'dicts_file_tile47_{a.YEAR}_.json'
    root = '/opt/ml/processing/blueprint/input'

    try:
        with open(f'{root}/{dict_file}', 'w') as outfile:
            json.dump(dicts, outfile)
    except Exception as e:
        with open(f'{root}/{dict_file}', 'w') as outfile:
            json.dumps(dicts, outfile)
        print(e)

    print('create dicts file on root')

    upload_dicts_on_aws(bucket=bucket, prefix=prefix,
                        source=f'{root}/{dict_file}')
    print('upload dicts on aws successfully')

    os.remove(f'{root}/{dict_file}')


def process(pdf_48=[], day=12, dicts={}, tile=[], a=None):
    start = datetime.now()
    source = ''
    # for 47
    if tile[:2] == '48':

        bucket = 'mitrphol-satellite-prod-553463144000'
        prefix = f'satellite/tiles/{tile[:2]}/{tile[2:3]}/{tile[3:]}/2020/10/{day}/0/R10m/B03.jp2'
        source = f'/opt/ml/processing/blueprint/input/{tile}_B03.jp2'

        downlode_raw_tile(bucket=bucket, prefix=prefix, source=source)

        print(f'This is process of {tile}')
        process_dicts = dataframe48(pdf_48=pdf_48, dicts=dicts,
                                    source=source, tile=tile, a=a)
        print(f'succees  process of {tile}')

    # for 48
    # if tile[:2] == '48':
    #     bucket = 'mitrphol-satellite-prod-553463144000'
    #     prefix = f'satellite/tiles/{tile[:2]}/{tile[2:3]}/{tile[3:]}/2022/6/22/0/R10m/B03.jp2'
    #     source = f'/root/FarmFocus/band/{tile}_B03.jp2'

    #     downlode_raw_tile(bucket=bucket, prefix=prefix, source=source)

    #     print(f'This is process of {tile}')
    #     dataframe48(pdf_48=pdf_48, dicts=dicts, source=source)
    #     print(f'succees  process of {tile}')

        end = datetime.now()
        e_s = end - start
        print(e_s)
    else:
        print('fail')

    if os.path.exists(source):
        os.remove(source)
        print(f'remove -> {source}')

    return process_dicts


def onGetArgs():
    class AttributeDict(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    parser = argparse.ArgumentParser(
        description='farmfocus-masterplot-hv2-prod')
    # parser.add_argument('-month',  '--MONTH',  help='Year', required=True)
    parser.add_argument('-year', '--YEAR', help='Month', required=True)
    args = vars(parser.parse_args())
    a = AttributeDict(args)

    # a['BUCKET_PROD'] = "mitrphol-satellite-prod-553463144000"
    # a['BUCKET_TEST'] = "mitrphol-farmfocus-test-553463144000"
    # a['KEY_PATH_TARGET'] = "satellite/tiles"

    a['PATH_INPUT_FOLDER'] = "/opt/ml/processing/blueprint/input"
    # a['PATH_OUTPUT_FOLDER'] = "/opt/ml/processing/adjust2band/output"

    # create sub_folder on sagemaker linux os
    createFolder(a['PATH_INPUT_FOLDER'])
    # createFolder(a['PATH_OUTPUT_FOLDER'])

    print(json.dumps(a, sort_keys=False, indent=4))  # show value
    return a

# scirpt


def main(a):

    re_dicts = {}
    tiles = tiles_of_list()
    print(tiles)
    # print(tiles[:1], tiles[0])
    pdf_48, source_file = query_dataframe(a)

    for tile in tiles:  # each tile

        if tile[:2] == '48':
            try:
                # if day = 11
                re_dicts = process(pdf_48=pdf_48, tile=tile,
                                   dicts=re_dicts, a=a)
            except Exception as e:
                try:
                    print('########error_1 this here', e)
                    # if day = 13
                    re_dicts = process(pdf_48=pdf_48, day=13,
                                       tile=tile, dicts=re_dicts, a=a)
                    # for 48
                    # elif tile[:2] == '48':
                    #     bucket = 'mitrphol-satellite-prod-553463144000'
                    #     prefix = f'satellite/tiles/{tile[:2]}/{tile[2:3]}/{tile[3:]}/2022/6/22/0/R10m/B03.jp2'
                    #     source = f'/root/FarmFocus/band/{tile}_B03.jp2'

                    #     downlode_raw_tile(bucket=bucket, prefix=prefix, source=source)

                    #     print(f'This is process of {tile}')
                    #     dataframe48(pdf_48=pdf_48, dicts=dicts, source=source)
                    #     print(f'succees  process of {tile}')
                except Exception as e:
                    print('###error_2 this here', e)
                    re_dicts = process(pdf_48=pdf_48, day=15,
                                       tile=tile, dicts=re_dicts, a=a)

    write_and_uplode_file(re_dicts, a=a)

    if os.path.exists(source_file):
        os.remove(source_file)
        print(f"It's remove {source_file}")


if __name__ == '__main__':
    a = onGetArgs()
    main(a)
