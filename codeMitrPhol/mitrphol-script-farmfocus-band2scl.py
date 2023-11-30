import rasterio
import boto3
import os
import json
import shutil
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.warp import reproject, Resampling
import numpy as np
import argparse

s3_client = boto3.client("s3")
print(s3_client)


def tile2Path(TILES):
    if len(TILES) == 5:
        pathTile = f"{TILES[:2]}/{TILES[2]}/{TILES[3:]}"
        print(pathTile)
    else:
        print("error")
    return pathTile


def onListDateProd2Test(bucket='', prefix=''):

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    my_list = []
    res = response["Contents"]
    while len(res) <= 1000:
        count = 0
        last_key = ''
        for i in response["Contents"]:
            year = int(i["Key"].split("/")[5])
            month = int(i["Key"].split("/")[6])
            day = int(i["Key"].split("/")[7])
            reso = i["Key"].split("/")[9]

            if (reso == 'R10m') or reso == 'R20m':
                if (day < 25) and (year == 2022) and (month == 1):
                    pass
                else:
                    my_list.append(i["Key"])
            count += 1
        if count == 1000:  # if contents no have 1000 = error
            last_key = i["Key"]
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                StartAfter=last_key
            )
            res = response["Contents"]
        # if list objects v2 < 1000 (max 1000) filter path file again and quit while loop
        elif len(res) < 1000:
            break
    return my_list


def PROD_NUM(my_list=[]):
    prod_num = []
    for j in my_list:
        prod_num.append(j.split("/")[7])  # find date

    num_list = list(set(prod_num))  # duplicate date
    len_num = len(num_list)
    num_int = []
    k = 0
    while k < len_num:
        num_int.append(int(num_list[k]))
        k += 1

    return num_int


bucket = 'mitrphol-satellite-prod-553463144000'


def APPEND_MONTH(a, changePathTile):
    try:
        MONTH = []
        for YY in range(2022, 2024):
            for MM in range(1, 13):
                prefix = f'{a.KEY_PATH_TARGET}/{changePathTile}/{YY}/{MM}/'
                lst = onListDateProd2Test(bucket=bucket, prefix=prefix)
                MONTH.append(lst)
                PROD_NUM(my_list=lst)
            # print(MONTH,len(MONTH))
            # print()
    except Exception as e:
        print(e)
        return MONTH


def tileJson2String(my_lists=[]):
    try:
        list_path = []
        if my_lists == []:
            print('hi')
            return
        print('ok')
        for my_list in my_lists:
            for path in my_list:
                ver = path.split('/')[9]
                band = path.split('/')[10]
                if (ver == 'R10m') or band == 'SCL.jp2':
                    list_path.append(path)
        return list_path
    except Exception as e:
        print(e)


def download(key='', source='', bucket=''):
    s3_client.download_file(bucket, key, source)


def upload(day='', day_blackslash='', a='', tile='', ver='', reso='R10m'):
    for band in ['AOT', 'B02', 'B03', 'B04', 'B08', 'TCI', 'WVP']:
        output_path = f'{a.PATH_OUTPUT_FOLDER}/{day}_{band}.jp2'
        path_2_s3 = f'{a.KEY_PATH_NEW_TARGET}/{tile}/{day_blackslash}/{ver}/{reso}/{band}.jp2'
        s3_client.upload_file(Filename=output_path, Key=path_2_s3,
                              Bucket=a.BUCKET_TEST)
        print(output_path + " It's upload success")


def remove_file(day, a):
    for band in ['AOT', 'B02', 'B03', 'B04', 'B08', 'TCI', 'WVP', 'SCL']:
        scl_path = f"{a.PATH_BUFFER_FOLDER}/{day}_SCL.jp2"
        band_input = f"{a.PATH_INPUT_FOLDER}/{day}_{band}.jp2"
        band_output = f"{a.PATH_OUTPUT_FOLDER}/{day}_{band}.jp2"

        if os.path.exists(scl_path):
            os.remove(scl_path)
            print(f'remove sucess {day}_{band}.jp2')

        if os.path.exists(band_input):
            os.remove(band_input)  # a['PATH_INPUT_FOLDER']
            print(f'remove sucess {day}_{band}.jp2')

        if os.path.exists(band_output):
            os.remove(band_output)  # a['PATH_INPUT_FOLDER']
            print(f'remove sucess {day}_{band}.jp2')


def resolution_scl(a, day='', key=''):
    # a['PATH_BUFFER_FOLDER']
    input_path = f"{a.PATH_BUFFER_FOLDER}/{day}_buffer_SCL.jp2"
    # from sat prod to instance buffer
    download(bucket=a.BUCKET_PROD, key=key, source=input_path)
    with rasterio.open(input_path) as src:
        # Define the new resolution (10m)
        new_resolution = (10.0, 10.0)

        # Define the output SCL raster file path
        # a['PATH_INPUT_FOLDER']
        output_path = f"{a.PATH_INPUT_FOLDER}/{day}_SCL.jp2"

        # Calculate the scaling factors for resampling
        x_scale = src.transform[0] / new_resolution[0]
        y_scale = src.transform[4] / new_resolution[1]

        # Create a profile for the output SCL raster
        profile = src.profile
        # print(profile)
        profile["transform"] = rasterio.Affine(src.transform.a / x_scale, src.transform.b, src.transform.c,
                                               src.transform.d, src.transform.e / -y_scale, src.transform.f)
        profile["width"] = int(src.width * x_scale)
        profile["height"] = int(src.height * -y_scale)
        profile['dtype'] = 'uint16'
        profile['blockxsize'] = 1024
        profile['blockysize'] = 1024
        # print(profile)

        # Create an empty output SCL raster
        with rasterio.open(output_path, "w", **profile) as dst:
            # Read the input SCL data
            data = src.read(1)

            # Resample the data to the new resolution using the mode (most frequent class)
            resampled_data = np.empty((dst.height, dst.width), dtype=data.dtype)
            reproject(data, resampled_data, src_transform=src.transform,
                      src_crs=src.crs, dst_transform=dst.transform,
                      dst_crs=dst.crs, resampling=Resampling.mode)

            # Write the resampled SCL data to the output SCL raster
            dst.write(resampled_data, 1)


def mask_cloud_each_band(day, a):
    for band in ['AOT', 'B02', 'B03', 'B04', 'B08', 'TCI', 'WVP']:

        # a['PATH_INPUT_FOLDER']
        path_band = f'{a.PATH_INPUT_FOLDER}/{day}_{band}.jp2'
        # a['PATH_INPUT_FOLDER']
        path_scl = f'{a.PATH_INPUT_FOLDER}/{day}_SCL.jp2'
        # a['PATH_OUTPUT_FOLDER']
        output_band = f'{a.PATH_OUTPUT_FOLDER}/{day}_{band}.jp2'
        # band
        with rasterio.open(path_band) as src_b:

            # band_arr = None
            profile_b = src_b.profile
            profile_b['dtype'] = 'uint16'
            band_arr = src_b.read(1).astype(float)
            # print(band_arr)
            # print(profile_b)

            # SCL
            with rasterio.open(path_scl) as src_s:
                profile_s = src_s.profile
                # print(profile_s)
                scl_arr = src_s.read(1).astype(float)
                mask = (scl_arr == 0) | (scl_arr == 1) | (scl_arr == 8) | (scl_arr == 9) | (
                    scl_arr == 3) | (scl_arr == 10) | (scl_arr == 7) | (scl_arr == 11)
                band_arr[mask] = np.nan
                # print(band_arr)
                with rasterio.open(output_band, "w", **profile_b) as dst:
                    dst.write(band_arr, 1)

                    print(f"upload succes -> {output_band}")


def createFolder(sPathParentFolder):  # Open folder in instance
    try:
        os.makedirs(sPathParentFolder)  # makedirs for os linux
        print("Directory ", sPathParentFolder,  " Created ")
    except FileExistsError:
        print("Directory ", sPathParentFolder,  " already exists")


def onGetArgs():
    class AttributeDict(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    parser = argparse.ArgumentParser(
        description='farmfocus-raw2adjustband')
    parser.add_argument('-tile',  '--TILES',  help='YYYY',
                        required=True)  # 9 portion of tile

    args = vars(parser.parse_args())
    a = AttributeDict(args)

    a['BUCKET_PROD'] = "mitrphol-satellite-prod-553463144000"
    a['BUCKET_TEST'] = "mitrphol-farmfocus-test-553463144000"
    a['KEY_PATH_TARGET'] = "satellite/tiles"
    a['KEY_PATH_NEW_TARGET'] = "satellite_mask_cloud/tiles"

    a['PATH_BUFFER_FOLDER'] = "/opt/ml/processing/band2scl/buffer"
    a['PATH_INPUT_FOLDER'] = "/opt/ml/processing/band2scl/input"
    a['PATH_OUTPUT_FOLDER'] = "/opt/ml/processing/band2scl/output"

    # list all tiles
    # a['TILES'] = ["47PNQ", "47PNR", "47PNS", "47PNT", "47PPS", "47PPT", "47PQS", "47PQT", "47PRS", "47PRT", "47QMU", "47QMV", "47QNU",
    #               "47QNV", "47QPU", "47QPV", "47QQU", "47QQV", "47QRU", "47QRV", "48PTB", "48PTC", "48PUB",
    #               "48PUC", "48PVB", "48PVC", "48PWB", "48PWC", "48QTD", "48QTE", "48QUD", "48QUE", "48QVD", "48QVE"]
    # a['PATH_YEAR_MONTH'] = f'{a.YEAR}/{a.MONTH}'

    # create sub_folder on sagemaker linux os
    createFolder(a['PATH_BUFFER_FOLDER'])
    createFolder(a['PATH_INPUT_FOLDER'])
    createFolder(a['PATH_OUTPUT_FOLDER'])

    print(json.dumps(a, sort_keys=False, indent=4))  # show value
    return a


def main(a):
    bucket = 'mitrphol-satellite-prod-553463144000'

    changePathTile = tile2Path(TILES=a.TILES)  # from 47QRU to 47/Q/RU

    test = tileJson2String(my_lists=APPEND_MONTH(
        changePathTile=changePathTile, a=a))

    print(f"check your sattelite {test[0]} and {test[-1]}")
    # work in difference parts
    for path in test:
        path = path.split('/')
        tile = '/'.join(path[2:5])  # 47/Q/RU
        day = '_'.join(path[-6:-3])  # 2022_1_26
        day_blackslash = '/'.join(path[-6:-3])  # 2022/1/26
        band = path[-1]  # AOT.jp2
        ver = path[-3]  # 0
        reso = path[-2]  # R10m
        # bucket = 'mitrphol-satellite-prod-553463144000'  # a['BUCKET_PROD']
        # satellite/tiles/47/Q/RU/2022/1/26/0/R10m/AOT.jp2
        key = '/'.join(path)
        # a['PATH_INPUT_FOLDER']
        source = f'{a.PATH_INPUT_FOLDER}/{day}_{band}'
        # filename = f''

        if path[-1] == 'SCL.jp2':
            # Open the input SCL raster file (20m resolution)
            resolution_scl(day=day, a=a, key=key)  # instance
            mask_cloud_each_band(day=day, a=a)  # instance
            print("SCL conversion complete. Output saved to", source)
            upload(day_blackslash=day_blackslash, day=day, a=a, tile=tile,
                   ver=ver)  # instance to s3
            remove_file(day, a=a)  # remove file on instance

        else:
            download(key=key, source=source, bucket=a.BUCKET_PROD)
            print(band, day)


if __name__ == "__main__":
    a = onGetArgs()

    main(a)