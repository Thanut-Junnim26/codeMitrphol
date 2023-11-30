from pathlib import Path

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

import geopandas as gpd
from osgeo import gdal, osr
from rasterstats import zonal_stats

import rasterio
import time

import argparse
import pandas as pd

import json
import boto3

import socket
import os

import random

####################################################################################################
import warnings; warnings.filterwarnings('ignore')
import numpy as np; np.seterr(divide = 'ignore', invalid = 'ignore')
####################################################################################################
def createFolder(sPathParentFolder):
    try:
        os.makedirs(sPathParentFolder)
        print("Directory " , sPathParentFolder ,  " Created ") 
    except FileExistsError:
        print("Directory " , sPathParentFolder ,  " already exists")

def onGetListProcessDays(DATE_START, DATE_ENDED):
    ads = DATE_START.split("-")
    ade = DATE_ENDED.split("-")
    start_date = date(int(ads[0]), int(ads[1]), int(ads[2]))
    end_date   = date(int(ade[0]), int(ade[1]), int(ade[2]))

    result = []
    while start_date <= end_date:
        YYYY_MM_DD = start_date.strftime("%Y-%m-%d")
        result.append(YYYY_MM_DD)
        start_date += relativedelta(days = 1)

    return result

class AttributeDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def onGetTileList_V2(FAC):
    list_fac_tiles = {
        'MAC':['48PUC','48PVB','48PVC','48PWB','48PWC','48QUD','48QVD'],
        'MCE':['47QMU'],
        'MDC':['47PNR','47PNS','47PNT','47PPS'],
        'MKS':['48PUC','48PVC','48QTD','48QUD','48QUE','48QVD','48QVE'],
        'MPK':['47PQT','47PRT','47QQU','47QRU','48PTC','48QTD'],
        'MPL':['47QQU','47QQV','47QRU','47QRV','48QTD','48QTE'],
        'MPV':['47PRT','47QQU','47QRU','47QRV','48PTC','48QTD','48QTE','48QUD'],
        'MSB':['47PNS','47PPS','47PPT','47PQS','47PQT'],
        'ALL':['47PNQ','47PNR','47PNS','47PNT','47PPS','47PPT','47PQS','47PQT','47PRS','47PRT',
               '47QMU','47QMV','47QNU','47QNV','47QPU','47QPV','47QQU','47QQV','47QRU','47QRV',
               '48PTB','48PTC','48PUB','48PUC','48PVB','48PVC','48PWB','48PWC','48QTD','48QTE',
               '48QUD','48QUE','48QVD','48QVE' ]
    }
    return list_fac_tiles[FAC]

def printDict(d):
    print(json.dumps(d, sort_keys=False, indent=4))

def onReadShape(PATH, FACTORY):
    df = gpd.read_file(PATH)
    print("Total shape main size:", len(df))
    # เลือกโรงงาน
    df = df[df.company_code == FACTORY]
    # ตัด row ที่ไม่มี geometry ออก
    df = df[df.geometry != None]
    print("Total shape main size by factory:",FACTORY, len(df))
    return df

def onDownloadRaster(PROCESS_PATH, YEAR, MONTH, DAY):
    arExistTile = []
    s3client = boto3.client('s3')
    for TILE in a.TILES:
        try:
            raster_filename = f"S2_T{TILE}_{YEAR}{MONTH}{DAY}_NDVI.tif"
            print("[onDownloadRaster] Downloading", raster_filename)
            s3client.download_file(
                Bucket = "mitrphol-satellite-prod-553463144000", 
                Key = f"sentinel-s2-l2a-ndvi/tiles/{TILE}/{YEAR}/{MONTH}/{raster_filename}", 
                Filename = f"{PROCESS_PATH}/{raster_filename}",
            )
            arExistTile.append(TILE)
            print("[onDownloadRaster] Successful downloaded :", raster_filename)
        except Exception as ex:
            print("[onDownloadRaster] File do not exist!", raster_filename)

    print("[onDownloadRaster] List Tile Exist :", arExistTile)
    return arExistTile

def getYearCrop(YEAR, MONTH):
    base = 543
    MONTH = int(MONTH)
    YEAR  = int(YEAR)

    if MONTH < 9:
        base = base - 1

    sYEARTH_1 = str( YEAR + base )[-2:]
    sYEARTH_2 = str( YEAR + base + 1 )[-2:]

    return str(f'{sYEARTH_1}{sYEARTH_2}')

def onDownloadGeojsonPlot(a, s3_client):
    print("[onDownloadGeojsonPlot]")
    s3_client.download_file(
        Bucket = "mitrphol-farmfocus-prod-553463144000", 
        Key = f"master/plot/{a.FILENAME_INPUT_PLOT}", 
        Filename = f"{a.PATH_INPUT_FILE_PLOT}",
    )
    print(f"Downloaded : master/plot/{a.FILENAME_INPUT_PLOT}")
    return True

def onGetArgs():
    parser = argparse.ArgumentParser(description='processing-harvest-monitoring')
    parser.add_argument('-f',  '--FACTORY',    help='', required=True)
    parser.add_argument('-ds', '--DAYSTART',  help='', required=True)
    parser.add_argument('-de', '--DAYEND',    help='', required=True)
    parser.add_argument('-PLOTNAME', '--PLOTNAME',    help='', required=True)
    args = vars(parser.parse_args())
    a = AttributeDict(args)
    
    # Initial parameter
    a['DAYSTART']           = str(a.DAYSTART).zfill(2)
    a['DAYEND']             = str(a.DAYEND).zfill(2)
    a['arYYYY_MM_DD']       = onGetListProcessDays(a.DAYSTART, a.DAYEND)

    a['BASE_DIR']           = str(Path(__file__).resolve().parent)
    a['PROCESS_PATH']       = f"{a.BASE_DIR}/process"
    a['SHP_FIELD_FID_NAME'] = "plot_code"

    a['PATH_INPUT_FOLDER_PLOT']   = "/opt/ml/processing/input/plot/"
    a['FILENAME_INPUT_PLOT']      = a.PLOTNAME
    a['PATH_INPUT_FILE_PLOT']     = f"{a.PATH_INPUT_FOLDER_PLOT}{a.FILENAME_INPUT_PLOT}"

    a['TILES']              = onGetTileList_V2(a.FACTORY)
    printDict(a)

    return a

if __name__ == "__main__":
    local_mode = False

    if str(socket.gethostname()) == "NATAKRANP-MBP.local":
        # This for local development testing computer.
        df = pd.read_csv('/Users/natakranp/credential-mitrphol-farmfocus-user-natakranp-prod.csv')
        credential = df.iloc[0]

        session = boto3.Session(
            aws_access_key_id     = credential['AccesskeyID'],
            aws_secret_access_key = credential['Secretaccesskey'],
        )
        s3_client = session.client('s3')
        local_mode = True
    else:
        # This will invoke from any aws services.
        s3_client = boto3.client('s3')

    a = onGetArgs()
    createFolder(a.PATH_INPUT_FOLDER_PLOT)
    onDownloadGeojsonPlot(a, s3_client)

    output_fields = [
        "FACTORY",
        "FID",
        "MaxPixel", "Harvested", "Cloud", "UnHarvested", "HavestDate", "Scene",
        "zs_mock_count",     "zs_mock_mean",     "zs_mock_max",     "zs_mock_min",
        "zs_raster_count",   "zs_raster_mean",   "zs_raster_max",   "zs_raster_min",
        "plot_id",
    ]

    # FACTORY_condition = {
    #     'MPK'  : { 11:0.25, 12:0.23, 1:0.20, 2:0.19, 3:0.18, 4:0.16 },
    #     'MPL'  : { 11:0.25, 12:0.23, 1:0.20, 2:0.20, 3:0.18, 4:0.18 },
    #     'MPV'  : { 11:0.25, 12:0.22, 1:0.20, 2:0.20, 3:0.19, 4:0.17 },
    #     'MDC'  : { 11:0.27, 12:0.25, 1:0.23, 2:0.19, 3:0.16, 4:0.15 },
    #     'MSB'  : { 11:0.28, 12:0.25, 1:0.23, 2:0.20, 3:0.19, 4:0.18 },
    #     'MAC'  : { 11:0.25, 12:0.22, 1:0.19, 2:0.18, 3:0.17, 4:0.15 },
    #     'MKS'  : { 11:0.25, 12:0.25, 1:0.22, 2:0.20, 3:0.20, 4:0.18 },
    #     'MCE'  : { 11:0.27, 12:0.25, 1:0.23, 2:0.19, 3:0.16, 4:0.15 }
    # } 
    FACTORY_condition = { # 4 repeat to 10, use shapefile update 6667
        'MPK' : {11:0.36, 12:0.36, 1:0.34, 2:0.31, 3:0.30, 4:0.28}, 
        'MKB' : {11:0.35, 12:0.35, 1:0.35, 2:0.30, 3:0.30, 4:0.25}, 
        'MPL' : {11:0.35, 12:0.35, 1:0.35, 2:0.33, 3:0.30, 4:0.30}, 
        'MPV' : {11:0.36, 12:0.35, 1:0.34, 2:0.32, 3:0.30, 4:0.30}, 
        'MDC' : {11:0.35, 12:0.35, 1:0.34, 2:0.32, 3:0.30, 4:0.29}, 
        'MCE' : {11:0.35, 12:0.35, 1:0.34, 2:0.32, 3:0.30, 4:0.29}, 
        'MSB' : {11:0.40, 12:0.40, 1:0.35, 2:0.30, 3:0.30, 4:0.25}, 
        'MAC' : {11:0.40, 12:0.40, 1:0.35, 2:0.30, 3:0.30, 4:0.25}, 
        'MKS' : {11:0.40, 12:0.40, 1:0.35, 2:0.30, 3:0.30, 4:0.25}, 
        'LAO' : {11:0.40, 12:0.40, 1:0.35, 2:0.30, 3:0.30, 4:0.25}, 
        'ALL' : {11:0.37, 12:0.37, 1:0.35, 2:0.31, 3:0.30, 4:0.27}
    } 
    # Update 2022-12-189
    harvest_NDVI = FACTORY_condition[a.FACTORY]
    
    ########################################

    df_shp_main        = onReadShape(PATH=a.PATH_INPUT_FILE_PLOT, FACTORY=a.FACTORY)
    df_shp_main["FID"] = df_shp_main[a.SHP_FIELD_FID_NAME]
    
    ########################################

    for YYYY_MM_DD in a.arYYYY_MM_DD:
        print()
        print()
        ar_YYYY_MM_DD  = YYYY_MM_DD.split("-")
        a['YEAR']      = ar_YYYY_MM_DD[0]
        a['MONTH']     = str(ar_YYYY_MM_DD[1]).zfill(2)
        a['DAY']       = str(ar_YYYY_MM_DD[2]).zfill(2)

        a['YEARCROP']  = getYearCrop(a.YEAR, a.MONTH)

        print("######################################################")
        print("PROCESS AT: ", a.YEARCROP, a.YEAR, a.MONTH, a.DAY)
        print(" Starting Download Tile and Check Existing..")

        a['arTILE_PROC'] = onDownloadRaster(a.PROCESS_PATH, a.YEAR, a.MONTH, a.DAY )

        print("######################################################")
        print("EXIST TILE: ", len(a.arTILE_PROC), a.arTILE_PROC)
        print("######################################################")
        printDict(a)
        print("######################################################")
        
        ar_df = []
        if len(a.arTILE_PROC) != 0:
            for TILE in a.arTILE_PROC:
                sPathTIFFile   = f"{a.PROCESS_PATH}/S2_T{TILE}_{a.YEAR}{a.MONTH}{a.DAY}_NDVI.tif"
                src_raster     = rasterio.open(sPathTIFFile)
                arr_raster     = src_raster.read(1, masked=False)
                affine_raster  = src_raster.transform
                raster_crs     = src_raster.crs
                raster_crs     = str(raster_crs).split(":")[1] # "EPSG:123456"
                raster_crs     = int(raster_crs)
                raster_bounds  = src_raster.bounds
                print(sPathTIFFile, raster_crs)

                df = df_shp_main.copy()

                # df = df.tail(200) # Debug Mode
                df = df.to_crs(epsg=raster_crs)
                df['FACTORY'] = a.FACTORY
                df['Scene'] = TILE

                df['raster_bounds_left']   = raster_bounds.left
                df['raster_bounds_right']  = raster_bounds.right
                df['raster_bounds_top']    = raster_bounds.top
                df['raster_bounds_bottom'] = raster_bounds.bottom

                df['poly_centroid_x'] = df['geometry'].centroid.x
                df['poly_centroid_y'] = df['geometry'].centroid.y

                df = df.loc[
                    (df['raster_bounds_left']   < df['poly_centroid_x']) & (df['poly_centroid_x'] < df['raster_bounds_right']) &
                    (df['raster_bounds_bottom'] < df['poly_centroid_y']) & (df['poly_centroid_y'] < df['raster_bounds_top']) 
                ]

                if len(df) != 0:
                    zs_mock = zonal_stats(
                        df, 
                        np.ones(arr_raster.shape,dtype='uint8'), 
                        affine=affine_raster,
                    )

                    def zonal_add_Harvest(NDVI):
                        """ตัดไปกี่จุด"""
                        harvest_point = 0
                        NDVI = NDVI.ravel()
                        NDVI = NDVI[NDVI.mask == False]
                        for i in NDVI:
                            MONTH_CURR = int(a.MONTH)
                            if MONTH_CURR >= 5 and MONTH_CURR <= 10:
                                MONTH_CURR = 4
                            if i < harvest_NDVI[MONTH_CURR]:
                                harvest_point +=1
                        return harvest_point

                    zs_raster = zonal_stats(
                        df, 
                        sPathTIFFile,
                        add_stats = {
                            'harvest': zonal_add_Harvest
                        }
                    )

                    df['zs_mock']     = zs_mock
                    df['zs_raster']   = zs_raster
                    
                    def get_dict(x, val):
                        v = x[val]
                        try:
                            v = round(float(v),4)
                        except:
                            v = v
                        return v

                    df['zs_raster_count']    = df['zs_raster'].apply( lambda x : x['count'] )
                    df['zs_raster_mean']     = df['zs_raster'].apply( get_dict, val='mean' )
                    df['zs_raster_max']      = df['zs_raster'].apply( get_dict, val='max' )
                    df['zs_raster_min']      = df['zs_raster'].apply( get_dict, val='min' )
                    
                    df['zs_mock_count']  = df['zs_mock'].apply( lambda x : x['count'] )
                    df['zs_mock_mean']   = df['zs_mock'].apply( get_dict, val='mean' )
                    df['zs_mock_max']    = df['zs_mock'].apply( get_dict, val='max' )
                    df['zs_mock_min']    = df['zs_mock'].apply( get_dict, val='min' )

                    df['MaxPixel']    = df['zs_mock_count']
                    df['Harvested']    = df['zs_raster'].apply(lambda x : x['harvest'])
                    df['Cloud']        = df['zs_mock_count'] - df['zs_raster_count']
                    df['UnHarvested']  = df['zs_mock_count'] - df['Harvested'] - df['Cloud']
                    df['HavestDate']  = f"{a.YEAR}-{a.MONTH}-{a.DAY}"

                    ar_df.append(df[output_fields])

            outputFilename = f"{a.FACTORY}_{a.YEAR}_{a.YEAR}{a.MONTH}{a.DAY}.csv"
            outputFilePath = f"{a.PROCESS_PATH}/{outputFilename}"
            df_out = pd.concat(ar_df, ignore_index=True)
            df_out = df_out.reset_index(drop=True)
            df_out = df_out.reset_index()
            df_out['index'] = df_out['index'].apply(lambda x : str(x).zfill(7))

            df_out['randhash'] = ""
            def randhash(x):
                return str("%08x" % random.getrandbits(32))
            df_out['randhash'] = df_out['randhash'].apply(randhash)

            df_out['UID'] = df_out['FACTORY'].str.replace('','') + df_out['FID'].str.replace('','') + df_out['HavestDate'].str.replace('-','') + df_out['Scene'].str.replace('','') + df_out['index'].str.replace('','') + df_out['randhash'].str.replace('','')
            df_out = df_out[ ['UID'] + output_fields ]
            df_out = df_out.set_index('UID')
            df_out.to_csv(outputFilePath)
            print("Last df output len: ", len(df_out))
            print("#####################################################")
            print("#####################################################")
            print()

            s3client = boto3.client('s3')
            s3client.upload_file(
                Filename = outputFilePath,
                Bucket   = "mitrphol-farmfocus-prod-553463144000", 
                Key      = f"harvest/{a.FACTORY}/{a.YEARCROP}/{outputFilename}"
            )

        else:
            print("No date to process")
            print()