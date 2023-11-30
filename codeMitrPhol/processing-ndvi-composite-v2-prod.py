import json
import argparse
from lib.common import onGetTileList
from pathlib import Path
import boto3
from osgeo import gdal, osr
import geopandas as gpd
import time
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
import os
import sys
import warnings
warnings.filterwarnings('ignore')


# to avoid error due to 'divide by zero' or 'divide by Nan'
np.seterr(divide='ignore', invalid='ignore')


############################################################################################################
def getCurrentDateTime():
    currentDateTime = datetime.now()
    d = currentDateTime + timedelta(hours=7)
    return d.strftime("%Y-%m-%d %H:%M:%S")


def onGetListProcessMonth(DATE_START, DATE_ENDED):
    ads = DATE_START.split("-")
    ade = DATE_ENDED.split("-")
    ds_yyyy = int(ads[0])
    ds_mm = int(ads[1])
    de_yyyy = int(ade[0])
    de_mm = int(ade[1])
    start_date = date(ds_yyyy, ds_mm, 1)
    end_date = date(de_yyyy, de_mm, 1)
    delta = relativedelta(months=1)

    result = []
    while start_date <= end_date:
        result.append(start_date.strftime("%Y-%m"))
        start_date += delta

    return result

#####################################################################


def createComposite(imgNDVIList, operator, OutputFile):
    print(f"{getCurrentDateTime()} NDVI Composite :")
    # dictinary to return
    # {1: [ndvi15path, evi15path, savi15path]} - if success
    # {0: 0} - if some error
    result = {}
    value = 1
    VI15PathList = list()
    nameVI = "NDVI"

    imgList = imgNDVIList  # ["",""]
    if len(imgList) == 0:
        print("No process for this. Cos no image.")
        return 0

    n_img = len(imgList)
    try:
        dsList = list()
        for imgPath in imgList:
            print(imgPath)
            dsList.append(gdal.Open(imgPath, gdal.GA_ReadOnly))
        # print(dsList)

        # reading the first dataset to extract the required information
        dsTest = dsList[0]
        driver = dsTest.GetDriver()
        # print(driver.GetDescription())
        # extracting projection and geotransform information
        dsTestProj = dsTest.GetProjection()
        dsTestGeo = dsTest.GetGeoTransform()
        # width and height of the input image
        sizeX = dsTest.RasterXSize
        sizeY = dsTest.RasterYSize
        # print('Image Size: {}, {}'.format(sizeX, sizeY))
        # extracting the band
        band = dsTest.GetRasterBand(1)
        # extracting block size
        blockSize = band.GetBlockSize()
        # print('Block size: {}'.format(blockSize))
        blockSizeX = sizeX  # blockSize[0]
        blockSizeY = 1  # blockSize[1]

        # releasing memory after extracting required information
        dsTest.FlushCache()
        # dsTest = None
        band = None

        # ******************************************************************************************************

        driver = gdal.GetDriverByName('MEM')
        dsOutput = driver.Create('', sizeX, sizeY, 1, gdal.GDT_Float32)
        # set the GeoTransform and Projection for new dataset
        dsOutput.SetProjection(dsTestProj)
        dsOutput.SetGeoTransform(dsTestGeo)

        # creating an empty array
        compositeArray = np.zeros((sizeY, sizeX), dtype=np.float32)

        print('[{}] {} days {} composite ({}) operation started'.format(
            getCurrentDateTime(), n_img, nameVI, operator))
        # reading image and performing operation row by row
        for y in range(0, sizeY, blockSizeY):
            # if condition will read from 2nd row to last row of the image
            if y + blockSizeY < sizeY:
                row = blockSizeY
            # else condition will read the first row of the image
            else:
                row = sizeY - y
            # reading all the columns of each row at once
            for x in range(0, sizeX, blockSizeX):
                # if condition will always be false if blockSizeX is equal to sizeX
                if x + blockSizeX < sizeX:
                    col = blockSizeX
                else:
                    col = sizeX - x

                # print(x, y, row, col)

                # creating an empty array
                imgSubsetArray = np.zeros(
                    (n_img, blockSizeY, blockSizeX), dtype=np.float32)
                for i in range(n_img):
                    imgSubsetArray[i, :, :] = dsList[i].GetRasterBand(
                        1).ReadAsArray(x, y, col, row)

                # options for composite
                if operator == 'max':
                    # creating max composite
                    maxCompositeSubset = np.nanmax(imgSubsetArray, axis=0)
                    # # replacing the nan values of maxComposite by -9999
                    # maxCompositeSubset[np.isnan(maxCompositeSubset)] = -9999

                    # transfering the max composite of subset to compositeArray
                    compositeArray[y, :] = maxCompositeSubset

                    # Flushing the memory inside the loop
                    maxCompositeSubset = None

                elif operator == 'mean':

                    # creating mean composite
                    meanCompositeSubset = np.nanmean(imgSubsetArray, axis=0)
                    # replacing the nan values of meanComposite by -9999
                    # meanCompositeSubset[np.isnan(meanCompositeSubset)] = -9999

                    # transfering the mean composite of subset to compositeArray
                    compositeArray[y, :] = meanCompositeSubset

                    # Flushing the memory inside the loop
                    meanCompositeSubset = None

                elif operator == 'min':
                    # creating mean composite
                    meanCompositeSubset = np.nanmin(imgSubsetArray, axis=0)
                    # replacing the nan values of meanComposite by -9999
                    # meanCompositeSubset[np.isnan(meanCompositeSubset)] = -9999

                    # transfering the mean composite of subset to compositeArray
                    compositeArray[y, :] = meanCompositeSubset

                    # Flushing the memory inside the loop
                    meanCompositeSubset = None

        # setting output array as the 1 output raster band
        dsOutput.GetRasterBand(1).WriteArray(compositeArray)
        # setting no data value to -9999
        dsOutput.GetRasterBand(1).SetNoDataValue(-9999)
        # # Alternative
        # band = dsOutput.GetRasterBand(1)
        # band.SetNoDataValue(-9999)
        # band..WriteArray(CompositeArray)

        # Building overviews upto 128 level
        dsOutput.BuildOverviews('cubic', [2, 4, 8, 16, 32, 64, 128])
        # saving
        driver = gdal.GetDriverByName('GTiff')
        dsOutput1 = driver.CreateCopy(OutputFile, dsOutput, options=[
                                      "COPY_SRC_OVERVIEWS=YES", "TILED=YES", "COMPRESS=DEFLATE"])

        # flushing the memory
        compositeArray = None
        # band.FlushCache()
        dsOutput = None
        dsOutput1 = None
        dsList = None
        print(f"[{getCurrentDateTime()}] : Ended Process Composite.")
        return 1

    except Exception as error:
        print("error", error)

#####################################################################


def getDateExistMP(s3_client, tile, YYYY, MM):
    print(f"[{getCurrentDateTime()}] : getDateExist", tile, YYYY, MM)

    MM = str(int(MM))
    YYYY = str(YYYY)
    bucket = 'mitrphol-satellite-prod-553463144000'
    prefix = f'sentinel-s2-l2a-ndvi/tiles/{tile.replace("/","")}/{YYYY}/{str(MM).zfill(2)}/'
    print(f"[{getCurrentDateTime()}] :", bucket, prefix)

    ar_result_exist_date = []

    result = s3_client.list_objects(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter='.tif',
    )

    if "CommonPrefixes" in result:
        for o in result['CommonPrefixes']:
            res = o['Prefix']
            # print(res)
            ar_res = res.split(f"/")
            ar_result_exist_date.append(ar_res[-1])
    else:
        print("File in folder - Do not exist!")

    return ar_result_exist_date

#####################################################################


def onDownload_File_S3(s3_client, key, outputPath):
    bucket = 'mitrphol-satellite-prod-553463144000'

    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=outputPath,
    )
    print(f"[{getCurrentDateTime()}] Downloaded: {bucket}/{key}")

#####################################################################


def upload_ndvi15(s3_client, source_upload, filepath):
    taget_bucket = 'mitrphol-satellite-prod-553463144000'
    target_upload = f"sentinel-s2-l2a-ndvi15/tiles/{filepath}"

    s3_client.upload_file(
        source_upload,
        taget_bucket,
        target_upload
    )
    print(f"[{getCurrentDateTime()}] Uploaded: From {source_upload} ---> {taget_bucket} {target_upload}")

#####################################################################


def onDeleteFile(FILEPATH):
    os.remove(FILEPATH)
    print(f"[{getCurrentDateTime()}] Deleted files", FILEPATH)

#####################################################################


class AttributeDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def onGetArgs():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-ds', '--DAYSTART',  help='', required=True)
    parser.add_argument('-de', '--DAYEND',    help='', required=True)
    parser.add_argument('-tile', '--TILE',    help='', required=False)
    parser.add_argument('-mode', '--MODE',    help='', required=True)
    args = vars(parser.parse_args())
    a = AttributeDict(args)

    a['BASE_DIR'] = str(Path(__file__).resolve().parent)
    a['PROCESS_PATH'] = f"{a.BASE_DIR}/process"

    if a.TILE == None or a.TILE == "ALL":
        a['TILES'] = onGetTileList("ALL")
    else:
        a['TILES'] = [f"{a.TILE}"]

    a['ar_YYYYMM'] = onGetListProcessMonth(a.DAYSTART, a.DAYEND)

    print(json.dumps(a, sort_keys=False, indent=4))
    return a


#####################################################################
if __name__ == "__main__":
    print("##### Start Process #####")
    a = onGetArgs()

    # ---------------------------------------------------------------

    for YYYYMM in a.ar_YYYYMM:
        arYYYY_MM = YYYYMM.split("-")
        YYYY = arYYYY_MM[0]
        MM = str(arYYYY_MM[1]).zfill(2)

        for TILE in a.TILES:
            ##########################################################
            print("Process at : ", TILE, YYYY, MM)
            s3_client = boto3.client("s3")

            ar_File = getDateExistMP(s3_client, TILE, YYYY, MM)
            print(ar_File)
            ##########################################################

            if len(ar_File) > 0:
                # Download all file if exist
                for fNDVI in ar_File:
                    onDownload_File_S3(
                        s3_client,
                        f'sentinel-s2-l2a-ndvi/tiles/{TILE}/{YYYY}/{MM}/{fNDVI}',
                        f'{a.PROCESS_PATH}/{fNDVI}'
                    )

                NDVI_FileList_00 = []
                NDVI_FileList_01 = []
                NDVI_FileList_16 = []

                # Add file path into array
                for fNDVI in ar_File:
                    file_path = f"{a.PROCESS_PATH}/{fNDVI}"

                    date = fNDVI.split("_")[2]
                    day = int(date[-2:])

                    NDVI_FileList_00.append(file_path)
                    if day <= 15:
                        NDVI_FileList_01.append(file_path)
                    elif day >= 16 and day <= 31:
                        NDVI_FileList_16.append(file_path)
                    else:
                        pass

                def onProcessComposite(a, arListFilePath, FILENAME):
                    print("Process Composite in arFile:", arListFilePath)
                    print(arListFilePath)
                    print("#### File output name:", FILENAME)

                    outputfile = f"{a.PROCESS_PATH}/{FILENAME}"
                    result = createComposite(
                        imgNDVIList=arListFilePath,
                        operator="max",
                        OutputFile=outputfile
                    )

                    if result == 1:
                        upload_ndvi15(
                            s3_client,
                            outputfile,
                            f"{TILE}/{YYYY}/{MM}/{FILENAME}"
                        )
                        # sometimes when you want to delete file now You cant cuz it's so fast you have to wait about 1 secs or more for delete file
                        time.sleep(1)
                        onDeleteFile(outputfile)
                    else:
                        print("!! Nothing uploaded !")

                FILENAME_01 = f"S2_T{TILE}_{YYYY}{MM}01_NDVI15.tif"
                FILENAME_16 = f"S2_T{TILE}_{YYYY}{MM}16_NDVI15.tif"
                FILENAME_00 = f"S2_T{TILE}_{YYYY}{MM}00_NDVI15.tif"

                if a.MODE == "01":
                    onProcessComposite(a, NDVI_FileList_01, FILENAME_01)
                elif a.MODE == "16":
                    onProcessComposite(a, NDVI_FileList_16, FILENAME_16)
                elif a.MODE == "00":
                    onProcessComposite(a, NDVI_FileList_00, FILENAME_00)
                elif a.MODE == "1600":
                    onProcessComposite(a, NDVI_FileList_16, FILENAME_16)
                    onProcessComposite(a, NDVI_FileList_00, FILENAME_00)
                elif a.MODE == "ALL":
                    onProcessComposite(a, NDVI_FileList_01, FILENAME_01)
                    onProcessComposite(a, NDVI_FileList_16, FILENAME_16)
                    onProcessComposite(a, NDVI_FileList_00, FILENAME_00)
                else:
                    print(
                        "Do not process anything because no mode exist the list is [01,16,1600,ALL]")

                # ------------------------------------------------------------------------------------
                for deleteNDVI in NDVI_FileList_00:
                    onDeleteFile(deleteNDVI)
                # ------------------------------------------------------------------------------------
                print("################################")
