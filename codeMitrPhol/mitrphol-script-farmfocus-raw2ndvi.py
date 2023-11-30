###############################################################
import boto3
import os, json, argparse #import มา ทำไม
import socket #ใช้ทำอะไร
from pathlib import Path
import pandas as pd

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pytz

import numpy as np
np.seterr(divide = 'ignore', invalid = 'ignore') # to avoid error due to 'divide by zero' or 'divide by Nan'

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import geopandas as gpd
from osgeo import gdal, osr #import มาใช้กับอะไร
###############################################################
def onStandardize_TileIDtoPath(t): 
    t = str(t).replace("/","")  #   47QRU->47/Q/RU
    if len(t) == 5: #ทำไมต้องเท่ากับ 5?
        return f"{t[0:2]}/{t[2]}/{t[3:5]}"
    else:
        raise Exception(f"Something wrong with TileID! --> {t}")

def onStandardize_TileIDtoID(t): 
    t = str(t).replace("/","") # 47/Q/RU -> 47QRU
    if len(t) != 5:
        raise Exception(f"Something wrong with TileID! --> {t}")
    else:
        return t
###############################################################
def onGetListProcessMonth(DATE_START, DATE_ENDED): # function วน date 
    ads = DATE_START.split("-")
    ade = DATE_ENDED.split("-")
    start_date = date(int(ads[0]), int(ads[1]), 1)
    end_date = date(int(ade[0]), int(ade[1]), 1)

    result = []
    while start_date <= end_date:
        YYYY_MM = start_date.strftime("%Y-%m")
        result.append(YYYY_MM)
        start_date += relativedelta(months = 1)

    return result

###############################################################
def createFolder(sPathParentFolder): #ใช้ในการเปิดไฟล์
    try:
        os.makedirs(sPathParentFolder) # os ใช้ในการสร้างไฟล์หรือว่ามันคือภาษา cmd ที่เขียนใน python ได้ โดยที่ต้อง import มาใช่หรือไม่
        print("Directory " , sPathParentFolder ,  " Created ") 
    except FileExistsError: 
        print("Directory " , sPathParentFolder ,  " already exists")

###############################################################
'''
Get tile list from excel on aws s3
'''
def onGetTileFromMasterExcel(a, s3_client): #Mastertile จะเก็บว่าแต่ละโรงงานมีำฟล์อะไรบ้าง
    s3_client.download_file(
        Bucket = "mitrphol-farmfocus-prod-553463144000", 
        Key = f"master/tile/{a.FILENAME_TILES_EXCEL}", 
        Filename = f"{a.PATH_INPUT_FOLDER_EXCEL}/{a.FILENAME_TILES_EXCEL}",
    )
    dict_df = pd.read_excel(f"{a.PATH_INPUT_FOLDER_EXCEL}/{a.FILENAME_TILES_EXCEL}", sheet_name=None, header=0)
    return dict_df
###############################################################
def onGetS3Session(): #ไฟล์ในเครื่องพี่กับไฟล์ใน s3 มันแตกต่างกันยังไง
    if str(socket.gethostname()) == "NATAKRANP-MBP.local": 
        df = pd.read_csv('/Users/natakranp/credential-mitrphol-farmfocus-user-natakranp-prod.csv')
        credential = df.iloc[0] #คำถามคือ ใช้ iloc เสร็จแล้วได้ data เป็น type อะไร
        session = boto3.Session(
            aws_access_key_id     = credential['AccesskeyID'],
            aws_secret_access_key = credential['Secretaccesskey'],
        )
        s3_client = session.client('s3')
    else:
        s3_client = boto3.client('s3')
    
    return s3_client

###############################################################
'''
Get arguments and parse to dict
'''
def onGetArgs(): #datatype = json
    class AttributeDict(dict): #class ที่ทำให้ arg ถูก convert ไปในรูปของ dict
        __getattr__ = dict.__getitem__  
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    parser = argparse.ArgumentParser( description = 'farmfocus-raw2ndvi-prod' )
    parser.add_argument('-ds',  '--DAYSTART',  help='YYYY-MM', required=False) # สามารถรับค่าเจอ terminal ไ้ด
    parser.add_argument('-de',  '--DAYEND',    help='YYYY-MM', required=False) # โดยที่ใช้ space ในการรับหลายๆค่า
    parser.add_argument('-loc', '--LOCATION',  help='MPK', required=True) # return เป็น json
    args = vars(parser.parse_args())
    a = AttributeDict(args)

    a['HOSTNAME']       = str(socket.gethostname())
    a['BASE_DIR']       = str(Path(__file__).resolve().parent) #เมื่อเราไปรัน server จริงๆทำให้รู้ว่่ามันจะอยู่ path ไหน

    a['PATH_OUTPUT_FOLDER_NDVI'] = "/opt/ml/processing/output/ndvi/"
    a['PATH_INPUT_FOLDER_RASTER'] = "/opt/ml/processing/input/raster/"

    a['PATH_INPUT_FOLDER_EXCEL'] = "/opt/ml/processing/input/excel/"
    a['FILENAME_TILES_EXCEL']    = "TILES.xlsx"

    createFolder(a.PATH_INPUT_FOLDER_EXCEL) 
    createFolder(a.PATH_INPUT_FOLDER_RASTER)
    createFolder(a.PATH_OUTPUT_FOLDER_NDVI)

    # Get tile list
    s3_client  = onGetS3Session()
    dict_df    = onGetTileFromMasterExcel(a, s3_client)
    a['TILES'] = dict_df[a.LOCATION]['TILEID'].to_list()


    if (a.DAYSTART != None) and (a.DAYEND != None): # ดัก debug
        a['ar_YYYYMM'] = onGetListProcessMonth(a.DAYSTART, a.DAYEND)

    print(json.dumps(a, sort_keys=False, indent=4)) 
    return a

###############################################################
class s3accessProd:
    def __init__(self, s3_client):
        self.name     = "s3accessProd"
        self.bucket    = "mitrphol-satellite-prod-553463144000"
        self.source_prefix   = "satellite/tiles"#คืออะไร
        self.target_prefix   = "sentinel-s2-l2a-ndvi/tiles" #l2a คืออะไร
        self.s3_client = s3_client #เขียนไปแล้วใน function นี้ เพราะจะได้ไม่ต้องเขียนอีกรอบ onGetS3Session

    def onDownload(self, key, outputPath): # download นี่ จากไฟล์ ในเครื่องเราใช่หรือไม่
        print(f"Downloading: {self.bucket}/{key}")
        self.s3_client.download_file(
            Bucket = self.bucket, 
            Key = key, 
            Filename = outputPath,
        )
        print(f"Downloaded: {self.bucket}/{key}")

    def onDownloadFile2ProcessNDVI(self, ProcessPATH, tile, YYYY, MM, DD): 
        print(f"onDownloadFile2ProcessNDVI ({ProcessPATH}, {tile}, {YYYY}, {MM}, {DD})")

        tile = onStandardize_TileIDtoPath(tile)
        YYYY = str(int(YYYY))
        MM   = str(int(MM))
        DD   = str(int(DD))
        prefix_download = f'{self.source_prefix}/{tile}/{YYYY}/{MM}/{DD}'.replace('//','/')

        verSCL = 0 # version 0 กับ 1 ต่างกันยังไง

        for band_R10m in ['B04','B08']: #download ndvi เข้ามา / หมายความว่า เรา get มาบาง band ใช่หรือไม่
            try:
                self.onDownload(
                    f'{prefix_download}/0/R10m/{band_R10m}.jp2', 
                    f'{ProcessPATH}/{band_R10m}.jp2'
                )
                verSCL = 0
            except:
                # in-case of version 0 file do not exist
                self.onDownload(
                    f'{prefix_download}/1/R10m/{band_R10m}.jp2', 
                    f'{ProcessPATH}/{band_R10m}.jp2'
                )
                verSCL = 1
                print(f" -> Using version {verSCL}")

        for band_R20m in ['SCL']: #ใน s3 ของ service มีให้ download SCL ใช่หรือไม่
            self.onDownload(
                f'{prefix_download}/{verSCL}/R20m/{band_R20m}.jp2', 
                f'{ProcessPATH}/{band_R20m}.jp2'
            ) # download มาในเครื่องตัวเองใช่หรือไม่

    def onUploadFileNDVI(self, source_upload, TILE, YYYY, MM): # upload ไปไหน | เดือนกับปี datatype เป็น str หรือว่า datatime
        print(f"onUploadFileNDVI ({source_upload}, {TILE}, {YYYY}, {MM})")

        TILE = onStandardize_TileIDtoID(TILE)

        filename = os.path.basename(source_upload)
        MM = str(MM).zfill(2) #zfill คืออะไร 
        
        target_upload = f"{self.target_prefix}/{TILE}/{YYYY}/{MM}/{filename}"
        
        self.s3_client.upload_file(
            Filename = source_upload,
            Bucket   = self.bucket, 
            Key      = target_upload
        )
        print(f"Uploaded: {source_upload} -> {self.bucket}/{target_upload}") 


####################################################
class s3compareFileProd:
    def __init__(self, s3_client):
        self.name     = "s3compareFileProd"
        self.bucket   = "mitrphol-satellite-prod-553463144000"
        self.source_prefix   = "satellite/tiles"
        self.target_prefix   = "sentinel-s2-l2a-ndvi/tiles"
        self.s3_client = s3_client

    def onListDate_AWSL2A(self, tile, YYYY, MM):
        print(f"onListDate_AWSL2A ({tile}, {YYYY}, {MM})")

        tile  = onStandardize_TileIDtoPath(tile)

        MM = str(int(MM))
        YYYY = str(YYYY)
        bucket = self.bucket
        prefix = f'{self.source_prefix}/{tile}/{YYYY}/{MM}/'

        print(f"{bucket}/{prefix}")
        
        ar_result_exist_date = []

        result = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix, 
            Delimiter='/',
        ) #คืออะไร list_object เป็นแบบไหน | ได้อะไร

        try:
            for o in result['CommonPrefixes']: #อันนี้ [ข้างใน bucket เป็นอะไร] ใช่ key ไหม | แล้ว o ได้ออกมาเป็น value ใหม่
                res = o['Prefix'] # คำว่า Prefix เป็น value หรือว่าเป็น key | ที่ไปเรียก value ในส่วนที่ต้องการแล้ว | ถ้าเรียก value นั้นคืออะไร
                ar_res = res.split(f"/")
                ar_result_exist_date.append(int(ar_res[-2])) # -2 becos it include / at last path  #ตัวแปรอ่านว่าอะไร
        except Exception as ex:
            print(f"This month no data or error! / Exception: {ex}")
        
        date_list = np.sort(ar_result_exist_date) 
        print(f" -> {date_list}")
        return np.array(date_list) #ทำไมต้อง return เป็น array 

    def onListDate_BucketFarmFocus(self, tile, YYYY, MM):
        print(f"onListDate_BucketFarmFocus ({tile}, {YYYY}, {MM})")

        MM = str(int(MM)).zfill(2)
        YYYY = str(YYYY)
        tile = onStandardize_TileIDtoID(tile)

        bucket = self.bucket
        prefix = f'{self.target_prefix}/{tile}/{YYYY}/{MM}/'
        print(f"{bucket}/{prefix}")

        ar_result_exist_date = []
        result = self.s3_client.list_objects(
            Bucket=bucket,
            Prefix=prefix, 
            Delimiter='.tif',
        ) # list_objects นี้ต่างกับ ตัว v2 ยังไง
        if "CommonPrefixes" in result: #มันคืออะไร
            for o in result['CommonPrefixes']:
                res = o['Prefix']
                ar_res = res.split(f"/")
                file_name = ar_res      [-1] # S2_T48QUD_20190802_NDVI.tif
                ar_file_name = file_name.split("_")
                file_name_date = ar_file_name[2] # ..20190802..
                date_file_name = datetime.strptime(file_name_date, "%Y%m%d").date() #ได้เป็น str หรือ datetime

                ar_result_exist_date.append(date_file_name.day) 
        else:
            print("Do not exist!")
        
        date_list = np.sort(ar_result_exist_date)

        print(f" -> {date_list}")
        return np.array(date_list)

    def onFindDiffDate(self, tile, YYYY, MM): #หา diff ทำไม
        print(f"onFindDiffDate ({tile}, {YYYY}, {MM})")

        ar_source = self.onListDate_AWSL2A(tile, YYYY, MM)
        ar_target = self.onListDate_BucketFarmFocus(tile, YYYY, MM) # self.function ใน class หมายความว่าอะไร
        
        result = list(set(ar_source) - set(ar_target))
        print(f"Difference date : {result}")
        print()

        return np.sort(result)

    def onFindDateSource(self, tile, YYYY, MM): # data source ไม่ได้มี วันบอกอยู่แล้วหอรเราถึงต้องหา วันเพิ่ม
        print(f"onFindDateSource ({tile}, {YYYY}, {MM})")

        ar_source = self.onListDate_AWSL2A(tile, YYYY, MM)
        
        result = list(set(ar_source))
        print(f"Force target date : {result}")
        print()

        return np.sort(result)
###############################################################

######################
# L2A to VI
######################
def getCurrentDateTime():
    d = datetime.now(pytz.timezone('Asia/Bangkok'))
    return d.strftime("%Y-%m-%d %H:%M:%S")

def computeVI_AllCloudMask(B2, B4, B8, SCL, outputDS, vi, maskcloud, offset):
    print(f"computeVI_AllCloudMask()")
    
    outBand = outputDS.GetRasterBand(1)
    
    sizeX = outputDS.RasterXSize
    sizeY = outputDS.RasterYSize
    
#     blockSize = B2.GetBlockSize()
    blockSize = B4.GetBlockSize()
    blockSizeX = blockSize[0]
    blockSizeY = blockSize[1]
    
    # reading image and preforming operation row by row
    for y in range(0, sizeY, blockSizeY):
        # if condition will read from 2nd row to last row of the image
        if y + blockSizeY < sizeY:
            row = blockSizeY
        # else condition will read the first row of the image
        else:
            row = sizeY - y

        # reading all the columns of each row at once
        for x in range (0, sizeX, blockSizeX):
            # if condition will always be false if blockSizeX is equal to sizeX
            if x + blockSizeX < sizeX:
                col = blockSizeX
            else:
                col = sizeX - x
            
            #mask
            scl = SCL.ReadAsArray(x, y, col, row).astype(np.float32)
            ## 0 - No Data, 1 = Saturated, 8 - Cloud (Low Probability), 9 - Cloud (High Probability), 3 Cloud Shadow
            mask = (scl == 0) | (scl == 1) | (scl == 8) | (scl == 9) | (scl == 3) | (scl == 10) | (scl == 7) | (scl == 11)
            # mask = (scl == 0) | (scl == 1) | (scl == 9)
            
            red = (B4.ReadAsArray(x, y, col, row).astype(np.float32) + offset) / 10000
            nir = (B8.ReadAsArray(x, y, col, row).astype(np.float32) + offset) / 10000
#             blue = B2.ReadAsArray(x, y, col, row).astype(np.float32)/10000

            if maskcloud == 1:
                red[mask] = np.nan
                nir[mask] = np.nan
#                 blue[mask] = np.nan

            # ndvi
            if vi == 'ndvi':
                # ndvi
                VI_value = ((nir - red) / (nir + red))
                # assign np.nan to out of range NDVI values (Edited:2023-04-27)
                VI_value[(VI_value < -1.0) | (VI_value > 1.0)] = np.nan
            
            # # evi
            # elif vi == 'evi':
            #     # evi
            #     VI_value = (2.5*(nir - red) / (nir + 6.0 * red - 7.5 * blue + 1))
            
            # # savi
            # elif vi == 'savi':
            #     # savi
            #     L = 0.428
            #     VI_value = ((1.0 + L) * (nir - red) / (nir + red + L))
                
            else:
                print('[{}] {} cannot be computed'.format(getCurrentDateTime(), vi))

            # wrting the computed VI to the band
            outBand.WriteArray(VI_value, x, y)
    
    # flushing memory
    outBand = None
    blue = None
    nir = None
    red = None
    scl = None
    VI_value = None
#     B2 = None
    B4 = None
    B8 = None
    SCL = None
    
    # returning the 
    return outputDS

def reproject_VI2_Inmemory(ds, outputPath):
    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
    ds.BuildOverviews('cubic', [2, 4, 8, 16, 32, 64, 128])
    driver = gdal.GetDriverByName('GTiff') # saving as GeoTIFF
    # output image filename / COMPRESS = DEFLATE OR LZW
    dst_ds = driver.CreateCopy(outputPath, ds, options = ["COPY_SRC_OVERVIEWS=YES", "TILED=YES", "COMPRESS=DEFLATE"]) 
    ds = None
    dst_ds = None

def NDVI_Process_AllCloudMask(B2File, B4File, B8File, SCLFile, NDVIOutput, offset):
    print(f"NDVI_Process_AllCloudMask()")
#     B2_ds = gdal.Open(B2File)
    B4_ds = gdal.Open(B4File)
    B8_ds = gdal.Open(B8File)
    SCL_ds = gdal.Open(SCLFile)

    # getting the spatial information from the input file
    geoTransform = B4_ds.GetGeoTransform()
    geoProjection = B4_ds.GetProjection()

    # size of the image
    sizeX = B4_ds.RasterXSize
    sizeY = B4_ds.RasterYSize
    # print('Image Size: {}, {}'.format(sizeX, sizeY))

    # extracting the SRS of input image file
    # input image file SRS
    dsSRS = osr.SpatialReference(geoProjection)
    dsEPSG = dsSRS.GetAttrValue('Authority', 1)
    toEPSG = 'EPSG:'+str(dsEPSG)

    # converting SCL from 20 m to 10 m
    SCL_ds1 = gdal.Warp('', SCL_ds, dstSRS = toEPSG, format = 'VRT', outputType = gdal.GDT_Byte, xRes = 10.0, yRes = 10.0)
    # print(SCL_ds1.GetGeoTransform())
    SCL = SCL_ds1.GetRasterBand(1)
    # print(gdal.GetDataTypeName(b.DataType))

    # reading the first band
#     B2 = B2_ds.GetRasterBand(1)
    B4 = B4_ds.GetRasterBand(1)
    B8 = B8_ds.GetRasterBand(1)

    # NDVI
    NDVIFileName = NDVIOutput
    print('[{}] NDVI Filename: {}'.format(getCurrentDateTime(), NDVIFileName))

    driver = gdal.GetDriverByName ( "GTiff" )
    dsOutput = driver.Create(NDVIFileName, sizeX, sizeY, 1, gdal.GDT_Float32)
    dsOutput = computeVI_AllCloudMask(None, B4, B8, SCL, dsOutput, 'ndvi', 1, offset)
    dsOutput.SetProjection(geoProjection)
    dsOutput.SetGeoTransform(geoTransform)
    dsOutput.GetRasterBand(1).SetNoDataValue(-9999)
    
    reproject_VI2_Inmemory(dsOutput, NDVIFileName )
    # flushing memory
#     B2_ds = None
    B4_ds = None
    B8_ds = None
    SCL_ds = None
    SCL_ds1 = None
#     B2 = None
    B4 = None
    B8 = None
    SCL = None
    dsOutput = None

############################################################################################################
def process_job(a, sYYYYMM, force_process=False):
    arYYYYMM = sYYYYMM.split("-")
    Year  = arYYYYMM[0]
    Month = arYYYYMM[1]

    print(f"########################### Process Job : {Year}-{Month} ##################################")

    ##############################################################
    ar_process = []
    s3session = onGetS3Session()
    m_s3compareFileProd = s3compareFileProd(s3session)

    for TILE in a.TILES:
        if force_process == False:
            ar_Date2Process = m_s3compareFileProd.onFindDiffDate(TILE, Year, Month)
        else:
            ar_Date2Process = m_s3compareFileProd.onFindDateSource(TILE, Year, Month)

        ar_TileAndDates = [TILE, Year, Month, ar_Date2Process]
        ar_process.append(ar_TileAndDates)
    
    for observ in ar_process:
        print(observ)

    for process in ar_process:
        print(f"Process : ######################################")
        print(f"Process : {process}") # ['00TIL', '2022', '08', [26, 21, 31]]
        TILE = process[0]
        YYYY = process[1]
        MM = process[2]
        
        for DD in process[3]:
            m_s3accessProd = s3accessProd(s3session)

            print(f"Start process : {TILE}, {YYYY}, {MM}, {DD}")
            m_s3accessProd.onDownloadFile2ProcessNDVI(a.PATH_INPUT_FOLDER_RASTER, TILE, YYYY, MM, DD)
            DD = str(DD).zfill(2)
            filename_ndvi_output = f"S2_T{TILE}_{YYYY}{MM}{DD}_NDVI.tif"
            NDVIOutputPATH = f"{a.PATH_OUTPUT_FOLDER_NDVI}{filename_ndvi_output}"

            offset = 0
            if datetime(2022, 1, 25) <= datetime(int(YYYY), int(MM), int(DD)):
                offset = -1000

            print(f"Offset :{offset}")

            NDVI_Process_AllCloudMask(
                    B2File     = None,
                    B4File     = f"{a.PATH_INPUT_FOLDER_RASTER}B04.jp2",
                    B8File     = f"{a.PATH_INPUT_FOLDER_RASTER}B08.jp2", 
                    SCLFile    = f"{a.PATH_INPUT_FOLDER_RASTER}SCL.jp2",
                    NDVIOutput = NDVIOutputPATH,
                    offset     = offset
            )

            print("NDVI Output: ", filename_ndvi_output)


            m_s3accessProd.onUploadFileNDVI(NDVIOutputPATH, TILE, YYYY, MM)

            os.remove(f"{a.PATH_INPUT_FOLDER_RASTER}B04.jp2")
            os.remove(f"{a.PATH_INPUT_FOLDER_RASTER}B08.jp2")
            os.remove(f"{a.PATH_INPUT_FOLDER_RASTER}SCL.jp2")
            os.remove(NDVIOutputPATH)
            print(f"Deleted local process files")

    
##################################################################################################

if __name__ == "__main__":
    a = onGetArgs()

    try:
        if (a.DAYSTART != None) and (a.DAYEND != None):
            print("Force processing : replace files")

            for YYYY_MM in a.ar_YYYYMM:
                process_job(a, YYYY_MM, True)

        else:
            print("Schedule processing : comparison files")
            YYYYMM = datetime.now(pytz.timezone('Asia/Bangkok'))
            sYYYYMM = YYYYMM.strftime("%Y-%m")
            process_job(a, sYYYYMM, False)

            if YYYYMM.day <= 10:
                prev_month_YYYYMM  = YYYYMM - relativedelta(months = 1)
                sYYYYMM = prev_month_YYYYMM.strftime("%Y-%m")
                process_job(a, sYYYYMM, False)
    
    except Exception as ex:

        print("error:", ex)
        raise Exception("Raise Exception")

