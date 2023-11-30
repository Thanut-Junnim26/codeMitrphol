###############################################################
import boto3
from botocore.exceptions import ClientError
import os
import json
import argparse
import socket
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely import wkt
import redshift_connector
import yaml
from urllib.parse import urlparse

import numpy as np
from re import search

import warnings
warnings.filterwarnings("ignore")

###############################################################


def onGetBotoSession():
    if str(socket.gethostname()) == "NATAKRANP-MBP.local":
        # This for local development testing.
        df = pd.read_csv(
            '/Users/natakranp/credential-mitrphol-farmfocus-user-natakranp-prod.csv')
        credential = df.iloc[0]

        session = boto3.Session(
            aws_access_key_id=credential['AccesskeyID'],
            aws_secret_access_key=credential['Secretaccesskey'],
        )
        print("Local")
    else:
        # This will invoke from any aws services.
        session = boto3.session.Session()
        print("AWS cloud")
    return session
###############################################################


def createFolder(sPathParentFolder):
    try:
        os.makedirs(sPathParentFolder)
        print("Directory ", sPathParentFolder,  " Created ")
    except FileExistsError:
        print("Directory ", sPathParentFolder,  " already exists")
###############################################################
# def get_secret(session):
#     secret = yaml.load(open('env.yaml'), Loader=yaml.loader.SafeLoader)
#     return (secret)


def get_secret(session):
    client = session.client(
        service_name='secretsmanager',
        region_name="ap-southeast-1"
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId="prod/mitrphol-redshift-prod/mitrphol-farmfocus"
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)
###############################################################


def s3DownloadFile(boto3session, SOURCE_S3PATH, TARGET_LOCALPATH):
    print("s3DownloadFile", SOURCE_S3PATH, TARGET_LOCALPATH)
    o = urlparse(SOURCE_S3PATH, allow_fragments=False)
    BUCKET = o.netloc
    KEY = o.path.lstrip('/')

    s3_client = boto3session.client('s3')
    s3_client.download_file(
        Bucket=BUCKET,
        Key=KEY,
        Filename=TARGET_LOCALPATH,
    )


def s3UploadFile(boto3session, SOURCE_PARH, TARGET_S3PATH):
    print("s3UploadFile", SOURCE_PARH, TARGET_S3PATH)
    o = urlparse(TARGET_S3PATH, allow_fragments=False)
    BUCKET = o.netloc
    KEY = o.path.lstrip('/')

    s3_client = boto3session.client('s3')
    s3_client.upload_file(
        Filename=SOURCE_PARH,
        Bucket=BUCKET,
        Key=KEY
    )
###############################################################


def step_JoinLastYearWithHarvest(df_lastyear, df_harvest):
    df_lastyear = df_lastyear.copy()
    df_harvest = df_harvest.copy()
    # ยึด Rows ของ Lastyear เป็นหลักและเติม HarvestDate เข้าไปด้วย How='Left'
    df_lastyear = df_lastyear.merge(
        df_harvest, left_on='plot_id', right_on='plot_id', how='left')  # if df.harvest.sizr == df.lastyear.size: it's ok
    df_lastyear = df_lastyear[[
        'production_year',
        'company_code_x',
        'quota',
        'plot_code_x',
        'plot_distance',
        'area_size_x',
        'plot_gis_status_x',
        'plant_date',
        'plot_type',
        'plot_type_ff',
        'soil_name',
        'watering_type',
        'water_source_name',
        'zone_id',
        'sub_zone_id',
        'subspecies_name',
        'plot_id',
        'geometry',
        'maxpixel',
        'harvested',
        'harvested_area',
        'percentharvested',
        'cloud',
        'percentcloud',
        'unharvested',
        'percentunharvested',
        'plotstatus',
        'harvestdate',
        'process_date'
    ]]

    df_lastyear = df_lastyear.rename(columns={
        'company_code_x': 'company_code',
        'plot_code_x': 'plot_code',
        'area_size_x': 'area_size',
        'plot_gis_status_x': 'plot_gis_status',
        'zone_id': 'zone_id',
        'sub_zone_id': 'sub_zone_id'
    })

    return df_lastyear
###############################################################


'''
Get arguments and parse to dict
'''


def onGetArgs():
    class AttributeDict(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    parser = argparse.ArgumentParser(
        description='farmfocus-masterplot-hv-prod')
    parser.add_argument('-DATELASTYEAR', '--DATELASTYEAR',
                        help='YYYYMMDD', required=False)
    parser.add_argument('-DATETODAY', '--DATETODAY',
                        help='YYYYMMDD', required=False)
    args = vars(parser.parse_args())
    a = AttributeDict(args)

    a['BUCKET_TARGET'] = "mitrphol-farmfocus-prod-553463144000"
    a['KEY_PATH_TARGET'] = "master/plot_hv"
    a['HOSTNAME'] = str(socket.gethostname())
    a['BASE_DIR'] = str(Path(__file__).resolve().parent)

    a['PATH_INPUT_FOLDER'] = "/opt/ml/processing/masterplothv/input/"
    a['PATH_OUTPUT_FOLDER'] = "/opt/ml/processing/masterplothv/output/"
    a['PATH_S3_MASTERPLOT'] = "s3://mitrphol-farmfocus-prod-553463144000/master/plot/"

    a['PATH_S3_OUTPUT_MASTERPLOT_HV'] = "s3://mitrphol-farmfocus-prod-553463144000/master/plot_hv_new/"

    a['FILENAME_PLOT_LASTYEAR'] = f"{a['DATELASTYEAR']}_PLOT.geojson"
    a['FILENAME_PLOT_TODAY'] = f"{a['DATETODAY']}_PLOT.geojson"
    a['FILENAME_REDSHIFT_HARVEST'] = "HARVEST_RESULT.csv"

    a['FILENAME_PLOT_TODAY_OUTPUT_CSV'] = f"{a['DATETODAY']}_PLOT_HV.csv"
    a['FILENAME_PLOT_TODAY_OUTPUT_GEOJSON'] = f"{a['DATETODAY']}_PLOT_HV.geojson"

    print(json.dumps(a, sort_keys=False, indent=4))
    return a


###############################################################
'''
Main Function
'''


def main():
    PROD_MODE = True
    a = onGetArgs()
    createFolder(a.PATH_INPUT_FOLDER)
    createFolder(a.PATH_OUTPUT_FOLDER)

    session = onGetBotoSession()
    secret = get_secret(session)

    if PROD_MODE:
        # -------------------- Data preparation ----------------------
        print("Download masterplot lastyear")
        source = a.PATH_S3_MASTERPLOT + a.FILENAME_PLOT_LASTYEAR
        target = a.PATH_INPUT_FOLDER + a.FILENAME_PLOT_LASTYEAR
        s3DownloadFile(session, source, target)

        print("Download masterplot today")
        source = a.PATH_S3_MASTERPLOT + a.FILENAME_PLOT_TODAY
        target = a.PATH_INPUT_FOLDER + a.FILENAME_PLOT_TODAY
        s3DownloadFile(session, source, target)

        print("Redshift harvest to csv")
        conn = redshift_connector.connect(
            host=secret['host'],
            database='mitrphol_prod',
            user=secret['username'],
            password=secret['password']
        )
        cursor: redshift_connector.Cursor = conn.cursor()
        # Go to harvesttttttt
        cursor.execute("SELECT * FROM mitrphol_prod.farm_focus.fact_harvest")
        df: pd.DataFrame = cursor.fetch_dataframe()
        df.to_csv(a.PATH_INPUT_FOLDER +
                  a.FILENAME_REDSHIFT_HARVEST, encoding='utf-8')

    # -------------------- Data loading ----------------------
    df_harvest_main = pd.read_csv(
        a.PATH_INPUT_FOLDER + a.FILENAME_REDSHIFT_HARVEST, index_col=0)
    df_lastyear_main = gpd.read_file(
        a.PATH_INPUT_FOLDER + a.FILENAME_PLOT_LASTYEAR)
    df_today_main = gpd.read_file(a.PATH_INPUT_FOLDER + a.FILENAME_PLOT_TODAY)

    # Convert crs into 4326
    df_lastyear_main = df_lastyear_main.to_crs(epsg=int(4326))
    df_today_main = df_today_main.to_crs(epsg=int(4326))

    df_lastyear = df_lastyear_main.copy()
    df_today = df_today_main.copy()
    df_harvest = df_harvest_main.copy()

    print(
        f"[CHECK_LEN : Original] df_lastyear={len(df_lastyear)} | df_today={len(df_today)} | df_harvest={len(df_harvest)}")

    # IF FAC == MAC PlantType = None Replace by "RT"
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Remove after MAC have PlotType
    df_today.loc[df_today['company_code'] == 'MAC', 'plot_type_ff'] = 'RT'
    print("[!!!!!] Replace MAC PlotType with == RT")

    df_today_NonRT = df_today.query("plot_type_ff != 'RT'")
    df_today = df_today.query("plot_type_ff == 'RT'")
    print(
        f"[CHECK_LEN : Filter RT] df_lastyear={len(df_lastyear)} | df_today={len(df_today)} | df_harvest={len(df_harvest)}")

    # -------------------- Left Join LastYear With Harvest ----------------------
    df_lastyear = step_JoinLastYearWithHarvest(df_lastyear, df_harvest)
    print(
        f"[CHECK_LEN : step_JoinLastYearWithHarvest] df_lastyear={len(df_lastyear)} | df_today={len(df_today)} | df_harvest={len(df_harvest)}")
    df_harvest = None
    df_harvest_main = None

    # -------------------- Inner Join Today With Lastyear ----------------------
    df_join = gpd.sjoin(df_today, df_lastyear, how="inner", op="intersects")
    print(f"[CHECK_LEN : step_JoinTodayWithLastyear] df_join={len(df_join)}")

    # -------------------- Inner Join Today With Lastyear ----------------------
    # List ของ Today หลังจาก Join ด้วย Spacial Success
    list_today_Intersection = df_join.plot_id_left.values.tolist()
    # df_123 == DF ของตัวที่ Join ไม่สำเร็จ
    df_today_UnIntersection = df_today.query(
        'plot_id != @list_today_Intersection')
    print(
        f"[CHECK_LEN : step_Keep Un-Intersection] df_today_UnIntersection={len(df_today_UnIntersection)} | list_today_Intersection={len(list_today_Intersection)}")

    # -------------------- pCheck and dif_area==0 ----------------------
    df_join.loc[df_join['plot_code_left'] ==
                df_join['plot_code_right'], 'pCheck'] = 'Y'
    df_join.loc[df_join['plot_code_left'] !=
                df_join['plot_code_right'], 'pCheck'] = 'N'
    df_join['dif_area'] = pd.to_numeric(
        df_join['area_size_left']) - pd.to_numeric(df_join['area_size_right'])
    df_join['dif_per'] = (abs(df_join['dif_area']) /
                          pd.to_numeric(df_join['area_size_left']))*100
    df_samearea = df_join[df_join['dif_area'] == 0]
    print(f"[CHECK_LEN : df_area == 0] df_samearea={len(df_samearea)}")

    # -------------------- เช็คส่วน Duplicate Area หลังจาก AreaDiff == 0 ----------------------
    dup_samearea = df_samearea[df_samearea.duplicated(
        ['plot_id_left'], keep=False)]  # ดึงกลุ่มที่ duplicate
    # Plot_code ดันตรงกันอีก เลยไปเลือก Row แรก ด้วย code ด้านล่าง
    dup_samearea2 = dup_samearea[dup_samearea['pCheck'] == 'Y']
    dup_samearea3 = dup_samearea2.drop_duplicates(
        subset=['plot_id_left'], keep="first")  # ถ้า duplicate ก็เลือก row แรก
    # จะได้แปลงทั้งหมดที่มี areasize ตรงกับแปลงปีที่แล้วมา
    print("[กลุ่ม Dup หลังจาก AreaDif passed ]", len(dup_samearea), len(dup_samearea2),
          'Last result of filter duplicate after AreaDif==0 is here ->', len(dup_samearea3))

    # -------------------- ก็คือเส้นทางขวาที่หลังจาก sjoin แล้วไม่ duplicate เลย ----------------------
    samearea2 = df_samearea.drop_duplicates(
        subset=['plot_id_left'], keep=False)  # sjoin แล้วไม่ duplicate เลย
    # เอาสองเส้นมาต่อกัน แปลงที่ไม่ duplicate และแปลงที่ duplicate แต่ผ่านการกรองแล้ว
    ap_samearea = samearea2.append(dup_samearea3, ignore_index=True)

    # -------------------- กลุ่มที่หลุด ที่ผ่าน sjoin มาแล้วแต่หลุด condition หลังจาก sjoin ----------------------
    dv = ap_samearea.plot_id_left.values.tolist()
    df = df_join.query('plot_id_left != @dv')

    dff = df.drop_duplicates(subset=['plot_id_left'], keep="first")
    dff['plot_id_right'] = None

    dff = dff[[
        'production_year_left',
        'company_code_left',
        'quota_left',
        'plot_code_left',
        'plot_distance_left',
        'area_size_left',
        'plot_gis_status_left',
        'plant_date_left',
        'plot_type_left',
        'plot_type_ff_left',
        'soil_name_left',
        'watering_type_left',
        'water_source_name_left',
        'zone_id',
        'sub_zone_id',
        'subspecies_name_left',
        'plot_id_left',
        'geometry'
    ]]

    dff = dff.rename(columns={
        'production_year_left': 'production_year',
        'company_code_left': 'company_code',
        'quota_left': 'quota',
        'plot_code_left': 'plot_code',
        'plot_distance_left': 'plot_distance',
        'area_size_left': 'area_size',
        'plot_gis_status_left': 'plot_gis_status',
        'plant_date_left': 'plant_date',
        'plot_type_left': 'plot_type',
        'plot_type_ff_left': 'plot_type_ff',
        'soil_name_left': 'soil_name',
        'watering_type_left': 'watering_type',
        'water_source_name_left': 'water_source_name',
        'zone_id': 'zone_id',
        'sub_zone_id': 'sub_zone_id',
        'subspecies_name_left': 'subspecies_name',
        'plot_id_left': 'plot_id'

    })

    # df_today_UnIntersection is dataframe bucket
    dff = pd.concat([dff, df_today_UnIntersection])
    dff['plot_id_lastyear'] = None
    dff['plant_date_ff'] = None

    # -------- Last Process เป็น condition ในการเลือก และเป้นการ check status ว่าตัดเสร็จแล้วจึงเลือกวันนั้น ----------------------
    # -- ก็คือดูว่าตัดเสร็จหรือยัง ถ้ายังก็ไม่เอาค่ามา
    df_last = ap_samearea.copy()

    df_last.loc[df_last['plotstatus'] == 'Harvested',
                'plant_date_ff'] = df_last['harvestdate']
    df_last.loc[df_last['plotstatus'] != 'Harvested', 'plant_date_ff'] = None

    df_last = df_last[[
        'production_year_left',
        'company_code_left',
        'quota_left',
        'plot_code_left',
        'plot_distance_left',
        'area_size_left',
        'plot_gis_status_left',
        'plant_date_left',
        'plot_type_left',
        'plot_type_ff_left',
        'soil_name_left',
        'watering_type_left',
        'water_source_name_left',
        'zone_id',
        'sub_zone_id',
        'subspecies_name_left',
        'plot_id_left',
        'plot_id_right',
        'plant_date_ff',
        'geometry',
    ]]

    df_last = df_last.rename(columns={
        'production_year_left': 'production_year',
        'company_code_left': 'company_code',
        'quota_left': 'quota',
        'plot_code_left': 'plot_code',
        'plot_distance_left': 'plot_distance',
        'area_size_left': 'area_size',
        'plot_gis_status_left': 'plot_gis_status',
        'plant_date_left': 'plant_date',
        'plot_type_left': 'plot_type',
        'plot_type_ff_left': 'plot_type_ff',
        'soil_name_left': 'soil_name',
        'watering_type_left': 'watering_type',
        'water_source_name_left': 'water_source_name',
        'zone_id': 'zone_id',
        'sub_zone_id': 'sub_zone_id',
        'subspecies_name_left': 'subspecies_name',
        'plot_id_left': 'plot_id',
        'plot_id_right': 'plot_id_lastyear'
    })

    df_real_last = pd.concat([df_last, dff])
    print(f"[Result after MAPPING] df_real_last={len(df_real_last)}")

    df_today_NonRT['plot_id_lastyear'] = None
    df_today_NonRT['plant_date_ff'] = None

    df_real_last_result = pd.concat([df_real_last, df_today_NonRT])
    df_real_last_result = df_real_last_result.reset_index(drop=True)

    print(
        f"[LAST_RESULT] df_real_last_result={len(df_real_last_result)} --- Must Equal --- df_today={len(df_today_main)}")

    # Soil_type Process #############################################
    def SoilTypeDefine(x):
        if x == None or x == np.nan or x == "None":
            return None
        else:
            if search("ทราย", x):
                if search("เหนียว", x):
                    return "Clay"
                else:
                    return "Sand"
            else:
                return "Clay"

    df_real_last_result['soil_type1'] = df_real_last_result['soil_name'].apply(
        SoilTypeDefine)
    df_real_last_result['soil_type2'] = None  # SoilGrids

    df_real_last_result['ch_plant_date_ff'] = None
    df_real_last_result['ch_plant_date_ff'] = df_real_last_result['plant_date_ff']
    df_real_last_result.loc[df_real_last_result['plot_type_ff'] ==
                            'NP', 'ch_plant_date_ff'] = df_real_last_result['plant_date']

    #################################################################

    df_real_last_result.to_file(
        a.PATH_OUTPUT_FOLDER + a.FILENAME_PLOT_TODAY_OUTPUT_GEOJSON, driver='GeoJSON', encoding='utf-8')
    df_real_last_result.drop(columns=['geometry']).to_csv(
        a.PATH_OUTPUT_FOLDER + a.FILENAME_PLOT_TODAY_OUTPUT_CSV, encoding='utf-8')

    print("Upload output masterplot_hv csv")
    source = a.PATH_OUTPUT_FOLDER + a.FILENAME_PLOT_TODAY_OUTPUT_CSV
    target = a.PATH_S3_OUTPUT_MASTERPLOT_HV + a.FILENAME_PLOT_TODAY_OUTPUT_CSV
    s3UploadFile(session, source, target)

    print("Upload output masterplot_hv geojson")
    source = a.PATH_OUTPUT_FOLDER + a.FILENAME_PLOT_TODAY_OUTPUT_GEOJSON
    target = a.PATH_S3_OUTPUT_MASTERPLOT_HV + a.FILENAME_PLOT_TODAY_OUTPUT_GEOJSON
    s3UploadFile(session, source, target)


main()
