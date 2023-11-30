###############################################################
import boto3
from botocore.exceptions import ClientError

import os, json, argparse
import socket
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely import wkt

import redshift_connector

import numpy as np
from re import search

import time
###############################################################
'''
For standardize canetype by regex
'''
def replaceCaneType(x):
    if x == None or x == np.nan or x == "None":
        return None
    else:
        if search("ตอ", x):
            if search("รื้อตอ", x):
                return "NP"
            else:
                return "RT"
        else:
            return "NP"

'''
Get boto session base on instance.
'''
def onGetBotoSession():
    if str(socket.gethostname()) == "NATAKRANP-MBP.local":
        # This for local development testing.
        df = pd.read_csv('/Users/natakranp/credential-mitrphol-farmfocus-user-natakranp-prod.csv')
        credential = df.iloc[0]

        session = boto3.Session(
            aws_access_key_id     = credential['AccesskeyID'],
            aws_secret_access_key = credential['Secretaccesskey'],
        )
        print("Local")
    else:
        # This will invoke from any aws services.
        session = boto3.session.Session()
        print("AWS cloud")
    return session

'''
Create Folder 
'''
def createFolder(sPathParentFolder):
    try:
        os.makedirs(sPathParentFolder)
        print("Directory " , sPathParentFolder ,  " Created ") 
    except FileExistsError:
        print("Directory " , sPathParentFolder ,  " already exists")

###############################################################
'''
Get secret keys from AWS - Secret Manager
 - Add more last code with json.dumps
'''
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
'''
Get arguments and parse to dict
'''
def onGetArgs():
    class AttributeDict(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    parser = argparse.ArgumentParser( description = 'farmfocus-masterplot-prod' )
    parser.add_argument('-CROPYEAR',  '--CROPYEAR',   help = 'Like 6566, 6667',       required = True)
    parser.add_argument('-YEAR',      '--YEAR',       help = 'Year of datetime now',  required = True)
    parser.add_argument('-MONTH',     '--MONTH',      help = 'Month of datetime now', required = True)
    parser.add_argument('-DAY',       '--DAY',        help = 'Day of datetime now',   required = True)
    args = vars(parser.parse_args())
    a = AttributeDict(args)
    
    a['MONTH'] = str(a['MONTH']).zfill(2)
    a['DAY']   = str(a['DAY']).zfill(2)

    a['BUCKET_TARGET']   = "mitrphol-farmfocus-prod-553463144000"
    a['KEY_PATH_TARGET'] = "master/plot"
    a['HOSTNAME']       = str(socket.gethostname())
    a['BASE_DIR']       = str(Path(__file__).resolve().parent)
    a['PROCESS_PATH']   = f"{a.BASE_DIR}/process"

    a['FILENAME_OUTPUT_GEOJSON'] = f"{a.YEAR}{a.MONTH}{a.DAY}_PLOT.geojson"
    a['FILENAME_OUTPUT_CSV']     = f"{a.YEAR}{a.MONTH}{a.DAY}_PLOT-dat.csv"
    a['FILENAME_OUTPUT_LOG']     = f"{a.YEAR}{a.MONTH}{a.DAY}_PLOT-log.txt"
    print(json.dumps(a, sort_keys=False, indent=4))
    return a

###############################################################
def onUploadFileToS3(a, s3_client, a_FILENAME):
    s3_client.upload_file(
        Filename = f"{a.PROCESS_PATH}/{a_FILENAME}",
        Bucket   = f"{a.BUCKET_TARGET}", 
        Key      = f"{a.KEY_PATH_TARGET}/{a_FILENAME}"
    )
    print("Uploaded:", f"{a.PROCESS_PATH}/{a_FILENAME}", "TO", f"{a.BUCKET_TARGET}/{a.KEY_PATH_TARGET}/{a_FILENAME}")
    
###############################################################

'''
Main Function
'''
def main():
    
    a = onGetArgs()
    createFolder(a.PROCESS_PATH)

    session = onGetBotoSession()
    secret = get_secret(session)

    conn = redshift_connector.connect(
        host=secret['host'],
        user=secret['username'],
        password=secret['password'],
        database='mitrphol_prod',
    )
    cursor: redshift_connector.Cursor = conn.cursor()
        
    str_query = f'select * from "canemishq_mp_canemis_prd"."v_plot" where production_year = {a.CROPYEAR};'
    print("SQL Query String: ",str_query)
    cursor.execute(str_query)
    df_result: pd.DataFrame = cursor.fetch_dataframe()

    df = df_result.astype(str)
    #######################################################
    def ParseWKT(x):
        try:
            return wkt.loads(x)
        except:
            return None

    df['geometry'] = df['coordinates'].apply(lambda x: ParseWKT(x))
    df['plot_type_ff'] = df['plot_type'].apply(replaceCaneType)

    fields = [
        'production_year',
        'company_code',
        'quota',
        'plot_code',
        'plot_distance',
        'area_size',
        'plot_gis_status',
        'plant_date',
        'plot_type',
        'plot_type_ff',
        'soil_name',
        'watering_type',
        'water_source_name',
        'zone_id',
        'sub_zone_id',
        "subspecies_name",
        'plot_id',
        'geometry',
    ]

    df = df[fields]
    df = gpd.GeoDataFrame(df, geometry='geometry')
    df.to_file(f"{a.PROCESS_PATH}/{a.FILENAME_OUTPUT_GEOJSON}", driver='GeoJSON',encoding='utf-8')
    
    df = df.drop(columns=['geometry'])
    df.to_csv(f"{a.PROCESS_PATH}/{a.FILENAME_OUTPUT_CSV}",encoding='utf-8')

    f = open(f"{a.PROCESS_PATH}/{a.FILENAME_OUTPUT_LOG}", "w")
    f.write(str(a))
    f.write(str( ("\n\n") + ("#" * 100) + ("\n\n")))
    f.write(str(f"SQL QUERY STRING : {str_query}"))
    f.write(str( ("\n\n") + ("#" * 100) + ("\n\n")))
    f.write(str(f"Total Len : {len(df)}"))
    f.write(str( ("\n\n") + ("#" * 100) + ("\n\n")))
    f.write(str(df.groupby(['company_code'])['company_code'].count()))
    f.write(str( ("\n\n") + ("#" * 100) + ("\n\n")))
    f.write(str(df.groupby(['plot_type'])['plot_type'].count()))
    f.close()

    print("Dataframe total length: ", len(df))

    s3_client = session.client('s3')

    onUploadFileToS3(a, s3_client, a.FILENAME_OUTPUT_GEOJSON)
    onUploadFileToS3(a, s3_client, a.FILENAME_OUTPUT_CSV)
    onUploadFileToS3(a, s3_client, a.FILENAME_OUTPUT_LOG)

    print("Finish...")

try:
    main()
except Exception as ex:
    try:
        print("Error mes:",ex)
        time.sleep(120)
        main()
    except Exception as ex:
        try:
            print("Error mes:",ex)
            time.sleep(240)
            main()
        except Exception as ex:
            print("Error mes:",ex)
            raise Exception("Error")
