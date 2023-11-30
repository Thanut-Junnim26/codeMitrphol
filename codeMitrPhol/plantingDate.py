import os
import numpy as np
from re import search

# from geopandas import datasets, GeoDataFrame, read_file
import pandas as pd
import geopandas as gpd

import datetime
import calendar

import warnings
warnings.filterwarnings('ignore')


def replace_negatives_with_zeros(x):
    if x != None:
        return float(x) if float(x) >= 0 else None


def per_cloud(count, mock):
    if count == '0' or count == "None" or count == None or mock == '0' or mock == "None" or mock == None:
        result = None
    else:
        result = 100-((float(count)/float(mock))*100)
    return result


def PDST(mill):
    time_1 = datetime.datetime.now()

    rfile = '/crophealth/temp/NewPlantdateFromTimeseries/'+mill+'_NDVITS_PLOT_PN.geojson'

    gdf = gpd.read_file(rfile)
    gdf['index'] = gdf.index
    gdf['plot_id'] = gdf['index']

    cols = ['production_year', 'company_code', 'quota', 'plot_code',
            'plot_distance', 'area_size', 'plot_gis_status', 'plant_date',
            'plot_type', 'plot_type_ff', 'soil_name', 'watering_type',
            'water_source_name', 'zone_id', 'sub_zone_id', 'subspecies_name',
            'plot_id', 'plot_id_lastyear', 'plant_date_ff', 'soil_type1',
            'soil_type2', 'ch_plant_date_ff']

    meancols = ['plot_id', 'geo_check', 'index']
    for item in gdf.columns:
        datesplit = item.split('_')
        if len(datesplit) == 4 and datesplit[-1] == 'mean':
            count = item.replace('mean', 'count')
            mockcount = item.replace('mean', 'mockcount')
            percloud = item.replace('mean', 'percloud')
            ndvi = item

            meancols.append(ndvi)
            cols.append(ndvi)
            cols.append(percloud)

            gdf['tcount'] = gdf[count].astype('str')
            gdf['tmockcount'] = gdf[mockcount].astype('str')
            gdf[percloud] = gdf.apply(
                lambda x: per_cloud(x.tcount, x.tmockcount), axis=1)
            gdf.loc[gdf[percloud] > 30, ndvi] = None

    namefromlastcomposite = meancols[-1].split('_m')[0].replace("_", "")

    startcol = 3

    countmcol = len(meancols) - startcol

    data1 = gdf[gdf['geo_check'] == 'EXIST'][meancols]

    data = data1.iloc[:, 2:].applymap(replace_negatives_with_zeros)

    data['Min'] = data[data.columns[startcol:]].apply(
        pd.to_numeric, errors='coerce').min(axis=1)
    data['Min'].astype(float)

    datamain = gdf[['plot_id', 'geo_check', 'index']]
    data3 = datamain.merge(data, left_on='index', right_on='index')

    data2 = data3

    data2['namec'] = None
    data2['bf'] = None
    data2['af'] = None
    data2['newdate'] = None
    data2['Min'].astype(float)
    for ii in range(len(data2)):
        for i in range(countmcol):
            j = i+startcol
            if data2.loc[data2.index[ii]][j] != None:
                if float(data2['Min'][ii]) == float(data2.loc[data2.index[ii]][j]):
                    data2['namec'][ii] = j
                    if data2[data2.columns[j+1]][ii] != None and data2[data2.columns[j-1]][ii] != None:
                        data2['af'][ii] = float(data2[data2.columns[j+1]][ii])
                        if j == startcol:
                            data2['bf'][ii] = None
                            data2['newdate'][ii] = -999
                        else:
                            data2['bf'][ii] = float(
                                data2[data2.columns[j-1]][ii])
                            if data2['Min'][ii] <= data2['bf'][ii] and data2['Min'][ii] < data2['af'][ii]:
                                data2['newdate'][ii] = data2.columns[j][:10].replace(
                                    "_", "-")
                            else:
                                data2['newdate'][ii] = -999
                    else:
                        data2['newdate'][ii] = -999

    datajoin = data2[['index', 'newdate']]
    dff = gdf.merge(datajoin, left_on='index', right_on='index')

    NONEXIST = gdf[gdf['geo_check'] == 'NONEXIST']
    NONEXIST['newdate'] = None

    df = dff.append(NONEXIST)

#     df['New_Plantdate'] = None

    df['newdate'] = df['newdate'].astype('str')
    df.loc[(df['plot_type_ff'] == 'NP') & (df['newdate'].str.len()
                                           == 10), 'ch_plant_date_ff'] = df['newdate']
    df.loc[df['plot_type_ff'] == 'RT',
           'ch_plant_date_ff'] = df['plant_date_ff']

    cols.append('geometry')
    dfoutput = df[cols]

    path = '/crophealth/temp/NewPlantdateFromTimeseries'

    if not os.path.exists(path):
        os.mkdir(path)

    outp = path+'/'+mill+'_'+namefromlastcomposite+'_PLOT_PN_VersionUpgrade.geojson'
    dfoutput.to_crs(epsg=4326).to_file(
        outp, driver='GeoJSON', encoding='utf-8')

    time_2 = datetime.datetime.now()
    diftime = time_2-time_1

    print(mill, len(gdf)-len(df), len(gdf), len(df), diftime)


#####################################################################################################################################


def main():
    PROD_MODE = True
    a = onGetArgs()
    createFolder(a.PATH_INPUT_FOLDER)
    createFolder(a.PATH_OUTPUT_FOLDER)

    session = onGetBotoSession()
    # secret = get_secret(session)

    if PROD_MODE:
        print("Download NDVI15 Timeserise")
        source = a.PATH_S3_NDVI15_IMESERISE + a.FILENAME_PLOT_TIMESERISE
        print(source)
        target = a.PATH_INPUT_FOLDER + a.FILENAME_PLOT_TIMESERISE
        print("target_path:", target)
        s3DownloadFile(session, source, target)

    # --------------------Data Loading-------------------------------#
    gdf = gpd.read_file(a.PATH_INPUT_FOLDER + a.FILENAME_PLOT_TIMESERISE)

    print("finish")

    gdf['index'] = gdf.index
    gcol = gdf.columns

    meancol = ['plot_id', 'geo_check', 'index']
    for item in sorted(gcol):
        datesplit = item.split('_')
        if len(datesplit) == 4 and datesplit[3] == 'mean':
            #         print(item)
            meancol.append(item)

    namefromlastcomposite = meancol[-1].split('_m')[0].replace("_", "")
    startcol = 3
    countmcol = len(meancol) - startcol
    print("countmcol:", countmcol)
    print("meancol:", meancol)

    data = gdf[gdf['geo_check'] == 'EXIST'][meancol]
    data['Min'] = data[data.columns[startcol:]].apply(
        pd.to_numeric, errors='coerce').min(axis=1)
    data['Min'].astype(float)
    data2 = data

    print("data:", data)

    data2['namec'] = None
    data2['bf'] = None
    data2['af'] = None
    data2['daten'] = None
    data2['Min'].astype(float)
    for ii in range(len(data2)):
        for i in range(countmcol):
            j = i+startcol
    #         print(ii,j)
            if data2.loc[data2.index[ii]][j] != None:
                if float(data2['Min'][ii]) == float(data2.loc[data2.index[ii]][j]):
                    data2['namec'][ii] = j
                    if data[data.columns[j+1]][ii] != None and data[data.columns[j-1]][ii] != None:
                        data2['af'][ii] = float(data[data.columns[j+1]][ii])
                        if j == startcol:
                            data2['bf'][ii] = None
                            data2['daten'][ii] = -999
                        else:
                            data2['bf'][ii] = float(
                                data[data.columns[j-1]][ii])
                            if data2['Min'][ii] <= data2['bf'][ii] and data2['Min'][ii] < data2['af'][ii]:
                                data2['daten'][ii] = data2.columns[j][:10].replace(
                                    "_", "-")
                            else:
                                data2['daten'][ii] = -999
                    else:
                        data2['daten'][ii] = -999

    datajoin = data2[['index', 'daten']]
    dff = gdf.merge(datajoin, left_on='index', right_on='index')

    NONEXIST = gdf[gdf['geo_check'] == 'NONEXIST']
    NONEXIST['daten'] = None

    df = dff.append(NONEXIST)

    df['daten'] = df['daten'].astype('str')
    df.loc[(df['plot_type_ff'] == 'NP') & (
        df['daten'].str.len() == 10), 'ch_plant_date_ff'] = df['daten']
    df.loc[(df['plot_type_ff'] == 'NP') & (df['daten'].str.len() == 4),
           'ch_plant_date_ff'] = df['plant_date']
    df.loc[df['plot_type_ff'] == 'RT',
           'ch_plant_date_ff'] = df['plant_date_ff']

    cols = ['production_year', 'company_code', 'quota', 'plot_code',
            'plot_distance', 'area_size', 'plot_gis_status', 'plant_date',
            'plot_type', 'plot_type_ff', 'soil_name', 'watering_type',
            'water_source_name', 'zone_id', 'sub_zone_id', 'subspecies_name',
            'plot_id', 'plot_id_lastyear', 'plant_date_ff', 'soil_type1',
            'soil_type2', 'ch_plant_date_ff', 'geometry']

    dfoupput = df[cols]

    print(f'{a.LOCATION}', len(gdf)-len(df), len(gdf), len(df))
