# from datetime import date, timedelta, datetime
# import boto3
# import os
# import rasterio
# import numpy as np
# import json
# import argparse
# s3_client = boto3.client("s3")

# years = ["2022", "2023"]
# months = ["1","2","3","4","5","6","7","8","9","10","11","12"]


# def tile2Path(TILES):
#     if len(TILES) == 5:
#         pathTile = f"{TILES[:2]}/{TILES[2]}/{TILES[3:]}"
#         print(pathTile)
#     else:
#         print("error")
#     return pathTile

# def onListDateProd2Test(bucket='', prefix=''):

#     response = s3_client.list_objects_v2(
#         Bucket=bucket,
#         Prefix=prefix
#     )
#     my_list = []
#     res = response["Contents"]
#     while len(res) <= 1000:
#         count = 0
#         last_key = ''
#         for i in response["Contents"]:
#             if ((i["Key"].split("/")[9] == 'R10m') or (i["Key"].split("/")[9] == 'R20m')):
#                 if (int(i["Key"].split("/")[7]) < 25) and ((int(i["Key"].split("/")[5]) == 2022) and (int(i["Key"].split("/")[6]) == 1)):
#                     print(i["Key"], "Not append")
#                 else:
#                     print(i["Key"])
#                     my_list.append(i["Key"])
#             count += 1
#         print("#"*100, count)
#         if count == 1000: # if contents no have 1000 = error
#             last_key = i["Key"]
#         response = s3_client.list_objects_v2(
#             Bucket=bucket,
#             Prefix=prefix,
#             StartAfter=last_key
#         )
#         res = response["Contents"]
#         print("*"*100, "\n",sum, len(res))
#         if len(res) < 1000: # if list objects v2 < 1000 (max 1000) filter path file agian and quit while loop
#             for i in response["Contents"]:
#                 if ((i["Key"].split("/")[9] == 'R10m') or (i["Key"].split("/")[9] == 'R20m')):
#                     if (int(i["Key"].split("/")[7]) < 25) and ((int(i["Key"].split("/")[5]) == 2022) and (int(i["Key"].split("/")[6]) == 1)):
#                         print(i["Key"], "Not append")
#                     else:
#                         print(i["Key"])
#                         my_list.append(i["Key"])
#             break

#     prod_num = []
#     for j in my_list:
#         prod_num.append(j.split("/")[7]) # find date


#     num_list = list(set(prod_num)) # duplicate date
#     len_num = len(num_list)
#     num_int = []
#     k = 0
#     while k < len_num:
#         num_int.append(int(num_list[k]))
#         k += 1


#         # print(num_int)
#         # print()
#     return num_int

# def diffDateProd2Test(a, month, year, changePathTile):

#     try:# from 47QRU to 47/Q/RU

#         date_prod = set(onListDateProd2Test(a.BUCKET_PROD, prefix=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/"))# error somewhere Keyerror
#         date_test = set(onListDateProd2Test(a.BUCKET_TEST, prefix=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/"))# error somewhere Keyerror
#         print("let's get start brooo")
#         print("set of Date production --> {}". format(date_prod))
#         print("set of Date test --> {}".format(date_test) )

#         str_sort_date = date_prod - date_test

#         len_diff = len(str_sort_date)
#         # print(len_diff)
#         diff_date = []
#         count_of_list = 0
#         while count_of_list < len_diff:
#             sort_num_int = sorted(str_sort_date)
#             if count_of_list+1 == len_diff:
#                 for i in range(len(sort_num_int)):
#                     diff_date.append(str(sort_num_int[i]))
#             count_of_list += 1

#         return diff_date
#     except KeyError:
#         try:
#             return onListDateProd2Test(a.BUCKET_PROD, prefix=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/") # return ['2'] of date prod
#         except Exception as e: # this case is s3_prod not have file yet
#             print("Error pls wait until s3_prod the new month: ", e)

# def inputProcessOutput(input_file, output_file, offset):
#     with rasterio.open(input_file) as src:

#         # Read the raster data and convert to uint16
#         raster_data = src.read(1).astype(np.uint16)

#         # Apply offset
#         raster_data = (raster_data + offset)

#         # Output file at current path
#         with rasterio.open(output_file, 'w', **src.profile) as dst:
#             dst.write(raster_data, 1)

#     raster_data = None
#     src = None

# def createFolder(sPathParentFolder): # open path folder in computer cloud with linux
#     try:
#         os.makedirs(sPathParentFolder)
#         print("Directory " , sPathParentFolder ,  " Created ")
#     except FileExistsError:
#         print("Directory " , sPathParentFolder ,  " already exists")


# def onGetArgs():
#     class AttributeDict(dict):
#         __getattr__ = dict.__getitem__
#         __setattr__ = dict.__setitem__
#         __delattr__ = dict.__delitem__

#     parser = argparse.ArgumentParser( description = 'farmfocus-masterplot-hv2-prod' )
#     parser.add_argument('-tile',  '--TILES',  help='YYYY', required=True)
#     args = vars(parser.parse_args())
#     a = AttributeDict(args)


#     a['BUCKET_PROD']   = "mitrphol-satellite-prod-553463144000"
#     a['BUCKET_TEST']  = "mitrphol-farmfocus-test-553463144000"
#     a['KEY_PATH_TARGET'] = "satellite/tiles"

#     # a['PATH_INPUT_FOLDER']  = "/root/FarmFocus/Project_FarmFocus_ProcessRawRaster/input"
#     a['PATH_INPUT_FOLDER']  = "/opt/ml/processing/adjust2band/input"
#     # /opt/ml/processing/input/raster/
#     a['PATH_OUTPUT_FOLDER'] = "/opt/ml/processing/adjust2band/output"
#     # /root/FarmFocus/Project_FarmFocus_ProcessRawRaster/input
#     # a['PATH_YEAR_MONTH'] = f'{a.YEAR}/{a.MONTH}'
#     # a['TILES'] = a.TILE # add all tiles

#     createFolder(a['PATH_INPUT_FOLDER'])
#     createFolder(a['PATH_OUTPUT_FOLDER'])

#     print(json.dumps(a, sort_keys=False, indent=4))
#     return a

# def main(a):

#     for year in years:
#             for month in months:
#                 try:

#                     print(f"#################################----{year}----{month}----#################################")
#                     changePathTile = tile2Path(a.TILES)
#                     offset = -1000
#                     diff_date = diffDateProd2Test(a, month=month, year=year, changePathTile=changePathTile) # error this here or here on your def # return list()
#                     print("Different date--> ", diff_date)
#                     if diff_date != []:

#                         for DATE in diff_date:

#                             response = s3_client.list_objects_v2(
#                                 Bucket = a.BUCKET_PROD,
#                                 Prefix = f'{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/{DATE}' # cant list if it have [] = error
#                                 )


#                             for i in response['Contents']: # error
#                                 if (i["Key"].split("/")[9] == "R10m" or i["Key"].split("/")[9] == "R20m"):
#                                     obj = i["Key"]
#                                     ver = obj.split("/")[8]
#                                     RXm = obj.split("/")[9]
#                                     band = obj.split("/")[10]

#                                     input_file = f'{a.PATH_INPUT_FOLDER}/{band}'
#                                     output_file = f'{a.PATH_OUTPUT_FOLDER}/offset_{band}'
#                                     source_file = f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/{DATE}/{ver}/{RXm}/{band}"

#                                     print("#"*100)
#                                     print("Downloading --> ", obj)
#                                     s3_client.download_file(a.BUCKET_PROD,
#                                                             source_file,
#                                                             input_file
#                                                             )
#                                     print("Processing --> ", obj)
#                                     inputProcessOutput(input_file, output_file, offset)

#                                     print("Uploading --> ", obj)
#                                     s3_client.upload_file(Filename = output_file,
#                                                         Bucket = a.BUCKET_TEST,
#                                                         Key = f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/{int(DATE)}/{ver}/{RXm}/{band}"
#                                                         )

#                                     os.remove(input_file)
#                                     os.remove(output_file)
#                                     print("-------------------Delete local process file-------------------")

#                     else:
#                         print("See you tomorrow")

#                 except Exception as e: # error with [] and create directory file
#                     print("last error  --->", e)


# ###############################################################################
# if __name__ == "__main__":
#     a = onGetArgs()

#     main(a)

from datetime import date, timedelta, datetime
import boto3
import os
import rasterio
import numpy as np
import json
import argparse
s3_client = boto3.client("s3")

years = ["2022", "2023"]
months = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]


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
            if ((i["Key"].split("/")[9] == 'R10m') or (i["Key"].split("/")[9] == 'R20m')):
                if (int(i["Key"].split("/")[7]) < 25) and ((int(i["Key"].split("/")[5]) == 2022) and (int(i["Key"].split("/")[6]) == 1)):
                    print(i["Key"], "Not append")
                else:
                    my_list.append(i["Key"])
            count += 1
        print("#"*100, count)
        if count == 1000:  # if contents no have 1000 = error
            last_key = i["Key"]
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            StartAfter=last_key
        )
        res = response["Contents"]
        print("*"*100, "\n", sum, len(res))
        if len(res) < 1000:  # if list objects v2 < 1000 (max 1000) filter path file agian and quit while loop
            for i in response["Contents"]:
                if ((i["Key"].split("/")[9] == 'R10m') or (i["Key"].split("/")[9] == 'R20m')):
                    if (int(i["Key"].split("/")[7]) < 25) and ((int(i["Key"].split("/")[5]) == 2022) and (int(i["Key"].split("/")[6]) == 1)):
                        print(i["Key"], "Not append")
                    else:

                        my_list.append(i["Key"])
            break

    prod_num = []
    for j in my_list:
        prod_num.append(j.split("/")[7])  # find date

    num_list = list(set(prod_num))  # duplicate date
    len_num = len(num_list)
    num_int = []
    k = 0
    while k < len_num:
        num_int.append(int(num_list[k])) # list of string to list of int 
        k += 1

        # print(num_int)
    return num_int


def diffDateProd2Test(a, month, year, changePathTile):

    try:

        date_prod = set(onListDateProd2Test(
            a.BUCKET_PROD, prefix=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/"))  # error somewhere Keyerror
        date_test = set(onListDateProd2Test(
            a.BUCKET_TEST, prefix=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/"))  # error somewhere Keyerror
        print("let's get start brooo")
        print("set of Date production --> {}". format(date_prod))
        print("set of Date test --> {}".format(date_test))

        str_sort_date = date_prod - date_test

        len_diff = len(str_sort_date)
        # print(len_diff)
        diff_date = []
        count_of_list = 0
        while count_of_list < len_diff:
            sort_num_int = sorted(str_sort_date)
            if count_of_list+1 == len_diff:
                for i in range(len(sort_num_int)):
                    diff_date.append(str(sort_num_int[i]))
            count_of_list += 1

        return diff_date
    except KeyError:
        try:
            # return ['2'] of date prod
            print("we were in error")
            # return date of prod ex. ['2', '4'....]
            return onListDateProd2Test(a.BUCKET_PROD, prefix=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/")
        except Exception as e:  # this case is s3_prod not have file yet
            print("Error pls wait until s3_prod the new month: ", e)


def inputProcessOutput(input_file, output_file, offset):
    with rasterio.open(input_file) as src:

        # Read the raster data and convert to uint16
        raster_data = src.read(1).astype(np.uint16)

        # Apply offset
        raster_data = (raster_data + offset)

        # Output file at current path
        with rasterio.open(output_file, 'w', **src.profile) as dst:
            dst.write(raster_data, 1)

    raster_data = None
    src = None


def createFolder(sPathParentFolder):  # open path folder in computer cloud with linux
    try:
        os.makedirs(sPathParentFolder)
        print("Directory ", sPathParentFolder,  " Created ")
    except FileExistsError:
        print("Directory ", sPathParentFolder,  " already exists")


def onGetArgs():
    class AttributeDict(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    parser = argparse.ArgumentParser(
        description='farmfocus-masterplot-hv2-prod')
    parser.add_argument('-tile',  '--TILES',  help='YYYY', required=True)
    args = vars(parser.parse_args())
    a = AttributeDict(args)

    a['BUCKET_PROD'] = "mitrphol-satellite-prod-553463144000"
    a['BUCKET_TEST'] = "mitrphol-farmfocus-test-553463144000"
    a['KEY_PATH_TARGET'] = "satellite/tiles"

    a['PATH_INPUT_FOLDER'] = "/opt/ml/processing/adjust2band/input"
    a['PATH_OUTPUT_FOLDER'] = "/opt/ml/processing/adjust2band/output"
    # a['TILES'] = ["47PNQ", "47PNR", "47PNS", "47PNT", "47PPS", "47PPT", "47PQS", "47PQT", "47PRS", "47PRT", "47QMU", "47QMV", "47QNU", "47QNV", "47QPU", "47QPV", "47QQU", "47QQV", "47QRU", "47QRV", "48PTB", "48PTC", "48PUB",
    #               "48PUC", "48PVB", "48PVC", "48PWB", "48PWC", "48QTD", "48QTE", "48QUD", "48QUE", "48QVD", "48QVE"]
    # a['PATH_YEAR_MONTH'] = f'{a.YEAR}/{a.MONTH}'
    # a['TILES'] = a.TILE # add all tiles

    createFolder(a['PATH_INPUT_FOLDER'])
    createFolder(a['PATH_OUTPUT_FOLDER'])

    print(json.dumps(a, sort_keys=False, indent=4))
    return a


def main(a):

    # for tile in a.TILES:
    for year in years:
        for month in months:
            try:

                print(
                    f"#################################----{year}----{month}----#################################")  # a.YEAR and a.MONTH
                changePathTile = tile2Path(a.TILES)  # from 47QRU to 47/Q/RU
                offset = -1000
                # error this here or here on your def # return list()
                diff_date = diffDateProd2Test(
                    a, month=month, year=year, changePathTile=changePathTile)
                print("Different date--> ", diff_date)
                if diff_date != []:

                    for DATE in diff_date:

                        response = s3_client.list_objects_v2(
                            Bucket=a.BUCKET_PROD,
                            # cant list if it have [] = error
                            Prefix=f'{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/{DATE}'
                        )

                        for i in response['Contents']:  # error
                            if (i["Key"].split("/")[9] == "R10m" or i["Key"].split("/")[9] == "R20m"):
                                obj = i["Key"]
                                ver = obj.split("/")[8]
                                RXm = obj.split("/")[9]
                                band = obj.split("/")[10]

                                input_file = f'{a.PATH_INPUT_FOLDER}/{band}'
                                output_file = f'{a.PATH_OUTPUT_FOLDER}/offset_{band}'
                                source_file = f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/{DATE}/{ver}/{RXm}/{band}"

                                print("#"*100)
                                print("Downloading --> ", obj)
                                s3_client.download_file(a.BUCKET_PROD,
                                                        source_file,
                                                        input_file
                                                        )
                                print("Processing --> ", obj)
                                inputProcessOutput(
                                    input_file, output_file, offset)

                                print("Uploading --> ", obj)
                                s3_client.upload_file(Filename=output_file,
                                                      Bucket=a.BUCKET_TEST,
                                                      Key=f"{a.KEY_PATH_TARGET}/{changePathTile}/{year}/{month}/{int(DATE)}/{ver}/{RXm}/{band}"
                                                      )

                                os.remove(input_file)
                                os.remove(output_file)
                                print(
                                    "-------------------Delete local process file-------------------")

                else:
                    print("See you tomorrow")

            except Exception as e:  # error with [] and create directory file
                print("last error  --->", e)


###############################################################################
if __name__ == "__main__":
    a = onGetArgs()

    main(a)t