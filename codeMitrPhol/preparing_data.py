lst = []
for plant in plants:
    gdf = gpd.read_file(
        f'./new_{plant}_20220901_20231031_TIMESERIES_NDVI15.geojson')

    print('#'*100)
    print(gdf.tail())
    # clssification between datetimeseires and value
    var_col = gdf.columns.to_list()[:23]
    value_col = gdf.columns.to_list()[23:-1]
    # print(value_col)
    # mean_col = mean_ndvi
    mean_col = gdf.filter(regex='mean')
    count = mean_col.columns.shape[0]

    print(count, '\n', mean_col)

    # melt dataframe
    melt_col = mean_col.melt(value_name='ndvi', var_name='datetime')
    melt_col['datetime'].str.split('_')
    lsts = melt_col['datetime'].str.split('_').to_list()
    my_date = list()
    for lst_date in lsts:
        my_date.append(lst_date[:3])
    my_date_series = pd.Series(my_date, name="datetimes_series")
    date = my_date_series.str.join('-').to_frame()
    date_ndvi = date.join(melt_col['ndvi'])
    print("#"*100)
    print(date_ndvi)

    # append cuz same row
    df = gdf.loc[:, var_col]
    df_copy = df.copy()
    new_df = df.append([df_copy]*(count-1)).reset_index()
    join_df = new_df.join(date_ndvi).drop(['index'], axis=1)
    # append geo dataframe
    geo = gdf.iloc[:, -1]
    geo_copy = geo.append(
        [geo]*(count-1)).reset_index().drop(['index'], axis=1)
    join_df = join_df.join(geo_copy)
    df = join_df[join_df['ndvi'].notnull()].reset_index().drop([
        'index'], axis=1)
    print(df.columns)
    lst.append(df)
    print(len(lst))

    # save to csv
    # df.to_csv(f'/root/farmFocus/timseries_ndvi15/csv_file/{plant}.csv')
    print('save to csv success')
