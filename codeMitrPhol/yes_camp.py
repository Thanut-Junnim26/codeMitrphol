import ee
import streamlit as st
import geemap.foliumap as geemap
from geemap import geojson_to_ee
import pandas as pd
import numpy as np
import itertools
# from streamlit_jupyter import StreamlitPatcher, tqdm


def line_chart():
    GHG = {}
    # for pix in range(0, 6):
    #     rand = np.random.random(size=(6, 1))
    #     test = rand.tolist()
    #     lst = []
    #     for i in test:
    #         for j in i:
    #             lst.append(j)
    #     GHG[f'amount/tCO2/ไร่_{pix}'] = lst
    lst = []
    for i in range(0, 6):
        x = np.random.uniform(0.02, 0.03)
        x = np.around(x, 6)
        lst.append(x)  # A single value

    GHG.update({'Greenhouse_Glass_%': [
               1, 0.7, 0.5, 0.3, 0.15, 0], 'amount/tCO2/ไร่': lst})

    GHG.update
    df = pd.DataFrame(GHG)
    # df = df.set_index('GHG_%')
    df_sum = df.sum() / 6
    df_list = list(map(int, df_sum.to_list()))
    dict_string = zip_pixel(df_list)
    dict_string = dict(itertools.islice(dict_string.items(), 6))
    st.dataframe(df)
    st.write(dict_string)
    # st.line_chart(df, y=["avg_PIXEL_แปลงที่_0", "avg_PIXEL_แปลงที่_1", "avg_PIXEL_แปลงที่_2",
    #               "avg_PIXEL_แปลงที่_3", "avg_PIXEL_แปลงที่_4", "avg_PIXEL_แปลงที่_5"], x="Greenhouse_Glass_%")
    st.line_chart(df, y="Greenhouse_Glass_%", x="amount/tCO2/ไร่")
    # chart_data = pd.DataFrame(
    #     {
    #         "col1": np.random.randn(20),
    #         "col2": np.random.randn(20),
    #         "col3": np.random.choice(["A", "B", "C"], 20),
    #     }
    # )

    # st.line_chart(chart_data, x="col1", y="col2", color="col3")


def zip_pixel(df_list, d={}):

    for i, j in enumerate(df_list):
        d[f'ผลรวมเฉลี่ยก๊าซเรือนกระจกของ_PIXEL_แปลงที่_{i}'] = j
    return d


def main():

    c = ee.FeatureCollection(
        'USDOS/LSIB_SIMPLE/2017').filter(ee.Filter.eq('country_na', 'Thailand'))
    S2 = ee.ImageCollection(
        'COPERNICUS/S2').filterDate('2020-04-01', '2023-04-01').filterBounds(c)
    Map = geemap.Map()
    # file = open('polygon_800.geojson', encoding="utf-8")
    ee_data = geojson_to_ee('yes_camp_MDC_2.geojson', encoding="utf-8")

    def maskcloud1(image):
        QA60 = image.select(['QA60'])
        return image.updateMask(QA60.lt(1))

    image = S2.median().select(['B2', 'B3', 'B4', 'B8'], [
        'blue', 'green', 'red', 'nir'])

    EVI = image.expression(
        "2.5 * ((nir - red) / (nir + 6 * red - 7.5 * blue + 1))",
        {
            'nir': image.select('nir').divide(10000),
            'red': image.select('red').divide(10000),
            'blue': image.select('blue').divide(10000),
        })

    legend_dict = {
        "percent/tCO2 <100%": "ff0000",
        "percent/tCO2 <89%": "ff500d",
        "percent/tCO2 <78%": "ff6e08",
        "percent/tCO2 <67%": "ff8b13",
        "percent/tCO2 <56%": "ffb613",
        "percent/tCO2 <45%": "ffd611",
        "percent/tCO2 <34%": "fff705",
        "percent/tCO2 <23%": "b5e22e",
        "percent/tCO2 <12%": "86e26f",
        "soil or water or else": "0502a3"
    }
    # encode_lengend_dict = legend_dict
    act_legend = {
        '0.5< not activities(may be not sugercane) <0.3': '86e26f',
        '0.2< sugarcane <0.1': '3ae237',
        '0.2< Sugarcane planting activities <0.19': 'b5e22e',
        '0.19< harvest <0.18': 'fff705',
        '0.18< plant new sugarcane <0.17': 'ffd611',
        '0.17< Shovel the soil <0.16': 'ffb613',
        '0.15< fertilizer <0.14': '#ff6e08',
        '0.13< Weed control and herbicides <0.12': 'ff500d',
        '0.12< Weed control <0.10': 'ff0000',
        'soil or water or else': '0502a3'
    }

    pal = [
        '040274', '040281', '0502a3', '0502b8', '0502ce', '0502e6',
        '0602ff', '235cb1', '307ef3', '269db1', '30c8e2', '32d3ef',
        '3be285', '3ff38f', '86e26f', '3ae237', 'b5e22e', 'd6e21f',
        'fff705', 'ffd611', 'ffb613', 'ff8b13', 'ff6e08', 'ff500d',
        'ff0000', 'de0101', 'c21301', 'a71001', '911003']
    params = {'min': -0.1, 'max': 1, 'palette': pal}

    st.sidebar.title("Refer🚀")

    st.sidebar.info(
        """
        - Paper reference: <My วิจัย>💯
        - Contact us : <CEO เจเจ>🔞
        - road to 100,000 Bath 🏅
        """
    )
    st.title("🌎Greenhouse Glass Track By รวมmitr🪐")
    st.markdown('''แผนที่ฉบับสามารถดูปริมาณcarbonได้และปริมาณก๊าซเรือนกระจกอื่นๆได้
                ตามรูปภาพถ่ายดาวเทียม ซึ่งจะอัพโหลดทุกๆ 5 วันสามารถดูใหม่ได้อีกครั้ง
                โปรด!!!!รอจนถึงวันนั้น🔨''')

    st.markdown(
        '''แผนที่ฉบับนี้จัดทำโดยกลุ่ม รวมmitr กลุ่มที่ร่วมเข้าแข่งขัน hackaton ห้ามเผยแพร่ก่อนได้รับอนุญาตโดยเด็ดขาด🪓''')
    regions = 'yes_camp_MDC_2.geojson'
    Map.centerObject(c, 6)
    Map.setOptions("SATELLITE")
    Map.addLayer(EVI.clip(c), params, 'EVI')
    Map.add_geojson(regions, layer_name='ASIA Regions')
    # Map.addLayer(ee_data, {}, "US States EE")

    Map.add_legend(title="carbon quantity",
                   legend_dict=legend_dict, position='bottomleft', draggable=False)

    style = {
        'border-radius': '20px',
        'padding': '20px',
        'font-size': '18px',
        'bottom': '20px',
        'right': '20px',
        'left': '20px'
    }

    # Map.to_streamlit()
    Map.add_legend(title="Farm activities",
                   legend_dict=act_legend, position='bottomright', draggable=False, style=style)
    Map.to_streamlit(height=800, width=800)


# 4/1AfJohXlNQ8jCV_qnivvNw1I_7YwVkf6kpjKjpcKBespHlaFH7P5fz4xIvXg
# ee.Authenticate()
ee.Initialize()
main()
# line_chart()
