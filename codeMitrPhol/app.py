import streamlit as st

import pandas as pd
import numpy as np

import leafmap.foliumap as leafmap

# COL = ["area_target", "area_size", "Ripper", "adjust_area", "plant_sugarcane"]
# RANDOM = np.random.randn(10, 5)
# INDEX = ["gudJok", "cokeSaAud", "banPhech", "pooPhech", "sriBunRung1",
#          "nonRang", "conSan", "somSaAud", "pooPaMann", "sriBunRung2"]


# # def get_data_in_excel():
# #     st.button("carbon credit")


# def main():
#     with st.sidebar:
#         lst = []
#         formbtn = st.sidebar.button("ลงทะเบียน")

#         if "formbtn_state" not in st.session_state:
#             st.session_state.formbtn_state = False

#         if formbtn or st.session_state.formbtn_state:
#             st.session_state.formbtn_state = True
#             temp = []
#             # st.sidebar.subheader("ข้อมูลส่วนตัว")
#             # name = st.text_input("Name")
#             with st.form(key='user_info'):
#                 st.write('ข้อมูลส่วนตัว')

#                 # for i in range(5):
#                 name = st.text_input(label="ชื่อ 📛")

#                 #     temp.append(name)
#                 # st.write(temp)
#                 age = st.number_input(label="อายุ 🔢", value=0)
#                 email = st.text_input(label="อีเมล 📧")
#                 phone = st.text_input(label="เบอร์โทร 📱")
#                 gender = st.radio(
#                     "เพศ 🧑", ("ชาย", "หญิง", "บุคคลที่3"))

#                 submit_form = st.form_submit_button(
#                     label="ลงทะเบียน", help="กดเพื่อลงทะเบียนเป็นชาวไร่!")

#                 # Checking if all the fields are non empty
#                 # if st.form_submit_button(label="Break"):
#                 #     break

#                 if submit_form:
#                     st.write(submit_form)

#                     if name and age and email and phone and gender:
#                         # add_user_info(id, name, age, email, phone, gender)
#                         st.success(
#                             f"นี่คือ ID card ของคุณ  \nID:  \n Name: {name}  \n Age: {age}  \n Email: {email}  \n Phone: {phone}  \n Gender: {gender}"
#                         )
#                         # get_data_in_excel()
#                     else:
#                         st.warning("โปรดกรอกให้ครบถ้วน")

#     df = pd.DataFrame(RANDOM, columns=(
#         f"{i}" for i in COL), index=INDEX)

#     st.dataframe(df)

#     st.subheader("\nThis's bar chart\n")
#     st.bar_chart(df)


# main()

# ------------------------------------------------------------------------------------------------------------------------------------------------------

# import streamlit as st

# st.title('Counter Example')

# # Streamlit runs from top to bottom on every iteraction so
# # we check if `count` has already been initialized in st.session_state.

# # If no, then initialize count to 0
# # If count is already initialized, don't do anything
# st.session_state['key+'] = []
# st.session_state['key-'] = []
# if 'count' not in st.session_state:
#     st.session_state.count = 0

#     st.write(st.session_state)

# # select box
# TYPE = {'NAME': ["THANUT", "SRICHAI", "NUMCHOK"],
#         'SURNAME': ['JUNNIM', 'FOONSEGA', 'NUMCHAI']}
# box_name = st.selectbox("This fucking select dog", TYPE['NAME'])

# if box_name in TYPE['NAME']:
#     INDEX_NAME = TYPE['NAME'].index(box_name)
#     surname = TYPE['SURNAME'][INDEX_NAME]
#     st.write(f'{box_name} {surname}')

# # Create a button which will increment the counter
# increment = st.button('Increment')
# if increment:
#     st.session_state.count += 1
#     st.session_state['key+'].append(st.session_state.count)

# # A button to decrement the counter
# decrement = st.button('Decrement')
# if decrement:
#     st.session_state.count -= 1
#     st.session_state['key-'].append(st.session_state.count)

# st.write('Count = ', st.session_state.count)
# st.write('+ =', st.session_state['key+'])
# st.write('- =', st.session_state['key-'])

# --------------------------------------------------------------------------------------------------------------------
# import streamlit as st
# import pandas as pd
# import numpy as np
# import pydeck as pdk

# chart_data = pd.DataFrame(
#     np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
#     columns=['lat', 'lon'])

# st.pydeck_chart(pdk.Deck(
#     map_style=None,
#     initial_view_state=pdk.ViewState(
#         latitude=37.76,
#         longitude=-122.4,
#         zoom=11,
#         pitch=50,
#     ),
#     layers=[
#         pdk.Layer(
#             'HexagonLayer',
#             data=chart_data,
#             get_position='[lon, lat]',
#             radius=200,
#             elevation_scale=4,
#             elevation_range=[0, 1000],
#             pickable=True,
#             extruded=True,
#         ),
#         pdk.Layer(
#             'ScatterplotLayer',
#             data=chart_data,
#             get_position='[lon, lat]',
#             get_color='[200, 30, 0, 160]',
#             get_radius=200,
#         ),
#     ],
# ))
# -----------------------------------------------------------------------------------------
# import streamlit as st
# import leafmap.foliumap as leafmap

st.set_page_config(layout="wide")

st.sidebar.info(
    """
    - Web App URL: <https://streamlit.geemap.org>
    - GitHub repository: <https://github.com/giswqs/streamlit-geospatial>
    """
)

st.sidebar.title("Contact")
# st.sidebar.info(
#     """
#     Qiusheng Wu: <https://wetlands.io>
#     [GitHub](https://github.com/giswqs) | [Twitter](https://twitter.com/giswqs) | [YouTube](https://www.youtube.com/c/QiushengWu) | [LinkedIn](https://www.linkedin.com/in/qiushengwu)
#     """
# )

# st.title("โรงงานมิตรผลทั้งหมด")

# with st.expander("See source code"):
#     with st.echo():

m = leafmap.Map(center=[40, -100], zoom=4)
cities = 'https://raw.githubusercontent.com/giswqs/leafmap/master/examples/data/us_cities.csv'
regions = 'https://raw.githubusercontent.com/giswqs/leafmap/master/examples/data/us_regions.geojson'

m.add_geojson(regions, layer_name='US Regions')
m.add_points_from_xy(
    cities,
    x="longitude",
    y="latitude",
    color_column='region',
    icon_names=['gear', 'map', 'leaf', 'globe'],
    spin=True,
    add_legend=True,
)

m.to_streamlit(height=700)

# ---------------------------------------------------------------------------------------------------

# st.set_page_config(layout="wide")

st.sidebar.info(
    """
    - Web App URL: <https://streamlit.geemap.org>
    - GitHub repository: <https://github.com/giswqs/streamlit-geospatial>
    """
)

st.sidebar.title("Contact")
st.sidebar.info(
    """
    Qiusheng Wu: <https://wetlands.io>
    [GitHub](https://github.com/giswqs) | [Twitter](https://twitter.com/giswqs) | [YouTube](https://www.youtube.com/c/QiushengWu) | [LinkedIn](https://www.linkedin.com/in/qiushengwu)
    """
)

# Customize page title
st.title("บริษัท น้ำตาลมิตรผล จำกัด")

st.markdown(
    """
    This multipage app template demonstrates various interactive web apps created using [streamlit](https://streamlit.io) and [leafmap](https://leafmap.org). It is an open-source project and you are very welcome to contribute to the [GitHub repository](https://github.com/giswqs/streamlit-multipage-template).
    """
)

st.header("สาระสำคัญ")

markdown = """
1. platform นีี้จะประกอบไปด้วยการกรอก carbon credit เพื่อลดแรงการโกงของชาวไร่ บลาๆ.
2. สามารถดูแล้วเข้าใจง่าย ชัดเจนว่าปัจจุบัน carbon credit เป็นเท่าไหร่แล้ว.
3. สามารถเชื่อมต่อกับ plantform เช่น OneAgri Smart GIS etc. ของมิตรผลเพื่อเพิ่ม ฟีเจอร์ อื่นได้.
4. มีภาพถ่ายดาวเทียมเพื่อเพิ่มประสิทธิถาพในการทำงานมากขึ้น.

"""

st.markdown(markdown)

m = leafmap.Map(minimap_control=True, center=(16.465548, 102.126841), zoom=10)
m.add_basemap("OpenTopoMap")
m.to_streamlit(height=500)
