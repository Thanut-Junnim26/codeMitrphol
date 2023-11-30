import streamlit as st
import leafmap.foliumap as leafmap

st.set_page_config(layout="wide")

st.sidebar.info(
    """
    Reference website 🎈🎈🎄
    - Web App URL: <https://streamlit.geemap.org>
    - GitHub repository: <https://github.com/giswqs/streamlit-geospatial>
    """
)

st.sidebar.title("Contact")

st.sidebar.info(
    """
    - CEO: เจเจ ปี2 KMITL
    - CEO: กัตเบ ปี2 มศว
    """
)
# st.sidebar.info(
#     """
#     Qiusheng Wu: <https://wetlands.io>
#     [GitHub](https://github.com/giswqs) | [Twitter](https://twitter.com/giswqs) | [YouTube](https://www.youtube.com/c/QiushengWu) | [LinkedIn](https://www.linkedin.com/in/qiushengwu)
#     """
# )

st.title("โรงงาน ปตท ผลิตก๊าซธรรมชาติ BY QuakeGas Sentinel🛩🛸🚀")

# with st.expander("See source code"):
#     with st.echo():

m = leafmap.Map(center=[40, -100], zoom=4)
cities = 'https://raw.githubusercontent.com/giswqs/leafmap/master/examples/data/us_cities.csv'
regions = 'gaskathon.geojson'

m.add_geojson(regions, layer_name='ASIA Regions')

# m.add_points_from_xy(
#     cities,
#     x="longitude",
#     y="latitude",
#     color_column='region',
#     icon_names=['gear', 'map', 'leaf', 'globe'],
#     spin=True,
#     add_legend=True,
# )
m.add_basemap('HYBRID')

m.to_streamlit(height=700)

# Map.add("basemap_selector")

# legend_dict = {
#     "ปริมาณCO2สูงกว่าปกติ <100%": "ff0000",
#     "ปริมาณCO2สูงค่อนข้างมาก<89%": "ff500d",
#     "ปริมาณCO2สูงมาก <78%": "ff6e08",
#     "ปริมาณCO2สูง <67%": "ff8b13",
#     "ปริมาณCO2ระดับปกติ <56%": "ffb613",
#     "ปริมาณCO2ต่ำ <45%": "ffd611",
#     "ปริมาณCO2ต่ำค่อนข้างต่ำ <34%": "fff705",
#     "ปริมาณCO2ต่ำกว่าปกติ <23%": "b5e22e",
#     "ปริมาณCO2ต่ำมาก <12%": "86e26f",
#     "ดินหรือน้ำ": "0502a3"
# }
# Map.add_legend(title="carbon quantity",
#                legend_dict=legend_dict, position='bottomleft')
# Map.to_streamlit()

# Map

# st.write('Yes Camp please')
# ee.Authenticate()
# ee.Initialize()
# # import ee
# # ee.Authenticate()
# # ee.Initialize()
# print(ee.Image("NASA/NASADEM_HGT/001").get("title").getInfo())
# print('hi')
# # Store the initial value of widgets in session state
# if "visibility" not in st.session_state:
#     st.session_state.visibility = "visible"
#     st.session_state.disabled = False

# col1, col2 = st.columns(2)

# with col1:
#     st.checkbox("Disable text input widget", key="disabled")
#     st.radio(
#         "Set text input label visibility 👉",
#         key="visibility",
#         options=["กลบเศษซาก", "hidden", "collapsed"],
#     )
#     st.text_input(
#         "Placeholder for the other text input widget",
#         "This is a placeholder",
#         key="placeholder",
#     )

# with col2:
# area_input = st.text_input("name area 👇", key='hello')
# st.write(area_input)
# number = st.number_input('area of size', key='1,2,3,4,5')
# st.write(int(number))

# st.form_submit_button(label="Submit", on_click=None,
#                       type="secondary", disabled=False, use_container_width=False)
# bury_input = st.text_input()

# if text_input:
#     st.write("You entered: ", text_input)

# lst = []
# formbtn = st.button("Form")

# if "formbtn_state" not in st.session_state:
#     st.session_state.formbtn_state = False

# if formbtn or st.session_state.formbtn_state:
#     st.session_state.formbtn_state = True

#     st.subheader("User Info Form")
#     # name = st.text_input("Name")
#     with st.form(key='user_info'):
#         st.write('User Information')

#         while (True):
#             name = st.text_input(label="Name 📛")
#             age = st.number_input(label="Age 🔢", value=0)
#             email = st.text_input(label="Email 📧")
#             phone = st.text_input(label="Phone 📱")
#             gender = st.radio(
#                 "Gender 🧑", ("Male", "Female", "Prefer Not To Say"))

#             submit_form = st.form_submit_button(
#                 label="Register", help="Click to register!")

#             # Checking if all the fields are non empty
#             if st.form_submit_button(label="Break"):
#                 break

#             if submit_form:
#                 st.write(submit_form)

#                 if name and age and email and phone and gender:
#                     # add_user_info(id, name, age, email, phone, gender)
#                     st.success(
#                         f"ID:  \n Name: {name}  \n Age: {age}  \n Email: {email}  \n Phone: {phone}  \n Gender: {gender}"
#                     )
#                     lst.append(name)
#                     st.write(lst)
#                 else:
#                     st.warning("Please fill all the fields")


# def main():
#     st.title('Employer Test')

#     # Check if 'started' key exists in session_state, if not, display candidate information
#     if not st.session_state.get('started'):
#         st.markdown('## Candidate Information')

#         # Full Name Input
#         full_name = st.text_input('Full Name')

#         # Experience Dropdown
#         experience = st.selectbox("Experience", ["Fresher"], index=0)

#         # Language Dropdown
#         language = st.selectbox("Language", ["Python"], index=0)

#         # Button to start the test
#         if st.button('Submit'):
#             if full_name:
#                 st.session_state['started'] = True
#                 st.session_state['full_name'] = full_name
#                 run_python_test()
#     else:
#         run_python_test()


# def run_python_test():
#     st.title('Python Test')

#     # Dummy test questions and answers
#     questions = [
#         {
#             'question': 'What is the output of the following code?\n\n```python\nx = 5\nprint(x)\n```',
#             'options': ['5', '10', '0', 'Error'],
#             'correct_answer': '5'
#         },
#         {
#             'question': 'Which of the following is a Python data type?',
#             'options': ['List', 'Streamlit', 'GitHub', 'HTML'],
#             'correct_answer': 'List'
#         },
#         {
#             'question': 'What is the result of the expression 3 + 7 * 2?',
#             'options': ['13', '20', '17', 'Error'],
#             'correct_answer': '17'
#         }
#         # Add more questions here...
#     ]

#     total_questions = len(questions)
#     if 'answer' not in st.session_state:
#         st.session_state['answer'] = [''] * total_questions

#     for i, question_data in enumerate(questions):
#         question = question_data['question']

#         st.write(f'Question {i+1}: {question}')

#         # Display options for each question
#         st.session_state['answer'][i] = st.text_area(
#             f"Enter answer for Question {i+1}", value=st.session_state['answer'][i], key=f"answer_{i}")

#     # Add a submit button
#     if st.button("Submit Test", key="submit_test"):
#         score = 0
#         # Process the answers and calculate score
#         for i, ans in enumerate(st.session_state['answer']):
#             if ans == questions[i]['correct_answer']:
#                 score += 1
#             st.write(f"Question {i+1} Answer: {ans}")

#         percentage_score = (score / total_questions) * 100
#         st.write(percentage_score)

#         if percentage_score >= 60:
#             save_result(st.session_state['full_name'], percentage_score)
#             st.session_state['test_completed'] = True


# def save_result(full_name, score):
#     data = {'Full Name': [full_name], 'Score': [score]}
#     df = pd.DataFrame(data)
#     df.to_csv('test_results.csv', index=False)
#     st.write('Result saved successfully!')


# if __name__ == '__main__':
#     main()


# def test_1(val_1, val_2):

#     st.write(f"val_1: {val_1}")
#     st.write("----------------")
#     st.write(f"val_2: {val_2}")
#     st.write("----------------")

#     # This could be a CRUD function


# def test_2(val_1_key, val_2_key):

#     st.write(
#         f"val_1_key: {st.session_state[val_1_key] if val_1_key in st.session_state else 'Key-Error'}")
#     st.write("----------------")
#     st.write(
#         f"val_2_key: {st.session_state[val_2_key] if val_2_key in st.session_state else 'Key-Error'}")
#     st.write("----------------")

#     # This could be a CRUD function


# def insert_database(id: int, text: str) -> None:

#     with st.spinner(text="Writing to database"):

#         st.write(id)
#         st.write(text)


# def prepare_insert_database(id: int, key_identifier: str):

#     # This middleware function is not really generic... Probably there are variation which value is in the session_state and which is fixed
#     insert_database(id=id, text=st.session_state[key_identifier])


# def main():

#     with st.expander("Example without forms"):

#         st.text_area(label="Please enter values", key="text_refs")

#         st.button(label="Submit", on_click=test_1, kwargs=dict(
#             val_1=10, val_2=st.session_state.text_refs))

#         st.session_state["submit_v2_val1"] = 10

#         st.button(label="Submit - V2", on_click=test_2,
#                   kwargs=dict(val_1_key="submit_v2_val1", val_2_key="text_refs"))

#     with st.expander("With forms"):

#         with st.form(key="form_master_key", clear_on_submit=True):

#             st.text_area(label="Please enter values", key="text_refs_form")

#             test_val = 11
#             st.session_state["submit_v2_val1_form"] = test_val

#             # This doesn't work and the displayed value is one behind.
#             st.form_submit_button(label="Submit with on-click-Event", on_click=test_1,
#                                   kwargs=dict(val_1=10, val_2=st.session_state.text_refs_form))

#             # This works but submitting keys to function to retrieve the real values within the function. This feels not really pythonic
#             # st.form_submit_button(label="Submit with on-click-Event and keys", on_click=test_2, kwargs=dict(val_1_key="submit_v2_val1_form", val_2_key="text_refs_form"))

#     # Start examples

#     st.header("Real-Example without Callback and Form")

#     input_reference = st.text_input(
#         "Please input text", key="real_example_no_callback_reference")

#     if st.button("Save data"):

#         insert_database(id=0, text=input_reference)

#     st.header("Real-Example with Callback and Form (Working)")

#     with st.form(key="real_example_form", clear_on_submit=True):

#         input_reference_2 = st.text_input(
#             "Please input text", key="real_example_callback_reference")

#         st.form_submit_button(
#             label="Save data", on_click=prepare_insert_database, kwargs=dict(id=0, key_identifier="real_example_callback_reference")
#         )

#     st.header("Not working solution but the most elegant one (Broken)")

#     with st.form(key="real_example_form_not_working", clear_on_submit=True):

#         input_reference_2 = st.text_input(
#             "Please input text", key="real_example_callback_reference_not_working")

#         st.form_submit_button(
#             label="Save data", on_click=insert_database, kwargs=dict(id=0, text=st.session_state.real_example_callback_reference_not_working)
#         )

#     if st.button(label="Clear-Cache"):
#         for item in st.session_state:
#             del st.session_state[item]


# main()

# import ee
# ee.Authenticate()
# ee.Initialize()
# print(ee.Image("NASA/NASADEM_HGT/001").get("title").getInfo())

# ---------------------------------------------------------------------------------

# Data Source: https://public.tableau.com/app/profile/federal.trade.commission/viz/FraudandIDTheftMaps/AllReportsbyState
# US State Boundaries: https://public.opendatasoft.com/explore/dataset/us-state-boundaries/export/


# -------------------------------------------------------------------------------------------
# import streamlit as st
# import pandas as pd
# import folium
# from streamlit_folium import st_folium

# APP_TITLE = 'Fraud and Identity Theft Report'
# APP_SUB_TITLE = 'Source: Federal Trade Commission'

# def display_time_filters(df):
#     year_list = list(df['Year'].unique())
#     year_list.sort()
#     year = st.sidebar.selectbox('Year', year_list, len(year_list)-1)
#     quarter = st.sidebar.radio('Quarter', [1, 2, 3, 4])
#     st.header(f'{year} Q{quarter}')
#     return year, quarter

# def display_state_filter(df, state_name):
#     state_list = [''] + list(df['State Name'].unique())
#     state_list.sort()
#     state_index = state_list.index(state_name) if state_name and state_name in state_list else 0
#     return st.sidebar.selectbox('State', state_list, state_index)

# def display_report_type_filter():
#     return st.sidebar.radio('Report Type', ['Fraud', 'Other'])

# def display_map(df, year, quarter):
#     df = df[(df['Year'] == year) & (df['Quarter'] == quarter)]

#     map = folium.Map(location=[38, -96.5], zoom_start=4, scrollWheelZoom=False, tiles='CartoDB positron')

#     choropleth = folium.Choropleth(
#         geo_data='data/us-state-boundaries.geojson',
#         data=df,
#         columns=('State Name', 'State Total Reports Quarter'),
#         key_on='feature.properties.name',
#         line_opacity=0.8,
#         highlight=True
#     )
#     choropleth.geojson.add_to(map)

#     df_indexed = df.set_index('State Name')
#     for feature in choropleth.geojson.data['features']:
#         state_name = feature['properties']['name']
#         feature['properties']['population'] = 'Population: ' + '{:,}'.format(df_indexed.loc[state_name, 'State Pop'][0]) if state_name in list(df_indexed.index) else ''
#         feature['properties']['per_100k'] = 'Reports/100K Population: ' + str(round(df_indexed.loc[state_name, 'Reports per 100K-F&O together'][0])) if state_name in list(df_indexed.index) else ''

#     choropleth.geojson.add_child(
#         folium.features.GeoJsonTooltip(['name', 'population', 'per_100k'], labels=False)
#     )

#     st_map = st_folium(map, width=700, height=450)

#     state_name = ''
#     if st_map['last_active_drawing']:
#         state_name = st_map['last_active_drawing']['properties']['name']
#     return state_name

# def display_fraud_facts(df, year, quarter, report_type, state_name, field, title, string_format='${:,}', is_median=False):
#     df = df[(df['Year'] == year) & (df['Quarter'] == quarter)]
#     df = df[df['Report Type'] == report_type]
#     if state_name:
#         df = df[df['State Name'] == state_name]
#     df.drop_duplicates(inplace=True)
#     if is_median:
#         total = df[field].sum() / len(df[field]) if len(df) else 0
#     else:
#         total = df[field].sum()
#     st.metric(title, string_format.format(round(total)))

# def main():
#     st.set_page_config(APP_TITLE)
#     st.title(APP_TITLE)
#     st.caption(APP_SUB_TITLE)

#     #Load Data
#     df_continental = pd.read_csv('data/AxS-Continental_Full Data_data.csv')
#     df_fraud = pd.read_csv('data/AxS-Fraud Box_Full Data_data.csv')
#     df_median = pd.read_csv('data/AxS-Median Box_Full Data_data.csv')
#     df_loss = pd.read_csv('data/AxS-Losses Box_Full Data_data.csv')

#     #Display Filters and Map
#     year, quarter = display_time_filters(df_continental)
#     state_name = display_map(df_continental, year, quarter)
#     state_name = display_state_filter(df_continental, state_name)
#     report_type = display_report_type_filter()

#     #Display Metrics
#     st.subheader(f'{state_name} {report_type} Facts')

#     col1, col2, col3 = st.columns(3)
#     with col1:
#         display_fraud_facts(df_fraud, year, quarter, report_type, state_name, 'State Fraud/Other Count', f'# of {report_type} Reports', string_format='{:,}')
#     with col2:
#         display_fraud_facts(df_median, year, quarter, report_type, state_name, 'Overall Median Losses Qtr', 'Median $ Loss', is_median=True)
#     with col3:
#         display_fraud_facts(df_loss, year, quarter, report_type, state_name, 'Total Losses', 'Total $ Loss')


# if __name__ == "__main__":
#     main()
