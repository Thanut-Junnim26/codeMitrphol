import streamlit as st
import pandas as pd
file_path = 'example.csv'
st.title("Data collecting")
reply_container = st.container()
container = st.container()

with st.form(key='my_form', clear_on_submit=False):
    name = st.text_input(
        "Name (required)", placeholder="Please fill your name", key='input')
    age = st.number_input("Age (required)", placeholder="Please fill your age")
    date = st.date_input("Date (required)")
    submit_button = st.form_submit_button(label='Send')

    if submit_button and name and age and date:
        df = pd.read_csv(file_path)
        new_row_dict = {'Name': name, 'Age': age, 'Date': date}
        df = df.append(new_row_dict, ignore_index=True)
        df.to_csv(file_path, index=False)
