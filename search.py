import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import re

# Initialize Elasticsearch client
def get_es_client():
    return Elasticsearch(
        [{"host": "localhost", "port": 9200}]
    )

es = get_es_client()

def index_data(es, index_name, df):
    actions = [
        {
            "_index": index_name,
            "_source": row.to_dict(),
        }
        for _, row in df.iterrows()
    ]
    helpers.bulk(es, actions)

def read_and_index_sheets(uploaded_file):
    try:
        excel_data = pd.ExcelFile(uploaded_file)
        for sheet_name in excel_data.sheet_names:
            df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            df.columns = df.columns.str.lower()  # Lowercase column names
            df = df.applymap(lambda x: str(x).lower() if isinstance(x, str) else x)  # Lowercase all string data
            index_data(es, sheet_name, df)
    except Exception as e:
        st.error(f"Error reading or indexing Excel file: {e}")

def search_elasticsearch(es, index_name, keyword):
    query = {
        "query": {
            "multi_match": {
                "query": keyword,
                "fields": ["*"]
            }
        }
    }
    res = es.search(index=index_name, body=query)
    return res['hits']['hits']

# Streamlit app
uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx"])

if uploaded_file:
    # Index the uploaded file
    read_and_index_sheets(uploaded_file)
    st.write("Data indexed successfully!")

    # User input for the search keyword
    search_keyword = st.text_input("Enter keyword to search")
    
    if search_keyword:
        search_results = []
        excel_data = pd.ExcelFile(uploaded_file)
        for sheet_name in excel_data.sheet_names:
            hits = search_elasticsearch(es, sheet_name, search_keyword)
            if hits:
                for hit in hits:
                    search_results.append(hit['_source'])

        # Display search results if any
        if search_results:
            st.write("Search Results:")
            for result in search_results:
                st.write(result)
        else:
            st.write("No results found.")
