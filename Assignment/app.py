from flask import Flask, render_template, request, redirect, url_for, flash
from elasticsearch import Elasticsearch
import pandas as pd
import chardet

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Change this to a secure key in production
es = Elasticsearch("http://localhost:9200")


def create_collection(collection_name):
    if not es.indices.exists(index=collection_name):
        es.indices.create(index=collection_name)


def index_data(collection_name, exclude_column):
    # Detect encoding
    with open('employee.csv', 'rb') as f:  # Updated filename here
        result = chardet.detect(f.read())
        encoding = result['encoding']  # Use detected encoding

    # Read the CSV file with the detected encoding
    try:
        df = pd.read_csv('employee.csv', encoding=encoding)  # Updated filename here
    except UnicodeDecodeError:
        # Fallback to a known encoding if there's an error
        encoding = 'ISO-8859-1'
        df = pd.read_csv('employee.csv', encoding=encoding)  # Updated filename here

    df = df.drop(columns=[exclude_column])  # Exclude the specified column

    # Replace NaN values with None (or you could use another placeholder)
    df = df.where(pd.notnull(df), None)

    for _, row in df.iterrows():
        es.index(index=collection_name, document=row.to_dict())


def search_by_column(collection_name, column_name, column_value):
    query = {
        "query": {
            "match": {
                column_name: column_value
            }
        }
    }
    response = es.search(index=collection_name, body=query)
    return response['hits']['hits']


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/create_collection', methods=['POST'])
def create_collections():
    name_collection = request.form['name_collection']
    phone_collection = request.form['phone_collection']

    create_collection(name_collection)
    create_collection(phone_collection)

    flash(f"Collections '{name_collection}' and '{phone_collection}' created.")
    return redirect(url_for('index'))


@app.route('/index_data', methods=['POST'])
def index_employee_data():
    collection_name = request.form['collection_name']
    exclude_column = request.form['exclude_column']

    index_data(collection_name, exclude_column)
    flash(f"Data indexed in collection '{collection_name}' excluding column '{exclude_column}'.")
    return redirect(url_for('index'))


@app.route('/search', methods=['POST'])
def search():
    collection_name = request.form['collection_name']
    column_name = request.form['column_name']
    column_value = request.form['column_value']

    results = search_by_column(collection_name, column_name, column_value)
    return render_template('result.html', search_results=results)


if __name__ == '__main__':
    app.run(debug=True)
