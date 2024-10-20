# class EmployeeCollection:
#     def __init__(self):
#         self.collections = {}
#
#     def createCollection(self, p_collection_name):
#         if p_collection_name not in self.collections:
#             self.collections[p_collection_name] = []  # Initialize an empty list for the collection
#             print(f"Collection '{p_collection_name}' created.")
#         else:
#             print(f"Collection '{p_collection_name}' already exists.")
#
#     def indexData(self, p_collection_name, p_exclude_column):
#         if p_collection_name not in self.collections:
#             print(f"Collection '{p_collection_name}' does not exist.")
#             return
#
#         # Sample data to index
#         sample_data = [
#             {"id": "100001", "name": "Alice", "department": "HR", "gender": "Female"},
#             {"id": "100002", "name": "Bob", "department": "Engineering", "gender": "Male"},
#             {"id": "100003", "name": "Charlie", "department": "Sales", "gender": "Male"},
#             {"id": "100004", "name": "David", "department": "Engineering", "gender": "Male"},
#             {"id": "100005", "name": "Eve", "department": "", "gender": "Female"}
#         ]
#
#         for record in sample_data:
#             # Create indexed record excluding specified column
#             indexed_record = {k: v for k, v in record.items() if k != p_exclude_column}
#             self.collections[p_collection_name].append(indexed_record)
#
#         print(f"Data indexed into '{p_collection_name}' excluding '{p_exclude_column}'.")
#
#     def searchByColumn(self, p_collection_name, p_column_name, p_column_value):
#         if p_collection_name not in self.collections:
#             print(f"Collection '{p_collection_name}' does not exist.")
#             return []
#
#         results = [record for record in self.collections[p_collection_name] if
#                    record.get(p_column_name) == p_column_value]
#         return results
#
#     def getEmpCount(self, p_collection_name):
#         if p_collection_name not in self.collections:
#             print(f"Collection '{p_collection_name}' does not exist.")
#             return 0
#
#         emp_count = len([record for record in self.collections[p_collection_name] if record.get("id")])
#         print(f"Employee count in '{p_collection_name}': {emp_count}")
#         return emp_count
#
#     def delEmpById(self, p_collection_name, p_employee_id):
#         if p_collection_name not in self.collections:
#             print(f"Collection '{p_collection_name}' does not exist.")
#             return
#
#         initial_count = len(self.collections[p_collection_name])
#         self.collections[p_collection_name] = [record for record in self.collections[p_collection_name] if
#                                                record.get("id") != p_employee_id]
#         final_count = len(self.collections[p_collection_name])
#
#         if initial_count > final_count:
#             print(f"Employee with ID '{p_employee_id}' deleted from '{p_collection_name}'.")
#         else:
#             print(f"Employee with ID '{p_employee_id}' not found in '{p_collection_name}'.")
#
#     def getDepFacet(self, p_collection_name):
#         if p_collection_name not in self.collections:
#             print(f"Collection '{p_collection_name}' does not exist.")
#             return {}
#
#         department_count = {}
#         for record in self.collections[p_collection_name]:
#             dept = record.get("department")
#             if dept:
#                 department_count[dept] = department_count.get(dept, 0) + 1
#
#         print(f"Department count for collection '{p_collection_name}': {department_count}")
#         return department_count
#
#
# # Function Executions
# v_nameCollection = 'Hash_Reegan'  # Change to your name
# v_phoneCollection = 'Hash_2002'  # Change to your phone's last four digits
#
# # Create collections
# emp_collection = EmployeeCollection()
# emp_collection.createCollection(v_nameCollection)
# emp_collection.createCollection(v_phoneCollection)
#
# # Get initial employee counts
# print("Initial employee count in v_nameCollection:", emp_collection.getEmpCount(v_nameCollection))
# print("Initial employee count in v_phoneCollection:", emp_collection.getEmpCount(v_phoneCollection))
#
# # Index data while excluding the specified columns
# emp_collection.indexData(v_nameCollection, 'department')
# emp_collection.indexData(v_phoneCollection, 'gender')
#
# # Get employee counts after indexing
# print("Employee count in v_nameCollection after indexing:", emp_collection.getEmpCount(v_nameCollection))
# print("Employee count in v_phoneCollection after indexing:", emp_collection.getEmpCount(v_phoneCollection))
#
# # Example deletions and searches
# emp_collection.delEmpById(v_nameCollection, '100003')  # Deleting an employee by ID
# print("Employee count in v_nameCollection after deletion:", emp_collection.getEmpCount(v_nameCollection))
#
# # Searching by column
# search_results_name = emp_collection.searchByColumn(v_nameCollection, 'department', 'HR')
# print("Search results for 'HR' in v_nameCollection:", search_results_name)
#
# search_results_gender = emp_collection.searchByColumn(v_nameCollection, 'gender', 'Female')
# print("Search results for 'Female' in v_nameCollection:", search_results_gender)
#
# # Get department facets
# emp_collection.getDepFacet(v_nameCollection)
# emp_collection.getDepFacet(v_phoneCollection)
from elasticsearch import Elasticsearch
import pandas as pd

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200")

def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

def indexData(p_collection_name, p_exclude_column):
    # Load the dataset
    df = pd.read_csv('employee.csv')  # Change this to your file path
    df = df.drop(columns=[p_exclude_column])  # Exclude the specified column

    for _, row in df.iterrows():
        # Index each row as a document
        es.index(index=p_collection_name, document=row.to_dict())

    print(f"Data indexed in collection '{p_collection_name}' excluding column '{p_exclude_column}'.")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    return response['hits']['hits']

def getEmpCount(p_collection_name):
    return es.count(index=p_collection_name)['count']

def delEmpById(p_collection_name, p_employee_id):
    es.delete(index=p_collection_name, id=p_employee_id)
    print(f"Employee with ID '{p_employee_id}' deleted from collection '{p_collection_name}'.")

def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"  # Ensure you're using a keyword field for aggregation
                }
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    return response['aggregations']['departments']['buckets']

# Function Executions
v_nameCollection = 'Hash_Reegan'  # Replace 'Reegan' with your name
v_phoneCollection = 'Hash_2002'  # Replace '2002' with your phone last four digits

createCollection(v_nameCollection)
createCollection(v_phoneCollection)
print("Employee count in name collection:", getEmpCount(v_nameCollection))
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
delEmpById(v_nameCollection, 'E02003')
print("Employee count in name collection after deletion:", getEmpCount(v_nameCollection))
print("Search result for Department 'IT':", searchByColumn(v_nameCollection, 'Department', 'IT'))
print("Search result for Gender 'Male':", searchByColumn(v_nameCollection, 'Gender', 'Male'))
print("Search result for Department 'IT' in phone collection:", searchByColumn(v_phoneCollection, 'Department', 'IT'))
print("Department facet in name collection:", getDepFacet(v_nameCollection))
print("Department facet in phone collection:", getDepFacet(v_phoneCollection))
