from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import dotenv
import os

dotenv.load_dotenv(override=True)

URL = os.getenv("DB_URL")
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")

es = Elasticsearch(
    [URL],
    basic_auth=(USERNAME, PASSWORD),
    verify_certs=False,
    request_timeout=600
)

def fetch_documents(index_name):
    query = {
        "query": {
            "match_all": {}
        }
    }
    result = helpers.scan(es, query=query, index=index_name)
    return result

def count_documents(index_name):
    count = es.count(index=index_name)["count"]
    return count

def create_new_index(new_index_name):
    mapping = {
        "mappings": {
            "properties": {
                "sold": {
                    "type": "keyword"
                }
            }
        }
    }

    if not es.indices.exists(index=new_index_name):
        es.indices.create(index=new_index_name, body=mapping)
        print(f"Index '{new_index_name}' created.")
    else:
        print(f"Index '{new_index_name}' already exists.")

def replicate_documents_with_sold(original_index, new_index):
    total_docs = count_documents(original_index)
    documents = fetch_documents(original_index)

    with tqdm(total=total_docs, desc="Replicating documents") as pbar:
        bulk_docs = []
        
        for doc in documents:
            doc['_source']['sold'] = "N"
            bulk_docs.append(doc)

            if len(bulk_docs) == 500:
                try:
                    helpers.bulk(es, bulk_docs, index=new_index)
                    pbar.update(len(bulk_docs))
                except Exception as e:
                    print(f"Error during bulk insert: {e}")
                bulk_docs = []

        if bulk_docs:
            try:
                helpers.bulk(es, bulk_docs, index=new_index)
                pbar.update(len(bulk_docs))
                print(f"Successfully pushed {len(bulk_docs)} documents to '{new_index}' (final chunk).")
            except Exception as e:
                print(f"Error during final bulk insert: {e}")

    print(f"Documents from '{original_index}' replicated to '{new_index}' with 'sold' field set to 'N'.")

original_index = 'listings'
new_index = 'listings_main'

create_new_index(new_index)
replicate_documents_with_sold(original_index, new_index)
