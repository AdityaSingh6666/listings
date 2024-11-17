from process_listings import main

from elasticsearch import Elasticsearch, helpers
import dotenv
import os
from tqdm import tqdm

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

def save_listing_id(index_name,batch_size=500):
    result = es.search(index=index_name, scroll='5m', size=batch_size, body={"query": {"match_all": {}}})
    scroll_id = result['_scroll_id']
    hits = result['hits']['hits']

    total_hits = result['hits']['total']['value']

    output_file = "listings_old.txt"

    with open(output_file, 'w') as file:
        with tqdm(total=total_hits, desc="Saving Listing IDs", unit="listing") as pbar:
            while len(hits) > 0:
                for hit in hits:
                    listing_id = hit['_source'].get('listingId')
                    if listing_id:
                        file.write(listing_id + '\n')
                        pbar.update(1) 

                result = es.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = result['_scroll_id']
                hits = result['hits']['hits'] 

    print(f"All listing IDs have been saved to {output_file}")

if __name__ == "__main__":

    save_listing_id(index_name="listings_main")
    main()