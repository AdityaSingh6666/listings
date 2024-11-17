from base64 import b64decode
import time
import requests
import json
import random
import warnings
import dotenv
import os
from fake_useragent import UserAgent

dotenv.load_dotenv(override=True)

warnings.filterwarnings("ignore")

from listings_detail_graphql_query import LISTING_DETAIL_QUERY
from listings_list_graphql_query import LISTINGS_LIST_GRAPHQL_QUERY

class ListingsProcessor:
    def __init__(self, logger, proxies_file="proxies.txt", max_retries=50, retry_delay=5):
        self.graphql_api = "https://api-graphql-lambda.prod.zoopla.co.uk/graphql"
        self.logger = logger
        self.error_log_file = "/root/.rapidapi/logs/listings/postcode_and_listingid_error.txt"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.headers = {
            "accept": "*/*",
            "x-api-key": "3Vzj2wUfaP3euLsV4NV9h3UAVUR3BoWd5clv9Dvu",
            "origin": "zoopla-mobile-app",
            "Content-Type": "application/json",
            "Host": "api-graphql-lambda.prod.zoopla.co.uk",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
        }
        self.proxies_list = self.load_proxies(proxies_file)
        self.user_agent = UserAgent()
        self.zyte_api_key = os.getenv("ZYTE_API_KEY")
    
    def load_proxies(self, proxies_file):
        with open(proxies_file, "r") as file:
            proxies = [line.strip() for line in file if line.strip()]
        return proxies

    def get_random_proxy(self):
        proxy = random.choice(self.proxies_list)
        return {'http': proxy, 'https': proxy}
    
    def get_random_user_agent(self):
        return self.user_agent.random
    
    def resi_proxy(self):
        proxies = {
        'http': 'http://cxmzvqtw-rotate:zgp8h079llzg@p.webshare.io:80',
        'https': 'http://cxmzvqtw-rotate:zgp8h079llzg@p.webshare.io:80',
        }
        return proxies
    
    def fetch_listing(self, place_name):
        all_data = []
        listing_id_check = set()
        
        total_requests_count = 0
        page_number = 1
        
        identifier = place_name.lower().replace(' ', '-')
        value = place_name.lower().replace(' ', '%10')
        
        while True:
            # self.logger.info(f"Fetching page {page_number} for {place_name}...")
            url_path = f"/for-sale/property/{identifier}/?q={value}&results_sort=newest_listings&search_source=for-sale&pn={page_number}"
            # print(url)
            total_requests_count += 1
            # self.logger.info(f"Total requests: {total_requests_count}")

            api_response = None
            retries = 0
            while api_response is None and retries < self.max_retries:
                try:
                    payload = {
                        "operationName": "getListingData",
                        "variables": {
                            "path": url_path
                        },
                        "query": LISTINGS_LIST_GRAPHQL_QUERY
                    }
                    
                    self.headers["User-Agent"] = self.get_random_user_agent()
                    api_response = requests.post(url=self.graphql_api,
                                                 headers=self.headers,
                                                 data=json.dumps(payload),
                                                 verify=False,
                                                 timeout=15,
                                                 proxies=self.resi_proxy()
                                                 )
                    
                    if api_response.status_code != 200:
                        self.logger.error(f"Failed to fetch response for {place_name}: Status Code {api_response.status_code}. Retrying {retries}/{self.max_retries}...")
                    else:
                        break
                except Exception as error:
                    self.logger.error(f"Error in fetching response for {place_name}: {error}. Retrying {retries}/{self.max_retries}...")
                    api_response = None
                    retries += 1
                    time.sleep(self.retry_delay + random.uniform(2, 10))

            if retries >= self.max_retries:
                self.logger.error(f"Failed to fetch listings after {self.max_retries} retries for {place_name}")
                #check if the error log file exists
                if os.path.exists(self.error_log_file):
                    with open(self.error_log_file, "a") as file:
                        file.write(f"{place_name} - failed to fetch listings\n")
                else:
                    with open(self.error_log_file, "w") as file:
                        file.write(f"{place_name} - failed to fetch listings\n")
                break

            json_response = json.loads(api_response.text)
            
            # with open(f"response_{page_number}.json", 'w') as f:
            #     json.dump(json_response, f, indent=4)

            # if page_number == 1:
            #     with open('response.json', 'w') as f:
            #         json.dump(json_response, f, indent=4)
            
            no_more_data_error_str = "422: Unprocessable Entity"
            error_flag = False
            error_check = json_response.get('errors', [])
            if error_check:
                for error in error_check:
                    if error.get('message', "") == no_more_data_error_str:
                        self.logger.info(f"No more data available. Total pages fetched: {page_number - 1}, Total listings fetched: {len(all_data)} for {place_name}")
                        error_flag = True
                        break
            # if no more data available, break the loop
            if error_flag:
                break

            listings = json_response.get('data', {}).get('searchResults', {}).get('listings', {})
            
            current_page_dict = {
                'regular': listings.get('regular', []),
                # 'extended': listings.get('extended', []),
                'featured': listings.get('featured', [])
            }
            
            for listing in current_page_dict['regular']:
                listing_id = listing.get('listingId')
                if listing_id not in listing_id_check:
                    all_data.append(listing)
                    listing_id_check.add(listing_id)
            
            for listing in current_page_dict['featured']:
                listing_id = listing.get('listingId')
                if listing_id not in listing_id_check:
                    all_data.append(listing)
                    listing_id_check.add(listing_id)

            if current_page_dict['regular'] == [] and current_page_dict['featured'] == []:
                self.logger.info(f"No more data available. Total pages fetched: {page_number - 1}, Total listings fetched: {len(all_data)} for {place_name}")
                break
            
            # if current_page_dict['regular']:
            #     all_data.extend(current_page_dict['regular'])
            # ##REMOVED EXTENDED
            # # if current_page_dict['extended']:
            # #     all_data.extend(current_page_dict['extended'])
            # if current_page_dict['featured']:
            #     all_data.extend(current_page_dict['featured'])
            # print(f"Current page: {page_number}, Total listings fetched: {len(all_data)}")
            page_number += 1

        if all_data:
            return all_data
        else:
            return None

    def fetch_listing_details(self, listing_id):
        payload = {
            "operationName": "getListingDetails",
            "variables": {
                "listingId": int(listing_id),
                "include": ["EXPIRED"]
            },
            "query": LISTING_DETAIL_QUERY
        }
        
        retries = 0
        while retries < self.max_retries:
            try:
                self.headers["User-Agent"] = self.get_random_user_agent()
                resp = requests.post(url=self.graphql_api,
                                     headers=self.headers,
                                     data=json.dumps(payload),
                                     verify=False,
                                     timeout=10,
                                     proxies=self.resi_proxy()
                                     )
                if resp.status_code == 200:
                    # self.logger.info(f"Response fetched for listing ID {listing_id}")
                    break
            except Exception as e:
                self.logger.error(f"Error in fetching response for listing ID {listing_id}: {e}. Retrying {retries}/{self.max_retries}...")
                retries += 1
                time.sleep(self.retry_delay + random.uniform(2, 10))

        if retries >= self.max_retries:
            self.logger.error(f"Failed to fetch details after {self.max_retries} retries for listing ID {listing_id}")
            if os.path.exists(self.error_log_file):
                with open(self.error_log_file, "a") as file:
                    file.write(f"{listing_id} - failed to fetch details\n")
            else:
                with open(self.error_log_file, "w") as file:
                    file.write(f"{listing_id} - failed to fetch details\n")
            return None

        json_response = json.loads(resp.text)
        
        json_details = json_response.get('data', {}).get('listingDetails', {})
        if json_details:
            return json_details
        else:
            return None

# Example usage:
# from loguru import logger

# logger.add("zoopla.log", rotation="10 MB", retention="10 days", level="INFO")
# processor = ListingsProcessor(logger)

# # # # # Fetch listings for a location
# # place_name = "AL2 2FF"
# # all_data = processor.fetch_listing(place_name)
# # with open('all_data.json', 'w') as f:
# #     json.dump(all_data, f, indent=4)

# # # Fetch details for a listing
# listing_id = "67905027"
# listing_details = processor.fetch_listing_details(listing_id)
# with open('listing_details.json', 'w') as f:
#     json.dump(listing_details, f, indent=4)