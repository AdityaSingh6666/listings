from tqdm import tqdm
import concurrent.futures
import csv
import gc
from listings import ListingsProcessor
from common_setup import *

BATCH_SIZE = 100
MAX_WORKERS_POSTCODES = 50
MAX_WORKERS_LISTINGS = 10
BULK_FLUSH_SIZE = 1000
bulk_operations_count = 0

setup_logger("property_listings")

listings_folder = os.path.join('logs', 'listings')
os.makedirs(listings_folder, exist_ok=True)

POSTCODE_ERROR_TXT = os.path.join(listings_folder, 'postcode_error_property_listings.txt')
POSTCODE_SUCCESS_TXT = os.path.join(listings_folder, 'postcode_success_property_listings.txt')
LISTINGID_ERROR_TXT = os.path.join(listings_folder, 'listingid_error_property_listings.txt')

def flush_if_needed(force=False):
    global bulk_operations_count
    if force or bulk_operations_count >= BULK_FLUSH_SIZE:
        final_bulk_flush()
        bulk_operations_count = 0
        gc.collect()

def process_single_listing(listing, postcode,processed_listing_id):
    try:
        listing_id = listing.get('listingId')
        if not listing_id:
            logger.warning(f"Listing without ID found for {postcode}")
            return None
        existing_listing_ids = load_listing_main_ids()  
        listings_processor = ListingsProcessor(logger)
       
        delete_existing_entry(listing_id)
        if listing_id not in existing_listing_ids:
            listing_dict_to_be_pushed = {
                    'ref_postcode': postcode,
                    'soldStatus': 'No'
            }
            listing_dict_to_be_pushed.update(listing)

            listing_details = listings_processor.fetch_listing_details(listing_id)
            if listing_details:
                listing_dict_to_be_pushed.update(listing_details)
                listing_dict_to_be_pushed.pop('__typename', None)
            else:
                logger.error(f"Failed to get listing details for {listing_id}")
                with open(LISTINGID_ERROR_TXT, 'a') as f:
                    f.write(f"{listing_id} - {postcode} - NO DETAILS\n")
            
            listing_dict_to_be_pushed = convert_values_to_string(listing_dict_to_be_pushed)
            add_to_bulk('listings_main', listing_dict_to_be_pushed)
            bulk_operations_count += 1  # Increment after adding an operation to bulk
            flush_if_needed()
            
        return listing_id
    except Exception as e:
        logger.error(f"Error processing listing for {postcode}: {str(e)}")
        return None

def process_property_listings(postcode):
    global bulk_operations_count
    bulk_operations_count = 0
    listings_processor = ListingsProcessor(logger)
    
    try:
        listings = listings_processor.fetch_listing(postcode)
        if not listings:
            logger.warning(f"No listings found for {postcode}")
            with open(POSTCODE_SUCCESS_TXT, 'a') as f:
                f.write(f"{postcode} - 0\n")
            return
        
        existing_listing_ids = load_listing_main_ids()
        processed_ids = set()

        for i in range(0, len(listings), BATCH_SIZE):
            batch = listings[i:i + BATCH_SIZE]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_LISTINGS) as executor:
                futures = [
                    executor.submit(
                        process_single_listing, 
                        listing, 
                        postcode,
                    ) 
                    for listing in batch
                ]
                
                for future in concurrent.futures.as_completed(futures):
                    listing_id = future.result()
                    if listing_id:
                        processed_ids.add(listing_id)
            
            del batch
            gc.collect()
        
        sold_listing_ids = existing_listing_ids - processed_ids
        for i in range(0, len(sold_listing_ids), BATCH_SIZE):
            batch = list(sold_listing_ids)[i:i + BATCH_SIZE]
            
            for sold_id in batch:
                try:
                    sold_listing_dict = {
                        'listingId': str(sold_id),
                        'ref_postcode': postcode,
                        'soldStatus': 'Yes'
                    }

                    listing_details = listings_processor.fetch_listing_details(listing_id)
                    if listing_details:
                        sold_listing_dict.update(listing_details)
                        sold_listing_dict.pop('__typename', None)
                    else:
                        logger.error(f"Failed to get listing details for {listing_id}")
                        with open(LISTINGID_ERROR_TXT, 'a') as f:
                            f.write(f"{listing_id} - {postcode} - NO DETAILS\n")
                    
                    sold_listing_dict = convert_values_to_string(sold_listing_dict)
                    add_to_bulk('listings_main', sold_listing_dict)
                    bulk_operations_count += 1 
                    flush_if_needed()
                except Exception as e:
                    logger.error(f"Error processing sold listing {sold_id}: {str(e)}")

            del batch
            gc.collect()
        
        update_listings_file(processed_ids, sold_listing_ids)
        
        with open(POSTCODE_SUCCESS_TXT, 'a') as f:
            f.write(f"{postcode} - {len(listings)}\n")

    except Exception as e:
        logger.error(f"Failed to process listings for {postcode}: {e}")
        with open(POSTCODE_ERROR_TXT, 'a') as f:
            f.write(f"{postcode}, listings, ERROR, {e}\n")

def update_listings_file(new_listing_ids, sold_listing_ids):
    try:
        if not os.path.exists("listings_old.txt"):
            open("listings_old.txt", 'a').close()
        
        with open("listings_old.txt", 'r') as f:
            existing_lines = set(line.strip() for line in f)
        
        # Remove sold listings
        active_listings = existing_lines - sold_listing_ids
        
        # Add new listings
        active_listings.update(new_listing_ids)
        
        # Write back to file
        with open("listings_old.txt", 'w') as f:
            for listing_id in active_listings:
                f.write(f"{listing_id}\n")
        
        del existing_lines, active_listings
        gc.collect()
        
    except IOError as e:
        logger.error(f"Error handling listings file: {e}")
        raise

def main():
    create_index_if_not_exists('listings_main')
    csv_file_path = os.getenv("CSV_FILE_PATH")
    
    try:
        # Process postcodes in batches
        POSTCODE_BATCH_SIZE = 100
        
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header                     
            postcodes = [row[0] for row in reader]
        
        for i in range(0, len(postcodes), POSTCODE_BATCH_SIZE):
            batch_postcodes = postcodes[i:i + POSTCODE_BATCH_SIZE]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_POSTCODES) as executor:
                futures = {
                    executor.submit(process_property_listings, postcode): postcode 
                    for postcode in batch_postcodes
                }
                
                for future in tqdm(
                    concurrent.futures.as_completed(futures), 
                    total=len(futures),
                    desc=f"Processing batch {i//POSTCODE_BATCH_SIZE + 1}"
                ):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error processing postcode {futures[future]}: {e}")
            
            # Clear batch data
            del batch_postcodes
            gc.collect()
            
            # Force flush after each batch
            flush_if_needed(force=True)
    
    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user.")
        logger.info("Attempting to flush remaining records...")
        final_bulk_flush()
        logger.info("Remaining records saved.")
    
    finally:
        logger.warning("Final bulk flush...")
        final_bulk_flush()
        delete_duplicates(index_name = "listings_main")
        logger.info("Listings processing complete.")

if __name__ == "__main__":
    main()