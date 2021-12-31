from preprocessing import preprocess_data
from stage_data import stage_data
from extract_data import extract_data
from quality_check import check_data_quality

import logging
logging.basicConfig(filename='staging.log', level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)

def main():
    
    # Clean data
    logging.info("----- Clean data -----")
    preprocess_data()
    
    # Stage data
    logging.info("----- Stage data -----")
    stage_data()
    
    # Extract data into fact and dimension tables
    logging.info("----- Extract data -----")
    extract_data()
    
    # Check data quality
    logging.info("----- Check data quality -----")
    check_data_quality()
    

if __name__ == "__main__":
    main()