import requests
import yaml
import json
import logging
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def load_config():
    with open('accounts/quecreate/nocodb.yml', 'r') as f:
        return yaml.safe_load(f)

def main():
    # Load configuration
    config = load_config()
    base_url = config['base_url']
    headers = {
        'xc-token': config['api_token']
    }

    # Get table ID from meta API
    meta_url = f"{base_url}/api/v1/db/meta/projects/{config['base_id']}/tables"
    response = requests.get(meta_url, headers=headers)
    tables = response.json()['list']
    
    # Print raw response
    logger.info("\nRaw response:")
    logger.info(json.dumps(tables, indent=2))
    
    # Print available tables
    logger.info("\nAvailable tables:")
    for table in tables:
        if isinstance(table, dict):
            logger.info(f"- {table.get('title', 'Unknown')} (id: {table.get('id', 'Unknown')}, table_name: {table.get('table_name', 'Unknown')})")
    
    test_table_id = None
    for table in tables:
        if isinstance(table, dict) and 'title' in table and table['title'] == 'history_filters_users':
            test_table_id = table['id']
            logger.info(f"\nSelected table: {table['title']} (id: {test_table_id})")
            break
    
    if not test_table_id:
        logger.error("Test table not found!")
        return

    # Get table schema
    schema_url = f"{base_url}/api/v2/tables/{test_table_id}/columns"
    response = requests.get(schema_url, headers=headers)
    logger.info("\nTable schema:")
    logger.info(json.dumps(response.json(), indent=2))

    # First get sample data from the table to understand structure
    print("\nFetching sample data to analyze structure...")
    sample_url = f"{base_url}/api/v2/tables/{test_table_id}/records?limit=1"
    sample_response = requests.get(sample_url, headers=headers)
    
    if sample_response.status_code == 200:
        sample_data = sample_response.json()
        if sample_data.get('list') and len(sample_data['list']) > 0:
            sample_record = sample_data['list'][0]
            print("\nSample record structure:")
            for key, value in sample_record.items():
                print(f"- {key}: {value}")
            
            # Try different filter formats based on actual data
            test_filters = [
                {
                    'name': 'Query string format',
                    'filter': "(User Id,eq,test_user_123)"
                },
                {
                    'name': 'Multiple conditions query string',
                    'filter': "(User Id,eq,test_user_123)~and(Filter Type,eq,test_filter)"
                },
                {
                    'name': 'LIKE operator',
                    'filter': "(User Id,like,test_user%)"
                },
                {
                    'name': 'IN operator',
                    'filter': "(User Id,in,test_user_123,test_user_456)"
                }
            ]
            
            for test in test_filters:
                print(f"\nTesting filter format: {test['name']}")
                filter_url = f"{base_url}/api/v2/tables/{test_table_id}/records"
                filter_response = requests.get(
                    filter_url,
                    params={'where': test['filter']},
                    headers=headers
                )
                
                print(f"Filter response status: {filter_response.status_code}")
                if filter_response.status_code == 200:
                    print("Filter successful!")
                    filter_data = filter_response.json()
                    print(f"Found {len(filter_data.get('list', []))} matching records")
                else:
                    print("Filter failed")
                    print(f"Response: {filter_response.text}")
                print("-" * 50)

    # First try without any filter
    logger.info("\nTesting without filter")
    url = f"{base_url}/api/v2/tables/{test_table_id}/records"
    params = {
        'limit': 25,
        'offset': 0
    }
    
    response = requests.get(url, headers=headers, params=params)
    logger.debug(f"Request URL: {response.url}")
    logger.debug(f"Response Status: {response.status_code}")
    logger.debug(f"Response Body: {response.text}")
    
    if response.status_code == 200:
        logger.info("\u2705 Successfully retrieved records without filter")
        records = response.json()
        if records and 'list' in records:
            # Get column names from first record
            sample_record = records['list'][0]
            logger.info(f"\nSample record fields: {list(sample_record.keys())}")
    else:
        logger.error("\u274c Failed to retrieve records without filter")

if __name__ == "__main__":
    main()
