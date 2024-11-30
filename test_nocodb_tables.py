import requests
import yaml
import logging
import os
import json

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def load_config():
    config_path = os.path.join('accounts', 'quecreate', 'nocodb.yml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_table(base_url, headers, table_name, columns):
    # Convert NocoDB column types to UI types (uidt)
    type_mapping = {
        'string': 'SingleLineText',
        'integer': 'Number',
        'boolean': 'Checkbox',
        'datetime': 'DateTime'
    }
    
    # Format columns for NocoDB API
    formatted_columns = []
    for col in columns:
        formatted_col = {
            'column_name': col['name'],
            'title': col['name'],
            'uidt': type_mapping[col['type']],
            'dt': col['type'].upper() if col['type'] != 'datetime' else 'TIMESTAMP',
        }
        formatted_columns.append(formatted_col)
    
    # Prepare table creation payload
    payload = {
        'table_name': table_name,
        'title': table_name,
        'columns': formatted_columns
    }
    
    # Use meta API endpoint for table creation
    meta_url = base_url.replace('/api/v1/db/data/v1/', '/api/v1/db/meta/projects/')
    create_url = f"{meta_url}/tables"
    
    logger.info(f"Creating table {table_name} at URL: {create_url}")
    logger.debug(f"Table creation payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(create_url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Successfully created table {table_name}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create table {table_name}: {str(e)}")
        if hasattr(e.response, 'text'):
            logger.error(f"Response: {e.response.text}")
        return False

def main():
    # Load configuration
    config = load_config()
    
    # Setup headers
    headers = {
        'accept': 'application/json',
        'xc-token': config['api_token'],
        'Content-Type': 'application/json'
    }
    
    # Create each table
    for table_config in config['tables'].values():
        table_name = table_config['name']
        columns = table_config['columns']
        
        success = create_table(
            config['base_url'],
            headers,
            table_name,
            columns
        )
        
        if not success:
            logger.error(f"Failed to create table {table_name}")
            return

if __name__ == '__main__':
    main()
