import logging
import os
import requests
import yaml
import traceback
import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path):
    """Load NocoDB configuration from YAML file."""
    try:
        if not os.path.exists(config_path):
            logger.error(f"NocoDB config file not found: {config_path}")
            return None
                
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate required fields
        required_fields = ["base_url", "api_token", "base_id"]
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            logger.error(f"Missing required fields in nocodb.yml: {missing_fields}")
            return None
                
        return config
                
    except Exception as e:
        logger.error(f"Failed to load NocoDB config: {str(e)}")
        return None

def init_tables(base_url, api_token, base_id, table_schemas):
    """Initialize NocoDB tables."""
    logger.info("Initializing NocoDB tables...")
    try:
        headers = {
            "xc-token": api_token,
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Test connection first
        response = requests.get(
            f"{base_url}/api/v1/health",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        logger.info("NocoDB connection successful!")
        
        # Get existing tables using the correct API endpoint
        response = requests.get(
            f"{base_url}/api/v1/db/meta/projects/{base_id}/tables",
            headers=headers
        )
        response.raise_for_status()
        
        # Log the actual response for debugging
        response_json = response.json()
        logger.info(f"API Response: {json.dumps(response_json, indent=2)}")
        
        # Handle empty list case and list wrapper
        if not response_json or not response_json.get('list'):
            existing_tables = set()
        else:
            existing_tables = {table["title"] for table in response_json.get('list', [])}
            
        logger.info(f"Existing tables: {existing_tables}")
        
        # Create missing tables
        for schema in table_schemas.values():
            if schema["title"] not in existing_tables:
                logger.info(f"Creating table: {schema['title']}")
                # Create table
                table_data = {
                    "title": schema["title"],
                    "table_name": schema["table_name"],
                    "columns": [
                        {
                            "column_name": col["column_name"],
                            "title": col["title"],
                            "uidt": col["uidt"],
                            "dt": col["dt"]
                        }
                        for col in schema["columns"]
                    ]
                }
                
                # Log request data for debugging
                logger.info(f"Creating table with data: {json.dumps(table_data, indent=2)}")
                
                response = requests.post(
                    f"{base_url}/api/v1/db/meta/projects/{base_id}/tables",
                    headers=headers,
                    json=table_data
                )
                response.raise_for_status()
                logger.info(f"Created table: {schema['title']}")
                
                # Log response for debugging
                logger.info(f"Table creation response: {json.dumps(response.json(), indent=2)}")
            else:
                logger.info(f"Table already exists: {schema['title']}")
                    
        logger.info("Table initialization completed!")
        return True
            
    except Exception as e:
        logger.error(f"Failed to initialize tables: {str(e)}")
        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def main():
    try:
        # Load NocoDB configuration
        config_path = os.path.join('accounts', 'quecreate', 'nocodb.yml')
        logger.info("Loading NocoDB configuration...")
        config = load_config(config_path)
        if not config:
            logger.error("Failed to load NocoDB configuration")
            return
            
        # Set up connection parameters
        base_url = config["base_url"]
        api_token = config["api_token"]
        base_id = config["base_id"]
        
        # Default table schemas if not in config
        default_schemas = {
            "interacted_users": {
                "table_name": "interacted_users",
                "title": "Interacted Users",
                "columns": [
                    {"column_name": "User_Id", "title": "User Id", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Username", "title": "Username", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Full_Name", "title": "Full Name", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Profile_URL", "title": "Profile URL", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Interaction_Type", "title": "Interaction Type", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Success", "title": "Success", "uidt": "Checkbox", "dt": "boolean"},
                    {"column_name": "Timestamp", "title": "Timestamp", "uidt": "DateTime", "dt": "datetime"}
                ]
            },
            "history_filters_users": {
                "table_name": "history_filters_users",
                "title": "History Filters Users",
                "columns": [
                    {"column_name": "User_Id", "title": "User Id", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Username", "title": "Username", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Filtered_At", "title": "Filtered At", "uidt": "DateTime", "dt": "datetime"},
                    {"column_name": "Filter_Type", "title": "Filter Type", "uidt": "SingleLineText", "dt": "varchar"}
                ]
            }
        }
        
        # Load table schemas from config or use defaults
        table_schemas = config.get("table_schemas", default_schemas)
        
        # Initialize tables
        logger.info("Initializing tables...")
        if init_tables(base_url, api_token, base_id, table_schemas):
            logger.info("Tables initialized successfully!")
        else:
            logger.error("Failed to initialize tables")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
