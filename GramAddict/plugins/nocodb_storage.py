"""Store Instagram interaction history in NocoDB cloud database."""

# IMPORTANT NOTE:
# NocoDB API Authentication:
# - The correct header key for authentication is "xc-token"
# - DO NOT use "xc-auth" as it will result in authentication failures
# - Example: headers = {"xc-token": api_token, "accept": "application/json"}
#
# Tables Initialization:
# - Tables must be initialized using nocodb_config.get("tables", self.tables)
# - This ensures fallback to default tables if not in config
# - Direct assignment (self.tables = nocodb_config["tables"]) will break if tables missing
# - Always validate tables exist before initialization with hasattr(self, 'tables')

import logging
import requests
import yaml
import os
import json
import time
from datetime import datetime
from typing import Dict, Optional, List
from GramAddict.core.plugin_loader import Plugin
import uuid
import traceback

# Use GramAddict's root logger
logger = logging.getLogger(__name__)

# Set up separate API logger for detailed API logs
os.makedirs('logs', exist_ok=True)
api_logger = logging.getLogger('nocodb_api')

# Remove all existing handlers to avoid duplicate logs
for handler in api_logger.handlers[:]:
    api_logger.removeHandler(handler)

api_logger.setLevel(logging.DEBUG)
api_handler = logging.FileHandler('logs/nocodb_operations.log')
api_handler.setLevel(logging.DEBUG)
api_formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
api_handler.setFormatter(api_formatter)
api_logger.addHandler(api_handler)
api_logger.propagate = False  # Don't propagate API logs

def log_api_call(method, url, headers=None, data=None, response=None):
    """Helper function to log API calls consistently"""
    api_logger.debug(f"\n{'='*80}")
    api_logger.debug(f"API Call: {method} {url}")
    if headers:
        sanitized_headers = headers.copy()
        if 'xc-token' in sanitized_headers:
            sanitized_headers['xc-token'] = '***'
        api_logger.debug(f"Headers: {json.dumps(sanitized_headers, indent=2)}")
    if data:
        api_logger.debug(f"Request Data: {json.dumps(data, indent=2)}")
    if response:
        api_logger.debug(f"Response Status: {response.status_code}")
        try:
            api_logger.debug(f"Response Data: {json.dumps(response.json(), indent=2)}")
        except:
            api_logger.debug(f"Response Text: {response.text}")
    api_logger.debug(f"{'='*80}\n")

class NocoDBStorage(Plugin):
    """Plugin for storing interaction history in NocoDB."""

    # Constants for column names
    COLUMN_USER_ID = "User Id"
    COLUMN_FILTER_TYPE = "Filter Type"
    COLUMN_FILTERED_AT = "filtered_at"

    def __init__(self, config_path: str = None):
        """Initialize NocoDB storage plugin.
        
        Args:
            config_path: Path to NocoDB config file
        """
        self.config_path = config_path or "accounts/quecreate/nocodb.yml"
        self.config = self.load_config(self.config_path)
        self.base_url = self.config["base_url"]
        self.api_token = self.config["api_token"]
        self.base_id = self.config["base_id"]
        self.headers = {"xc-token": self.api_token}
        self.table_configs = self.config["tables"]
        
        # Store table IDs - these will be updated when tables are created/retrieved
        self.interacted_table_id = None  # Will be set during table initialization
        self.history_filters_table_id = None  # Will be set during table initialization
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("NocoDBStorage plugin initialized with default configuration")

        self.tables = {
            "interaction_history": {
                "name": "interacted_users",
                "columns": [
                    {"name": "id", "type": "ID"},
                    {"name": "user_id", "type": "string"},
                    {"name": "username", "type": "string"},
                    {"name": "interaction_at", "type": "datetime"},
                    {"name": "session_id", "type": "string"},
                    {"name": "job_name", "type": "string"},
                    {"name": "target", "type": "string"},
                    {"name": "followed", "type": "boolean"},
                    {"name": "is_requested", "type": "boolean"},
                    {"name": "scraped", "type": "boolean"},
                    {"name": "liked_count", "type": "integer"},
                    {"name": "watched_count", "type": "integer"},
                    {"name": "commented_count", "type": "integer"},
                    {"name": "pm_sent", "type": "boolean"},
                    {"name": "success", "type": "boolean"}
                ]
            },
            "history_filters": {
                "name": "history_filters_users",
                "columns": [
                    {"name": "id", "type": "ID"},
                    {"name": "User Id", "type": "string"},
                    {"name": "filtered_at", "type": "datetime"},
                    {"name": "Filter Type", "type": "string"}
                ]
            }
        }
        
        self.logger.info("NocoDBStorage plugin initialized with default configuration")
        self.logger.info("="*80)

    def run(self, device, config):
        """Run the plugin.
        
        Args:
            device: Android device
            config: Configuration
        """
        logger.info("="*80)
        logger.info("NocoDBStorage Plugin Initialization")
        logger.info("="*80)
        api_logger.info("NocoDBStorage Plugin Initialization Started")
        
        # Check if config is valid
        if not config:
            logger.error("No configuration provided")
            api_logger.error("No configuration provided")
            return
            
        logger.info(f"Config type: {type(config)}")
        logger.info(f"Config keys: {config.keys() if hasattr(config, 'keys') else 'No keys method'}")
        logger.info(f"enable_nocodb value: {config.get('enable_nocodb')}")
        logger.info(f"username value: {config.get('username')}")
        
        api_logger.debug(f"Config received: {json.dumps(config, indent=2)}")

        if not config.get("enable_nocodb"):
            logger.info("NocoDB storage is disabled. Skipping initialization.")
            api_logger.info("NocoDB storage is disabled. Skipping initialization.")
            return
            
        if not config.get("username"):
            logger.error("No username provided in config")
            api_logger.error("No username provided in config")
            return

        try:
            # Validate required fields
            required_fields = ["base_url", "api_token", "base_id"]
            missing_fields = [field for field in required_fields if not self.config.get(field)]
            if missing_fields:
                logger.error(f"Missing required fields in nocodb.yml: {missing_fields}")
                api_logger.error(f"Missing required fields in nocodb.yml: {missing_fields}")
                return
            
            logger.info("NocoDB Configuration:")
            logger.info("-"*50)
            logger.info(f"Base URL: {self.config.get('base_url')}")
            logger.info(f"Base ID: {self.config.get('base_id')}")
            logger.info(f"Tables: {json.dumps(self.config.get('tables', {}), indent=2)}")
            logger.info("-"*50)
            
            api_logger.debug("NocoDB Configuration loaded successfully")
            api_logger.debug(f"Configuration: {json.dumps(self.config, indent=2)}")

            # Initialize connection parameters
            self.tables = self.config.get("tables", self.tables)  # Use default tables if not in config

            # Test connection
            logger.info("Testing NocoDB connection...")
            api_logger.info("Testing NocoDB connection...")
            try:
                response = requests.get(
                    f"{self.base_url}/api/v1/health",
                    headers=self.headers,
                    timeout=10
                )
                response.raise_for_status()
                logger.info("NocoDB connection successful!")
                api_logger.info("NocoDB connection successful!")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to connect to NocoDB: {str(e)}")
                api_logger.error(f"Failed to connect to NocoDB: {str(e)}")
                return
            
            # Initialize tables
            if not self._init_tables():
                logger.error("Failed to initialize tables")
                api_logger.error("Failed to initialize tables")
                return
                
            logger.info("NocoDBStorage Plugin initialized successfully!")
            logger.info("="*80)
            api_logger.info("NocoDBStorage Plugin initialized successfully!")

        except Exception as e:
            error_msg = f"Failed to initialize NocoDB: {str(e)}"
            logger.error(error_msg)
            api_logger.error(error_msg)
            if logger.getEffectiveLevel() == logging.DEBUG:
                tb = traceback.format_exc()
                logger.error(f"Traceback: {tb}")
                api_logger.error(f"Traceback: {tb}")
            return

    def after_interact(self, user_id: str, username: str, interaction_type: str, success: bool):
        """Called after an interaction with a user"""
        if not self.enabled:
            return
            
        logger.info(f"after_interact called: user_id={user_id}, username={username}, type={interaction_type}, success={success}")
        try:
            # Get current session info from plugin
            session_id = getattr(self.plugin, 'session_id', '')
            job_name = getattr(self.plugin, 'job_name', '')
            target = getattr(self.plugin, 'target', '')
            
            data = {
                "user_id": user_id,
                "username": username,
                "interaction_at": datetime.now().isoformat(),
                "session_id": session_id,
                "job_name": job_name,
                "target": target,
                "interaction_type": interaction_type,
                "success": success
            }
            
            stored = self._store_interaction(data)
            if stored:
                logger.info(f"Successfully stored interaction with {username}")
            else:
                logger.error(f"Failed to store interaction with {username}")
                
        except Exception as e:
            logger.error(f"Error in after_interact: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")

    def after_filter(self, user_id: str, filter_type: str):
        """Called after a user is filtered"""
        if not self.enabled:
            return
            
        logger.info(f"after_filter called: user_id={user_id}, filter_type={filter_type}")
        try:
            stored = self._store_filter(user_id, filter_type)
            if stored:
                logger.info(f"Successfully stored filter for {user_id}")
            else:
                logger.error(f"Failed to store filter for {user_id}")
                
        except Exception as e:
            logger.error(f"Error in after_filter: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")

    def _sync_interacted_user(self, username, **kwargs):
        """Sync interacted user to NocoDB."""
        logger.debug(f"add_interacted_user_hook called with args: ({username!r},), kwargs: {kwargs}")
        
        data = {
            "user_id": username,  # Using username as user_id since we don't have actual user IDs
            "username": username,
            "interaction_at": datetime.now().isoformat(),
            "session_id": kwargs.get("session_id", ""),
            "job_name": kwargs.get("job_name", ""),
            "target": kwargs.get("target", ""),
            "followed": kwargs.get("followed", False),
            "is_requested": kwargs.get("is_requested", False),
            "scraped": kwargs.get("scraped", False),
            "liked_count": kwargs.get("liked", 0),
            "watched_count": kwargs.get("watched", 0),
            "commented_count": kwargs.get("commented", 0),
            "pm_sent": kwargs.get("pm_sent", False),
            "success": True  # If we got here, the interaction was successful
        }
        
        try:
            stored = self._store_interaction(data)
            if stored:
                logger.debug(f"Successfully synced interacted user {username}")
            else:
                logger.error(f"Failed to sync interacted user {username}")
        except Exception as e:
            logger.error(f"Error syncing interacted user {username}: {str(e)}")

    def _sync_filter_user(self, username, profile_data, skip_reason=None):
        """Sync filtered user to NocoDB"""
        try:
            # Get user_id from profile_data if available
            user_id = username
            if isinstance(profile_data, dict):
                user_id = profile_data.get("user_id", username)
            elif profile_data is not None:
                user_id = getattr(profile_data, "user_id", username)

            # Convert skip_reason to string
            filter_type = "unknown"
            if skip_reason is not None:
                if isinstance(skip_reason, str):
                    filter_type = skip_reason
                elif isinstance(skip_reason, dict) and "name" in skip_reason:
                    filter_type = skip_reason["name"]
                elif hasattr(skip_reason, "name"):
                    filter_type = skip_reason.name
                elif isinstance(skip_reason, list):
                    filter_type = ', '.join(skip_reason)
                else:
                    filter_type = str(skip_reason)

            stored = self._store_filter(user_id, filter_type)
            if stored:
                logger.debug(f"Successfully synced filtered user {username}")
            else:
                logger.error(f"Failed to sync filtered user {username}")
            
        except Exception as e:
            logger.error(f"Error syncing filtered user {username}: {str(e)}")

    def _get_interaction_type(self, followed, unfollowed, scraped, liked, watched, commented, pm_sent):
        """Determine the primary interaction type"""
        if followed:
            return "followed"
        elif unfollowed:
            return "unfollowed"
        elif scraped:
            return "scraped"
        elif liked > 0:
            return "liked"
        elif watched > 0:
            return "watched"
        elif commented > 0:
            return "commented"
        elif pm_sent:
            return "pm_sent"
        return "other"

    def load_config(self, config_path):
        """Load NocoDB configuration from YAML file."""
        try:
            logger.info(f"Loading NocoDB config from {config_path}")
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            logger.debug(f"Loaded config: {json.dumps(config, indent=2)}")
            
            # Validate required fields
            required_fields = ['base_url', 'api_token', 'base_id', 'tables']
            missing_fields = [f for f in required_fields if f not in config]
            if missing_fields:
                logger.error(f"Missing required fields in nocodb.yml: {missing_fields}")
                return False
                
            # Set up headers for API calls
            self.headers = {
                'xc-token': config['api_token'],
                'accept': 'application/json',
                'Content-Type': 'application/json'
            }
            
            # Test connection
            logger.info("Testing NocoDB connection...")
            test_url = f"{config['base_url']}/api/v1/db/meta/projects/{config['base_id']}/tables"
            response = requests.get(test_url, headers=self.headers)
            response.raise_for_status()
            logger.info("NocoDB connection successful!")
            
            # Store table configurations
            self.table_configs = config['tables']
            logger.debug(f"Table configs: {json.dumps(self.table_configs, indent=2)}")
            
            return config
            
        except FileNotFoundError:
            logger.error(f"Could not find NocoDB config file: {config_path}")
            return False
        except yaml.YAMLError as e:
            logger.error(f"Error parsing NocoDB config file: {str(e)}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to NocoDB: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error loading NocoDB config: {str(e)}")
            return False

    def _log_api_call(self, method: str, url: str, headers: Dict, data: Optional[Dict] = None, params: Optional[Dict] = None, response: Optional[requests.Response] = None):
        """Log API call details to the dedicated log file"""
        api_logger.debug(f"\n{'='*80}")
        api_logger.debug(f"API Call: {method} {url}")
        api_logger.debug(f"Headers: {headers}")
        if data:
            api_logger.debug(f"Request Data: {data}")
        if params:
            api_logger.debug(f"Request Params: {params}")
        if response:
            api_logger.debug(f"Response Status: {response.status_code}")
            api_logger.debug(f"Response Body: {response.text}")
        api_logger.debug(f"{'='*80}\n")

    def _get_tables(self):
        """Get list of existing tables from NocoDB."""
        try:
            api_logger.info("Fetching existing tables from NocoDB")
            url = f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            
            response = requests.get(url, headers=self.headers)
            log_api_call('GET', url, headers=self.headers, response=response)
            response.raise_for_status()
            
            tables = response.json()
            table_names = [table['table_name'] for table in tables.get('list', [])]
            api_logger.info(f"Found existing tables: {table_names}")
            return table_names
            
        except Exception as e:
            error_msg = f"Failed to get tables list: {str(e)}"
            logger.error(error_msg)
            api_logger.error(error_msg)
            if hasattr(e, 'response'):
                api_logger.error(f"Response status: {e.response.status_code}")
                api_logger.error(f"Response text: {e.response.text}")
            return None

    def _init_tables(self):
        """Initialize tables in NocoDB."""
        self.logger.info("================================================================================")
        self.logger.info("Initializing NocoDB Tables")
        self.logger.info("================================================================================")

        try:
            # Get existing tables
            url = f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            existing_tables = {table['title']: table for table in response.json()['list']}
            
            # Update table configs with table IDs and full table names
            for table_key, table_config in self.tables.items():
                table_name = table_config['name']
                if table_name in existing_tables:
                    self.logger.info(f"Table {table_name} already exists")
                    table_config['id'] = existing_tables[table_name]['id']
                    table_config['table_name'] = existing_tables[table_name]['table_name']
                else:
                    # Create table
                    table_data = {
                        'table_name': f'nc_o8xg___{table_name}',
                        'title': table_name,
                        'columns': [
                            {
                                'column_name': col['name'],
                                'title': col['name'],
                                'uidt': self._map_column_type(col['type']),
                                'dt': self._map_type_to_dt(col['type'])
                            }
                            for col in table_config['columns']
                        ]
                    }
                    
                    response = requests.post(url, headers=self.headers, json=table_data)
                    response.raise_for_status()
                    self.logger.info(f"Created table {table_name}")
                    
                    # Wait for table to be ready
                    retries = 10
                    while retries > 0:
                        time.sleep(0.5)
                        response = requests.get(url, headers=self.headers)
                        response.raise_for_status()
                        tables = {t['title']: t for t in response.json()['list']}
                        if table_name in tables:
                            table_config['id'] = tables[table_name]['id']
                            table_config['table_name'] = tables[table_name]['table_name']
                            self.logger.info(f"Table {table_name} is now ready")
                            break
                        retries -= 1
                    
                    if retries == 0:
                        raise TimeoutError(f"Timed out waiting for table {table_name} to be ready")
            
            # Set table IDs
            self.interacted_table_id = self.tables['interaction_history']['id']
            self.history_filters_table_id = self.tables['history_filters']['id']
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize tables: {str(e)}")
            return False

    def _create_table(self, table_name, columns):
        """Create a new table in NocoDB."""
        try:
            api_logger.info(f"Creating table {table_name}")
            api_logger.debug(f"Columns configuration: {json.dumps(columns, indent=2)}")
            
            # Prepare table creation payload
            payload = {
                "table_name": table_name,
                "title": table_name,
                "columns": columns
            }
            
            url = f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            api_logger.debug(f"POST request to: {url}")
            
            response = requests.post(url, headers=self.headers, json=payload)
            log_api_call('POST', url, headers=self.headers, data=payload, response=response)
            response.raise_for_status()
            
            api_logger.info(f"Successfully created table {table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Error creating table {table_name}: {str(e)}"
            logger.error(error_msg)
            api_logger.error(error_msg)
            if hasattr(e, 'response') and e.response is not None:
                api_logger.error(f"Response: {e.response.text}")
            return False

    def _get_table_columns(self, table_name):
        """Get columns of a table from NocoDB."""
        try:
            api_logger.info(f"Getting columns for table {table_name}")
            url = f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables/{table_name}/columns"
            
            response = requests.get(url, headers=self.headers)
            log_api_call('GET', url, headers=self.headers, response=response)
            response.raise_for_status()
            
            columns = response.json()
            api_logger.debug(f"Retrieved columns: {json.dumps(columns.get('list', []), indent=2)}")
            return columns.get('list', [])
            
        except Exception as e:
            error_msg = f"Error getting table columns: {str(e)}"
            logger.error(error_msg)
            api_logger.error(error_msg)
            if hasattr(e, 'response'):
                api_logger.error(f"Response: {e.response.text}")
            return None

    def _verify_table_schema(self, table_name, current_columns, expected_columns):
        """Verify that a table has the expected schema."""
        try:
            api_logger.info(f"Verifying schema for table {table_name}")
            api_logger.debug(f"Current columns: {json.dumps(current_columns, indent=2)}")
            api_logger.debug(f"Expected columns: {json.dumps(expected_columns, indent=2)}")
            
            # Create sets of column names for comparison
            current_cols = {col['title'].lower() for col in current_columns}
            expected_cols = {col['title'].lower() for col in expected_columns}
            
            # Check for missing columns
            missing_cols = expected_cols - current_cols
            if missing_cols:
                api_logger.warning(f"Missing columns in {table_name}: {missing_cols}")
                return False
                
            # Check column types
            for expected_col in expected_columns:
                current_col = next((c for c in current_columns if c['title'].lower() == expected_col['title'].lower()), None)
                if current_col and current_col.get('uidt') != expected_col.get('uidt'):
                    api_logger.warning(f"Column type mismatch in {table_name}.{expected_col['title']}: "
                                     f"expected {expected_col.get('uidt')}, got {current_col.get('uidt')}")
                    return False
            
            api_logger.info(f"Schema verification successful for table {table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Error verifying table schema: {str(e)}"
            logger.error(error_msg)
            api_logger.error(error_msg)
            api_logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def store_filtered_user(self, user_id: str, filter_type: str) -> bool:
        """Store a filtered user in NocoDB"""
        if not self.enabled:
            return False

        stored = self._store_filter(user_id, filter_type)
        if stored:
            logger.info(f"NocoDB: Successfully stored filter for user {user_id}")
            api_logger.info(f"NocoDB: Successfully stored filter for user {user_id}")
            return True
        else:
            logger.warning(f"NocoDB: Failed to store filter for user {user_id}")
            api_logger.warning(f"NocoDB: Failed to store filter for user {user_id}")
            return False

    def update_history_file(self, username, path_to_file):
        if not self.enabled:
            return
            
        try:
            with open(path_to_file, 'r') as f:
                history = yaml.safe_load(f)
                
            if path_to_file.endswith('filtered_users.json'):
                for user_id, filter_data in history.items():
                    self.store_filtered_user(user_id, filter_data.get('filter_type', 'unknown'))
                    
            elif path_to_file.endswith('interacted_users.json'):
                for user_id, interaction_data in history.items():
                    self.store_interaction(
                        user_id,
                        interaction_data.get('type', 'unknown'),
                        interaction_data.get('success', False)
                    )
                    
        except Exception as e:
            logger.error(f"Failed to process history file {path_to_file}: {str(e)}")
            api_logger.error(f"Failed to process history file {path_to_file}: {str(e)}")

    def before_job(self, plugin, plugin_name):
        """Initialize NocoDB before each job"""
        if self.args.enable_nocodb:
            logger.info("NocoDB: Initializing storage")
            api_logger.info("NocoDB: Initializing storage")
            config_path = os.path.join('accounts', 'quecreate', 'nocodb.yml')
            
            # Check if config file exists
            if not os.path.exists(config_path):
                logger.error(f"NocoDB: Config file not found at {config_path}")
                api_logger.error(f"NocoDB: Config file not found at {config_path}")
                return
                
            # Load config and initialize
            success = self.load_config(config_path)
            if success:
                logger.info("NocoDB: Successfully initialized")
                api_logger.info("NocoDB: Successfully initialized")
                logger.debug(f"NocoDB: Using base URL: {self.base_url}")
                api_logger.debug(f"NocoDB: Using base URL: {self.base_url}")
            else:
                logger.warning("NocoDB: Failed to initialize")
                api_logger.warning("NocoDB: Failed to initialize")

    def _handle_follow(self, user_id, username, success=True):
        """Handle follow event"""
        logger.info(f"Follow event: user_id={user_id}, username={username}, success={success}")
        api_logger.info(f"Follow event: user_id={user_id}, username={username}, success={success}")
        self._store_interaction({
            "user_id": user_id,
            "username": username,
            "interaction_at": datetime.now().isoformat(),
            "interaction_type": "follow",
            "followed": True,
            "success": success
        })

    def _handle_like(self, user_id, username, count=1, success=True):
        """Handle like event"""
        logger.info(f"Like event: user_id={user_id}, username={username}, count={count}, success={success}")
        api_logger.info(f"Like event: user_id={user_id}, username={username}, count={count}, success={success}")
        self._store_interaction({
            "user_id": user_id,
            "username": username,
            "interaction_at": datetime.now().isoformat(),
            "interaction_type": "like",
            "liked_count": count,
            "success": success
        })

    def _handle_watch(self, user_id, username, count=1, success=True):
        """Handle watch event"""
        logger.info(f"Watch event: user_id={user_id}, username={username}, count={count}, success={success}")
        api_logger.info(f"Watch event: user_id={user_id}, username={username}, count={count}, success={success}")
        self._store_interaction({
            "user_id": user_id,
            "username": username,
            "interaction_at": datetime.now().isoformat(),
            "interaction_type": "watch",
            "watched_count": count,
            "success": success
        })

    def _handle_comment(self, user_id, username, count=1, success=True):
        """Handle comment event"""
        logger.info(f"Comment event: user_id={user_id}, username={username}, count={count}, success={success}")
        api_logger.info(f"Comment event: user_id={user_id}, username={username}, count={count}, success={success}")
        self._store_interaction({
            "user_id": user_id,
            "username": username,
            "interaction_at": datetime.now().isoformat(),
            "interaction_type": "comment",
            "commented_count": count,
            "success": success
        })

    def _handle_pm(self, user_id, username, success=True):
        """Handle PM event"""
        logger.info(f"PM event: user_id={user_id}, username={username}, success={success}")
        api_logger.info(f"PM event: user_id={user_id}, username={username}, success={success}")
        self._store_interaction({
            "user_id": user_id,
            "username": username,
            "interaction_at": datetime.now().isoformat(),
            "interaction_type": "pm",
            "pm_sent": True,
            "success": success
        })

    def _map_column_type(self, type_str):
        """Map Python types to NocoDB UI data types (uidt)."""
        type_map = {
            'string': 'SingleLineText',
            'datetime': 'DateTime',
            'boolean': 'Checkbox',
            'integer': 'Number'
        }
        return type_map.get(type_str, 'SingleLineText')

    def _map_type_to_dt(self, type_str):
        """Map Python types to NocoDB database types (dt)."""
        type_map = {
            'string': 'varchar',
            'datetime': 'timestamp',
            'boolean': 'boolean',
            'integer': 'integer'
        }
        return type_map.get(type_str, 'varchar')

    def _store_interaction(self, interaction_data):
        """Store interaction data in NocoDB."""
        try:
            table_config = self.tables['interaction_history']
            if 'id' not in table_config or 'table_name' not in table_config:
                raise ValueError("Table ID or name not found. Make sure tables are initialized.")
            
            url = f"{self.base_url}/api/v2/tables/{table_config['id']}/records"
            response = requests.post(url, headers=self.headers, json=interaction_data)
            response.raise_for_status()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store interaction: {str(e)}")
            return False

    def _store_filter(self, user_id: str, filter_type: str):
        """Store filter data in NocoDB."""
        try:
            table_config = self.tables['history_filters']
            if 'id' not in table_config or 'table_name' not in table_config:
                raise ValueError("Table ID or name not found. Make sure tables are initialized.")
            
            filter_data = {
                self.COLUMN_USER_ID: user_id,
                self.COLUMN_FILTERED_AT: datetime.now().isoformat(),
                self.COLUMN_FILTER_TYPE: filter_type
            }
            
            url = f"{self.base_url}/api/v2/tables/{table_config['id']}/records"
            response = requests.post(url, headers=self.headers, json=filter_data)
            response.raise_for_status()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store filter: {str(e)}")
            return False

    def _get_table_id(self, table_name):
        """Get the table ID for a given table name."""
        try:
            # Check cache first
            if table_name in self.table_ids:
                return self.table_ids[table_name]

            url = f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            tables = data.get('list', [])
            
            logger.debug(f"Available tables: {json.dumps(data, indent=2)}")

            # Get the actual table name from config
            actual_table_name = None
            if table_name == "interacted_users":
                actual_table_name = self.config['tables']['interaction_history']['name']
            elif table_name == "history_filters_users":
                actual_table_name = self.config['tables']['history_filters']['name']
            else:
                actual_table_name = table_name

            logger.debug(f"Looking for table: {actual_table_name}")
            
            # Try to find the table ID by title
            for table in tables:
                logger.debug(f"Checking table: {table.get('title')} (ID: {table.get('id')})")
                if isinstance(table, dict) and table.get('title') == actual_table_name:
                    table_id = table.get('id')
                    if table_id:
                        self.table_ids[table_name] = table_id  # Cache the ID
                        return table_id

            raise ValueError(f"Table {actual_table_name} not found")
        except Exception as e:
            logger.error(f"Failed to get table ID: {str(e)}")
            raise

    def get_user_interactions(self, user_id: str) -> list:
        """Get interaction records for a specific user.

        Args:
            user_id: User ID to get interactions for

        Returns:
            List of interaction records for the user
        """
        try:
            params = {}
            if user_id:
                params["where"] = self._build_filter("user_id", "eq", user_id)
                
            return self._get_records(self.interacted_table_id, params)

        except Exception as e:
            logger.error(f"Failed to get user interactions: {str(e)}")
            return None

    def get_filtered_user(self, user_id: str) -> list:
        """Get filter history records for a specific user.

        Args:
            user_id: User ID to get filter history for

        Returns:
            List of filter history records for the user
        """
        try:
            params = {}
            if user_id:
                params["where"] = self._build_filter(self.COLUMN_USER_ID, "eq", user_id)
                
            filters = self._get_records(self.history_filters_table_id, params)
            if filters:
                logger.debug(f"Found {len(filters)} filter records for user {user_id}")
            else:
                logger.debug(f"No filter records found for user {user_id}")
                
            return filters
            
        except Exception as e:
            logger.error(f"Failed to get filtered user: {str(e)}")
            return None

    def _build_filter(self, field: str, op: str, value: str) -> str:
        """Build a filter string in NocoDB v2 API format.
        
        Args:
            field: Field name to filter on
            op: Operator (eq, like, in, etc.)
            value: Value to filter for
            
        Returns:
            Filter string in format (field,op,value)
        """
        return f"({field},{op},{value})"

    def _build_and_filter(self, filters: list) -> str:
        """Build an AND filter from multiple conditions.
        
        Args:
            filters: List of filter strings from _build_filter()
            
        Returns:
            Combined filter string with ~and between conditions
        """
        return "~and".join(filters)

    def get_filtered_users(self, filter_type: str = None, user_id: str = None) -> list:
        """Get filtered users from history with optional filters.
        
        Args:
            filter_type: Optional filter type to filter by
            user_id: Optional user ID to filter by
            
        Returns:
            List of filtered user records
        """
        filters = []
        
        if filter_type:
            filters.append(self._build_filter(self.COLUMN_FILTER_TYPE, "eq", filter_type))
            
        if user_id:
            filters.append(self._build_filter(self.COLUMN_USER_ID, "eq", user_id))
            
        params = {}
        if filters:
            params["where"] = self._build_and_filter(filters)
            
        return self._get_records(self.history_filters_table_id, params)

    def get_interacted_users(self, user_id: str = None) -> list:
        """Get interacted users with optional user_id filter.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of interacted user records
        """
        params = {}
        if user_id:
            params["where"] = self._build_filter("user_id", "eq", user_id)
            
        return self._get_records(self.interacted_table_id, params)

    def _get_records(self, table_id: str, params: dict = None) -> list:
        """Get records from a table with optional parameters.
        
        Args:
            table_id: Table ID to get records from
            params: Optional query parameters
            
        Returns:
            List of records from the table
        """
        try:
            url = f"{self.base_url}/api/v2/tables/{table_id}/records"
            if not params:
                params = {}
            if 'limit' not in params:
                params['limit'] = 25
            if 'offset' not in params:
                params['offset'] = 0
                
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('list', [])
            
        except Exception as e:
            logger.error(f"Failed to get records: {str(e)}")
            raise
