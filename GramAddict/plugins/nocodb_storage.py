"""Store Instagram interaction history in NocoDB cloud database."""

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
import jwt
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

# Set up NocoDB operations logger
nocodb_logger = logging.getLogger('nocodb_operations')
nocodb_logger.setLevel(logging.DEBUG)

# Use absolute path for log file
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'nocodb_operations.log')

# Add file handler with debug level
fh = logging.FileHandler(log_file, mode='w')  # Changed to 'w' mode to ensure we can write
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
nocodb_logger.addHandler(fh)

# Add console handler for immediate feedback
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
nocodb_logger.addHandler(ch)

nocodb_logger.debug("NocoDB logger initialized")  # Added debug message

class NocoDBStorage(Plugin):
    """Store interaction data in NocoDB"""

    def __init__(self):
        super().__init__()
        self.description = "Store interaction data in NocoDB"
        self.arguments = [
            {
                "arg": "--use-nocodb",
                "help": "store interaction data in NocoDB",
                "nargs": None,
                "metavar": None,
                "default": False,
                "action": "store_true",
                "operation": True
            }
        ]
        
        # These will be set when config is loaded
        self.base_url = None
        self.api_token = None
        self.base_id = None
        self.headers = None
        self.config = None
        self.enabled = False
        self.token_refresh_attempts = 0
        self.max_token_refresh_attempts = 3
        
        # Define default table schemas
        self.table_schemas = {
            "interacted_users": {
                "table_name": "interacted_users",
                "title": "Interacted Users",
                "columns": [
                    {"column_name": "User Id", "title": "User Id", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Username", "title": "Username", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Full Name", "title": "Full Name", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Profile URL", "title": "Profile URL", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Interaction Type", "title": "Interaction Type", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Success", "title": "Success", "uidt": "Checkbox", "dt": "boolean"},
                    {"column_name": "Timestamp", "title": "Timestamp", "uidt": "DateTime", "dt": "datetime"},
                    {"column_name": "Session ID", "title": "Session ID", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Job Name", "title": "Job Name", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Target", "title": "Target", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Session Start Time", "title": "Session Start Time", "uidt": "DateTime", "dt": "datetime"},
                    {"column_name": "Session End Time", "title": "Session End Time", "uidt": "DateTime", "dt": "datetime"}
                ]
            },
            "history_filters_users": {
                "table_name": "history_filters_users", 
                "title": "History Filters Users",
                "columns": [
                    {"column_name": "User Id", "title": "User Id", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Username", "title": "Username", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Filtered At", "title": "Filtered At", "uidt": "DateTime", "dt": "datetime"},
                    {"column_name": "Filter Type", "title": "Filter Type", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Session ID", "title": "Session ID", "uidt": "SingleLineText", "dt": "varchar"},
                    {"column_name": "Session Start Time", "title": "Session Start Time", "uidt": "DateTime", "dt": "datetime"},
                    {"column_name": "Session End Time", "title": "Session End Time", "uidt": "DateTime", "dt": "datetime"}
                ]
            }
        }

    def run(self, device, configs, storage, sessions, filters, plugin_name):
        """Initialize NocoDB storage"""
        nocodb_logger.info("Initializing NocoDB storage...")
        
        # Set enabled flag based on --use-nocodb argument
        self.enabled = configs.args.use_nocodb
        if not self.enabled:
            nocodb_logger.info("NocoDB storage disabled - --use-nocodb flag not set")
            return
            
        try:
            nocodb_logger.info("Loading NocoDB configuration...")
            config_path = os.path.join('accounts', configs.username, 'nocodb.yml')
            nocodb_logger.debug(f"Looking for config at: {os.path.abspath(config_path)}")
            
            if not os.path.exists(config_path):
                nocodb_logger.error(f"NocoDB config file not found at {config_path}")
                self.enabled = False
                return
            
            self.config = self.load_config(config_path)
            if not self.config:
                nocodb_logger.error("Failed to load NocoDB configuration")
                self.enabled = False
                return
                
            # Validate required config fields
            required_fields = ['base_url', 'api_token', 'base_id']
            for field in required_fields:
                if field not in self.config:
                    nocodb_logger.error(f"Missing required field '{field}' in NocoDB config")
                    self.enabled = False
                    return
            
            # Set up connection parameters
            self.base_url = self.config["base_url"].rstrip('/')  # Remove trailing slash if present
            self.api_token = self.config["api_token"]
            self.base_id = self.config["base_id"]
            self.headers = {
                "xc-token": self.api_token,
                "accept": "application/json",
                "Content-Type": "application/json"
            }
            
            # Load table schemas from config
            self.table_schemas = self.config.get("table_schemas", self.table_schemas)
            
            # Test connection using v1 API
            nocodb_logger.info("Testing NocoDB connection...")
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/health",
                timeout=10
            )
            if not response or response.status_code != 200:
                nocodb_logger.error(f"Failed to connect to NocoDB: {response.status_code if response else 'No response'}")
                self.enabled = False
                return

            # Verify project exists
            nocodb_logger.info(f"Verifying project {self.base_id}...")
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}",
                timeout=10
            )
            if not response or response.status_code != 200:
                nocodb_logger.error(f"Project {self.base_id} not found or not accessible")
                self.enabled = False
                return

            nocodb_logger.info("NocoDB connection successful!")
            
            # Initialize tables if --init-db flag is set
            if configs.args.init_db:
                nocodb_logger.info("Initializing tables...")
                if not self.init_tables():
                    nocodb_logger.error("Failed to initialize tables")
                    self.enabled = False
                    return
            
            # Set nocodb reference in storage
            if storage is not None:
                storage.nocodb = self
                logger.info("NocoDB reference set in storage")
            else:
                logger.error("Storage object is None, cannot set NocoDB reference")
                self.enabled = False
                return
            
            nocodb_logger.info("NocoDB storage initialized successfully!")
            
        except Exception as e:
            nocodb_logger.error(f"Failed to initialize NocoDB storage: {str(e)}")
            nocodb_logger.debug(f"Traceback: {traceback.format_exc()}")
            self.enabled = False
                
    def load_config(self, config_path):
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

    def init_tables(self):
        """Initialize NocoDB tables."""
        nocodb_logger.info("NocoDB: Initializing tables...")
        try:
            # Check if project exists
            nocodb_logger.debug(f"NocoDB: Checking project {self.base_id}")
            response = self._make_request(
                "get",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}"
            )
            
            if not response or response.status_code != 200:
                nocodb_logger.error(f"NocoDB: Project {self.base_id} not found. Status code: {response.status_code if response else 'No response'}")
                return False
                
            nocodb_logger.info(f"NocoDB: Project {self.base_id} exists")
            
            # Get existing tables
            nocodb_logger.debug("NocoDB: Getting existing tables")
            response = self._make_request(
                "get",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            
            if not response or response.status_code != 200:
                nocodb_logger.error(f"NocoDB: Failed to get existing tables. Status code: {response.status_code if response else 'No response'}")
                return False
                
            existing_tables = response.json().get('list', [])
            existing_table_titles = [t['title'].lower() for t in existing_tables]
            nocodb_logger.debug(f"NocoDB: Found existing tables: {existing_table_titles}")
            
            # Create tables if they don't exist
            for schema in self.table_schemas.values():
                try:
                    nocodb_logger.debug(f"NocoDB: Checking table {schema['title']}")
                    if schema['title'].lower() not in existing_table_titles:
                        nocodb_logger.info(f"NocoDB: Creating table {schema['title']}")
                        success = self.create_table(schema)
                        if not success:
                            nocodb_logger.error(f"NocoDB: Failed to create table {schema['title']}")
                            return False
                        nocodb_logger.info(f"NocoDB: Table {schema['title']} created successfully")
                    else:
                        nocodb_logger.info(f"NocoDB: Table {schema['title']} already exists")
                        
                except Exception as e:
                    nocodb_logger.error(f"NocoDB: Failed to check/create table {schema['title']}: {str(e)}")
                    nocodb_logger.debug(f"NocoDB: Traceback: {traceback.format_exc()}")
                    return False
                    
            nocodb_logger.info("NocoDB: Tables initialized successfully")
            return True
            
        except Exception as e:
            nocodb_logger.error(f"NocoDB: Failed to initialize tables: {str(e)}")
            nocodb_logger.debug(f"NocoDB: Traceback: {traceback.format_exc()}")
            return False

    def create_table(self, schema):
        """Create a new table with the given schema"""
        try:
            nocodb_logger.debug(f"NocoDB: Creating table with schema: {schema}")
            
            # Create table data structure
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
            
            # Make API request to create table
            response = self._make_request(
                "post",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables",
                json=table_data,
                timeout=30  # Increased timeout for table creation
            )
            
            if not response or response.status_code not in [200, 201]:
                nocodb_logger.error(f"NocoDB: Failed to create table {schema['title']}. Status code: {response.status_code if response else 'No response'}")
                if response and response.text:
                    nocodb_logger.debug(f"NocoDB: Error response: {response.text}")
                return False
                
            nocodb_logger.info(f"NocoDB: Created table {schema['title']}")
            
            # Verify table was created
            response = self._make_request(
                "get",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            
            if not response or response.status_code != 200:
                nocodb_logger.error(f"NocoDB: Failed to verify table creation. Status code: {response.status_code if response else 'No response'}")
                return False
                
            existing_tables = response.json().get('list', [])
            if not any(t['title'].lower() == schema['title'].lower() for t in existing_tables):
                nocodb_logger.error(f"NocoDB: Table {schema['title']} not found after creation")
                return False
                
            nocodb_logger.info(f"NocoDB: Verified table {schema['title']} exists")
            return True
            
        except Exception as e:
            nocodb_logger.error(f"NocoDB: Failed to create table {schema['title']}: {str(e)}")
            nocodb_logger.debug(f"NocoDB: Traceback: {traceback.format_exc()}")
            return False

    def verify_tables(self) -> bool:
        """Verify that the necessary tables exist in NocoDB."""
        nocodb_logger.info("Verifying NocoDB tables...")
        try:
            response = self._make_request(
                "get",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            
            if not response:
                nocodb_logger.error("Failed to get tables list")
                return False

            # Check if response is JSON
            if "application/json" not in response.headers.get("Content-Type", ""):
                nocodb_logger.error("Unexpected content type: " + response.headers.get("Content-Type", ""))
                nocodb_logger.debug("Response content: " + response.text)
                return False

            tables_list = response.json().get('list', [])
            nocodb_logger.debug(f"Found tables: {[t['title'] for t in tables_list]}")

            existing_tables = {table["title"]: table for table in tables_list}

            # Initialize tables if they don't exist
            for table_name, table_config in self.table_schemas.items():
                if table_config["title"] not in existing_tables:
                    nocodb_logger.info(f"Table {table_config['title']} not found, creating...")
                    success = self.create_table(table_config)
                    if not success:
                        nocodb_logger.error(f"Failed to create table {table_config['title']}")
                        return False

            nocodb_logger.info("All necessary tables verified/created")
            return True

        except Exception as e:
            nocodb_logger.error(f"Error verifying tables: {str(e)}")
            nocodb_logger.debug(f"Traceback: {traceback.format_exc()}")
            return False

    def validate_token(self) -> Tuple[bool, str]:
        """Validate the current API token.
        
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        if not self.api_token:
            return False, "No API token configured"
            
        try:
            # Try to decode the token to check expiration
            # Note: This only works for JWT tokens, not API tokens
            try:
                decoded = jwt.decode(self.api_token, options={"verify_signature": False})
                exp = decoded.get('exp')
                if exp and datetime.fromtimestamp(exp) < datetime.now():
                    return False, "Token has expired"
            except jwt.InvalidTokenError:
                # If token is not a JWT, it's likely an API token which doesn't expire
                pass
                
            # Verify token by making a test API call
            response = requests.get(
                f"{self.base_url}/api/v1/health",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 401:
                return False, "Token is invalid or expired"
            elif response.status_code == 403:
                return False, "Token lacks necessary permissions"
            
            response.raise_for_status()
            return True, ""
            
        except requests.exceptions.RequestException as e:
            return False, f"Error validating token: {str(e)}"

    def refresh_token(self) -> bool:
        """Attempt to refresh the API token.
        
        Returns:
            bool: True if token was refreshed successfully
        """
        if self.token_refresh_attempts >= self.max_token_refresh_attempts:
            nocodb_logger.error("Maximum token refresh attempts reached")
            return False
            
        try:
            self.token_refresh_attempts += 1
            
            # Load fresh token from config
            config_path = os.path.join('accounts', self.username, 'nocodb.yml')
            fresh_config = self.load_config(config_path)
            
            if not fresh_config or 'api_token' not in fresh_config:
                nocodb_logger.error("Could not load fresh token from config")
                return False
                
            # Update token
            self.api_token = fresh_config['api_token']
            self.headers = {
                "xc-token": self.api_token,
                "accept": "application/json",
                "Content-Type": "application/json"
            }
            
            # Validate new token
            is_valid, error = self.validate_token()
            if not is_valid:
                nocodb_logger.error(f"Fresh token is invalid: {error}")
                return False
                
            nocodb_logger.info("Successfully refreshed API token")
            return True
            
        except Exception as e:
            nocodb_logger.error(f"Error refreshing token: {str(e)}")
            return False

    def handle_auth_error(self, error: str) -> bool:
        """Handle authentication errors by attempting to refresh token.
        
        Args:
            error: Error message from failed request
            
        Returns:
            bool: True if error was handled and operation should be retried
        """
        if "Invalid token" in error or "Token expired" in error or "Authentication failed" in error:
            nocodb_logger.warning("Authentication error detected, attempting to refresh token")
            return self.refresh_token()
        return False

    def _make_request(self, method, url, **kwargs):
        """Make a request to the NocoDB API with error handling and retries"""
        try:
            # Add default timeout if not specified
            if 'timeout' not in kwargs:
                kwargs['timeout'] = 10
                
            # Add headers if not specified
            if 'headers' not in kwargs:
                kwargs['headers'] = self.headers
                
            # Add retry logic for token refresh
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    nocodb_logger.debug(f"NocoDB: Making {method.upper()} request to {url}")
                    response = requests.request(method, url, **kwargs)
                    
                    # Handle different status codes
                    if response.status_code == 401:  # Unauthorized
                        nocodb_logger.warning("NocoDB: Token expired, attempting refresh...")
                        if self.refresh_token():
                            kwargs['headers'] = self.headers  # Use new token
                            retry_count += 1
                            continue
                        else:
                            nocodb_logger.error("NocoDB: Token refresh failed")
                            return None
                            
                    elif response.status_code == 429:  # Rate limit
                        retry_after = int(response.headers.get('Retry-After', 5))
                        nocodb_logger.warning(f"NocoDB: Rate limited, waiting {retry_after} seconds...")
                        time.sleep(retry_after)
                        retry_count += 1
                        continue
                        
                    elif response.status_code >= 500:  # Server error
                        if retry_count < max_retries - 1:
                            wait_time = (retry_count + 1) * 2
                            nocodb_logger.warning(f"NocoDB: Server error, retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                            retry_count += 1
                            continue
                            
                    # Log response details for debugging
                    if response.status_code not in [200, 201]:
                        nocodb_logger.debug(f"NocoDB: Response status code: {response.status_code}")
                        nocodb_logger.debug(f"NocoDB: Response headers: {response.headers}")
                        nocodb_logger.debug(f"NocoDB: Response body: {response.text[:1000]}")  # Truncate large responses
                        
                    return response
                    
                except requests.exceptions.Timeout:
                    nocodb_logger.error(f"NocoDB: Request timed out: {url}")
                    return None
                except requests.exceptions.ConnectionError:
                    if retry_count < max_retries - 1:
                        wait_time = (retry_count + 1) * 2
                        nocodb_logger.warning(f"NocoDB: Connection error, retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                    nocodb_logger.error(f"NocoDB: Connection failed after {max_retries} attempts: {url}")
                    return None
                    
            return None
            
        except Exception as e:
            nocodb_logger.error(f"NocoDB: Request failed: {str(e)}")
            nocodb_logger.debug(f"NocoDB: Traceback: {traceback.format_exc()}")
            return None

    def after_interact(self, user_id: str, username: str, interaction_type: str, success: bool):
        """Called after an interaction with a user"""
        if not self.enabled:
            nocodb_logger.debug("NocoDB storage disabled - skipping interaction")
            return
            
        try:
            nocodb_logger.info(f"NocoDB: Recording interaction for user {username} (type={interaction_type}, success={success})")
            
            # Get user metadata if possible
            user_metadata = {}
            try:
                from GramAddict.core.device_facade import DeviceFacade
                device = DeviceFacade()
                user_metadata = device.get_user_metadata(username)
                nocodb_logger.debug(f"NocoDB: Got user metadata: {user_metadata}")
            except Exception as e:
                nocodb_logger.warning(f"NocoDB: Could not get user metadata: {str(e)}")
            
            # Get session info
            session_info = {}
            try:
                from GramAddict.core.session_state import SessionState
                session = SessionState()
                session_info = {
                    "session_id": session.session_id,
                    "job_name": session.job_name,
                    "target": session.target,
                    "session_start": session.session_start.isoformat() if session.session_start else None,
                    "session_end": session.session_end.isoformat() if session.session_end else None
                }
                nocodb_logger.debug(f"NocoDB: Got session info: {session_info}")
            except Exception as e:
                nocodb_logger.warning(f"NocoDB: Could not get session info: {str(e)}")
            
            # Prepare interaction data
            interaction_data = {
                "User Id": user_id,
                "Username": username,
                "Full Name": user_metadata.get("full_name", None),
                "Profile URL": f"https://www.instagram.com/{username}/",
                "Interaction Type": interaction_type,
                "Success": success,
                "Timestamp": datetime.now().isoformat(),
                "Session ID": session_info.get("session_id", None),
                "Job Name": session_info.get("job_name", None),
                "Target": session_info.get("target", None),
                "Session Start Time": session_info.get("session_start", None),
                "Session End Time": session_info.get("session_end", None)
            }
            
            nocodb_logger.debug(f"NocoDB: Prepared interaction data: {interaction_data}")
            
            # Store interaction with retries
            max_retries = 3
            retry_delay = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    nocodb_logger.debug(f"NocoDB: Attempting to store interaction (attempt {attempt + 1}/{max_retries})")
                    if self._store_interaction(interaction_data):
                        nocodb_logger.info(f"NocoDB: Successfully recorded interaction for {username}")
                        return
                    else:
                        nocodb_logger.warning(f"NocoDB: Failed to store interaction for {username}, attempt {attempt + 1}/{max_retries}")
                        if attempt < max_retries - 1:
                            nocodb_logger.debug(f"NocoDB: Waiting {retry_delay}s before retry")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                            
                except Exception as e:
                    nocodb_logger.error(f"NocoDB: Error storing interaction on attempt {attempt + 1}: {str(e)}")
                    if attempt < max_retries - 1:
                        nocodb_logger.debug(f"NocoDB: Waiting {retry_delay}s before retry")
                        time.sleep(retry_delay)
                        retry_delay *= 2
            
            nocodb_logger.error(f"NocoDB: Failed to store interaction for {username} after {max_retries} attempts")
            
        except Exception as e:
            nocodb_logger.error(f"NocoDB: Error in after_interact: {str(e)}")
            if logger.getEffectiveLevel() == logging.DEBUG:
                nocodb_logger.error(f"NocoDB: Traceback: {traceback.format_exc()}")

    def store_interaction(self, user_id: str, username: str, interaction_type: str, success: bool = True, profile_data: Optional[Dict] = None) -> bool:
        """Store an interaction in NocoDB."""
        nocodb_logger.debug(f"Entering store_interaction with user_id={user_id}, username={username}, interaction_type={interaction_type}, success={success}")
        if not self.enabled:
            nocodb_logger.warning("NocoDB storage is disabled - skipping interaction storage")
            return False

        try:
            nocodb_logger.info(f"Storing interaction for user {username} ({user_id}) - Type: {interaction_type}, Success: {success}")
            
            # Prepare interaction data
            interaction_data = {
                "User Id": user_id,
                "Username": username,
                "Full Name": profile_data.get("full_name", "") if profile_data else "",
                "Profile URL": f"https://www.instagram.com/{username}/",
                "Interaction Type": interaction_type,
                "Success": success,
                "Timestamp": datetime.now().isoformat()
            }
            nocodb_logger.debug(f"Prepared interaction data: {interaction_data}")
            
            table_config = self.table_schemas.get("interacted_users")
            if not table_config:
                nocodb_logger.error("Table schema for interacted_users not found")
                return False
                
            # Get table ID
            nocodb_logger.debug("Getting table ID...")
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            if not response:
                return False
                
            tables = response.json().get('list', [])
            nocodb_logger.debug(f"Found tables: {[t['title'] for t in tables]}")
            
            table_id = None
            for table in tables:
                if table['title'] == table_config['title']:
                    table_id = table['id']
                    nocodb_logger.debug(f"Found table ID: {table_id}")
                    break
                    
            if not table_id:
                nocodb_logger.error(f"Table {table_config['title']} not found")
                return False
                
            # Store the interaction
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}"
            nocodb_logger.debug(f"Storing interaction data at: {url}")
            nocodb_logger.debug(f"Headers: {self.headers}")
            nocodb_logger.debug(f"Interaction data: {interaction_data}")
            
            response = self._make_request(
                'post',
                url,
                json=interaction_data,
                timeout=10
            )
            if not response:
                return False
                
            # Verify record was stored
            record_id = response.json().get("id")
            if not record_id:
                nocodb_logger.error("Failed to get record ID from response")
                nocodb_logger.debug(f"Response: {response.json()}")
                return False
                
            # Verify record exists
            verify_url = f"{url}/{record_id}"
            verify_response = self._make_request(
                'get',
                verify_url,
                timeout=10
            )
            if not verify_response:
                return False
                
            stored_data = verify_response.json()
            
            # Check if all fields were stored correctly
            for field, value in interaction_data.items():
                if str(stored_data.get(field)) != str(value):
                    nocodb_logger.error(f"Field mismatch in stored record - {field}: expected '{value}', got '{stored_data.get(field)}'")
                    return False
                    
            nocodb_logger.info(f"Successfully stored and verified interaction for user {username}")
            nocodb_logger.debug(f"Stored record: {stored_data}")
            return True
            
        except requests.exceptions.RequestException as e:
            nocodb_logger.error(f"Request error storing interaction: {str(e)}")
            nocodb_logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
        except Exception as e:
            nocodb_logger.error(f"Error storing interaction: {str(e)}")
            nocodb_logger.debug(f"Traceback: {traceback.format_exc()}")
            return False

    def _store_interaction(self, interaction_data):
        """Store interaction data in NocoDB."""
        try:
            logger.info(f"=== Storing Filter Record for {interaction_data.get('Username')} ===")
            logger.info(f"Filter Type: {interaction_data.get('Interaction Type')}")
            
            # Get table ID for interacted_users
            logger.debug("Getting table ID...")
            response = self._make_request(
                "get",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            
            if not response:
                logger.error("Failed to get tables list")
                return False
                
            tables = response.json().get('list', [])
            logger.debug(f"Found tables: {[t['title'] for t in tables]}")
            
            table_id = None
            for table in tables:
                if table['title'] == 'Interacted Users':
                    table_id = table['id']
                    break
                    
            if not table_id:
                logger.error("Could not find table ID for Interacted Users")
                return False
                
            logger.debug(f"Found table ID: {table_id} for Interacted Users")
            
            # Store interaction
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}"
            logger.debug(f"Storing interaction at: {url}")
            logger.debug(f"Headers: {self.headers}")
            logger.debug(f"Interaction data: {interaction_data}")
            
            response = self._make_request(
                "post",
                url,
                json=interaction_data
            )
            
            if not response:
                logger.error("Failed to store interaction")
                return False
                
            response_data = response.json()
            
            # Consider the operation successful if we get a response with the data back
            if isinstance(response_data, dict):
                # Check if at least User Id and Filter Type match what we sent
                if (response_data.get('User Id') == interaction_data['User Id'] and 
                    response_data.get('Interaction Type') == interaction_data['Interaction Type']):
                    logger.info("Successfully stored interaction")
                    return True
                else:
                    logger.error("Failed to store interaction - response data mismatch")
                    logger.debug(f"Expected: {interaction_data}")
                    logger.debug(f"Got: {response_data}")
                    return False
            else:
                logger.error("Failed to store interaction - unexpected response format")
                logger.debug(f"Response: {response_data}")
                return False
                
        except Exception as e:
            logger.error(f"Error storing interaction: {str(e)}")
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _store_filter(self, user_id: str, username: str, filter_type: str):
        """Store filter data in NocoDB."""
        try:
            # Get table ID for history_filters_users
            logger.debug("Getting table ID...")
            response = self._make_request(
                "get",
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            
            if not response:
                logger.error("Failed to get tables list")
                return False
                
            tables = response.json().get('list', [])
            logger.debug(f"Found tables: {[t['title'] for t in tables]}")
            
            table_id = None
            for table in tables:
                if table['title'] == 'History Filters Users':
                    table_id = table['id']
                    break
                    
            if not table_id:
                logger.error("Could not find table ID for History Filters Users")
                return False
                
            logger.debug(f"Found table ID: {table_id} for History Filters Users")
            
            # Store filter
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}"
            logger.debug(f"Storing filter at: {url}")
            
            filter_data = {
                "User Id": user_id,
                "Username": username,  # Add username field
                "Filter Type": filter_type,
                "Filtered At": datetime.now().isoformat(),
                "Session ID": str(uuid.uuid4()),
                "Session Start Time": datetime.now().isoformat(),
                "Session End Time": datetime.now().isoformat()
            }
            
            logger.debug(f"Filter data: {filter_data}")
            
            response = self._make_request(
                "post",
                url,
                json=filter_data
            )
            
            if not response:
                logger.error("Failed to store filter")
                return False
                
            response_data = response.json()
            
            # Consider the operation successful if we get a response with the data back
            if isinstance(response_data, dict):
                # Check if at least User Id and Filter Type match what we sent
                if (response_data.get('User Id') == filter_data['User Id'] and 
                    response_data.get('Filter Type') == filter_data['Filter Type']):
                    logger.info("Successfully stored filter")
                    return True
                else:
                    logger.error("Failed to store filter - response data mismatch")
                    logger.debug(f"Expected: {filter_data}")
                    logger.debug(f"Got: {response_data}")
                    return False
            else:
                logger.error("Failed to store filter - unexpected response format")
                logger.debug(f"Response: {response_data}")
                return False
                
        except Exception as e:
            logger.error(f"Error storing filter: {str(e)}")
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def store_filtered_user(self, username: str, filter_type: str, skip_reason: str = None):
        """Store filtered user data in NocoDB.
        
        Args:
            username: Instagram username
            filter_type: Type of filter applied (e.g. 'follower_filter', 'business_filter')
            skip_reason: Reason for skipping the user (optional)
        """
        if not self.enabled:
            return
            
        try:
            nocodb_logger.debug(f"Storing filtered user data for {username}")
            
            # Get table ID
            nocodb_logger.debug("Getting table ID...")
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            
            if not response or response.status_code != 200:
                nocodb_logger.error(f"Failed to get tables. Status code: {response.status_code if response else 'No response'}")
                return
                
            tables = response.json().get('list', [])
            history_table = next((t for t in tables if t['title'].lower() == 'history filters users'), None)
            
            if not history_table:
                nocodb_logger.error("History Filters Users table not found")
                return
                
            table_id = history_table['id']
            
            # Prepare record data matching the schema
            record_data = {
                "User Id": str(uuid.uuid4()),  # Generate a unique ID
                "Username": username,
                "Filtered At": datetime.now().isoformat(),
                "Filter Type": filter_type,
                "Session ID": str(uuid.uuid4()),
                "Session Start Time": datetime.now().isoformat(),
                "Session End Time": datetime.now().isoformat()
            }
            
            # Create record
            response = self._make_request(
                'post',
                f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}",
                json=record_data
            )
            
            if not response or response.status_code not in [200, 201]:
                nocodb_logger.error(f"Failed to store filtered user data. Status code: {response.status_code if response else 'No response'}")
                if response and response.text:
                    nocodb_logger.debug(f"Error response: {response.text}")
                return
                
            nocodb_logger.info(f"Successfully stored filtered user data for {username}")
            
        except Exception as e:
            nocodb_logger.error(f"Error storing filtered user data: {str(e)}")
            nocodb_logger.debug(f"Traceback: {traceback.format_exc()}")

    def get_user_interactions(self, user_id: str) -> List[Dict]:
        """Get all interactions for a user."""
        try:
            table_config = self.table_schemas.get('interacted_users')
            if not table_config:
                raise ValueError("Table schema for interacted_users not found in config")
                
            # Get actual table name from NocoDB
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            if not response:
                return []
                
            tables = response.json().get('list', [])
            table_id = None
            for table in tables:
                if table['title'].lower() == table_config['title'].lower():
                    table_id = table['id']
                    break
            
            if not table_id:
                raise ValueError(f"Table {table_config['title']} not found in NocoDB")
            
            # Use v1 API for querying records with pagination
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}"
            all_records = []
            page = 1
            page_size = 25  # Default page size in NocoDB
            
            while True:
                params = {
                    'limit': page_size,
                    'offset': (page - 1) * page_size,
                    'where': f"(User Id,eq,{user_id})"
                }
                
                response = self._make_request(
                    'get',
                    url,
                    params=params
                )
                if not response:
                    break
                
                data = response.json()
                if not isinstance(data, dict):
                    break
                    
                page_records = data.get('list', [])
                if not page_records:
                    break
                    
                all_records.extend(page_records)
                
                # Check if we've retrieved all records
                page_info = data.get('pageInfo', {})
                if page_info.get('isLastPage', True):
                    break
                    
                page += 1
            
            return all_records
            
        except Exception as e:
            logger.error(f"NocoDB: Failed to get user interactions: {str(e)}")
            return []

    def get_filtered_user(self, user_id: str) -> Optional[Dict]:
        """Get filter record for a user."""
        try:
            table_config = self.table_schemas.get('history_filters_users')
            if not table_config:
                raise ValueError("Table schema for history_filters_users not found in config")
            
            # Get actual table name from NocoDB
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            if not response:
                return None
                
            tables = response.json().get('list', [])
            table_id = None
            for table in tables:
                if table['title'].lower() == table_config['title'].lower():
                    table_id = table['id']
                    break
            
            if not table_id:
                raise ValueError(f"Table {table_config['title']} not found in NocoDB")
            
            # Use v1 API for querying records with pagination
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}"
            params = {
                'limit': 1,  # We only need the first record
                'where': f"(User Id,eq,{user_id})"
            }
            
            response = self._make_request(
                'get',
                url,
                params=params
            )
            if not response:
                return None
            
            data = response.json()
            if isinstance(data, dict) and data.get('list'):
                return data['list'][0] if data['list'] else None
            return None
            
        except Exception as e:
            logger.error(f"NocoDB: Failed to get filtered user: {str(e)}")
            return None

    def get_filtered_users(self, filter_type: str = None) -> list:
        """Get filtered users from history with optional filter type."""
        try:
            table_config = self.table_schemas.get('history_filters_users')
            if not table_config:
                raise ValueError("Table schema for history_filters_users not found in config")
            
            # Get actual table name from NocoDB
            response = self._make_request(
                'get',
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables"
            )
            if not response:
                return []
                
            tables = response.json().get('list', [])
            table_id = None
            for table in tables:
                if table['title'].lower() == table_config['title'].lower():
                    table_id = table['id']
                    break
            
            if not table_id:
                raise ValueError(f"Table {table_config['title']} not found in NocoDB")
            
            # Use v1 API for querying records with pagination
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_id}"
            all_records = []
            page = 1
            page_size = 25  # Default page size in NocoDB
            
            while True:
                params = {
                    'limit': page_size,
                    'offset': (page - 1) * page_size
                }
                
                if filter_type:
                    params['where'] = f"(Filter Type,eq,{filter_type})"
                
                response = self._make_request(
                    'get',
                    url,
                    params=params
                )
                if not response:
                    break
                
                data = response.json()
                if not isinstance(data, dict):
                    break
                    
                page_records = data.get('list', [])
                if not page_records:
                    break
                    
                all_records.extend(page_records)
                
                # Check if we've retrieved all records
                page_info = data.get('pageInfo', {})
                if page_info.get('isLastPage', True):
                    break
                    
                page += 1
            
            return all_records
            
        except Exception as e:
            logger.error(f"NocoDB: Failed to get filtered users: {str(e)}")
            return []
