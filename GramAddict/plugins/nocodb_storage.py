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

logger = logging.getLogger(__name__)

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
        self.table_schemas = None  # Will be loaded from config
        
        # Table schemas
        # self.table_schemas = {
        #     "interacted_users": {
        #         "table_name": "interacted_users",
        #         "title": "Interacted Users",
        #         "columns": [
        #             {"column_name": "User Id", "title": "User Id", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Username", "title": "Username", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Full Name", "title": "Full Name", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Profile URL", "title": "Profile URL", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Interaction Type", "title": "Interaction Type", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Success", "title": "Success", "uidt": "Checkbox", "dt": "boolean"},
        #             {"column_name": "Timestamp", "title": "Timestamp", "uidt": "DateTime", "dt": "datetime"}
        #         ]
        #     },
        #     "history_filters_users": {
        #         "table_name": "history_filters_users",
        #         "title": "History Filters Users",
        #         "columns": [
        #             {"column_name": "User Id", "title": "User Id", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Username", "title": "Username", "uidt": "SingleLineText", "dt": "varchar"},
        #             {"column_name": "Filtered At", "title": "Filtered At", "uidt": "DateTime", "dt": "datetime"},
        #             {"column_name": "Filter Type", "title": "Filter Type", "uidt": "SingleLineText", "dt": "varchar"}
        #         ]
        #     }
        # }

    def run(self, device, configs, storage, sessions, filters, plugin_name):
        """Initialize NocoDB storage"""
        logger.info("Initializing NocoDB storage...")
        
        # Set enabled flag based on --use-nocodb argument
        self.enabled = configs.args.use_nocodb
        if not self.enabled:
            logger.info("NocoDB storage disabled - --use-nocodb flag not set")
            return
            
        try:
            logger.info("NocoDB: Loading configuration...")
            config_path = os.path.join('accounts', configs.username, 'nocodb.yml')
            self.config = self.load_config(config_path)
            if not self.config:
                logger.error("Failed to load NocoDB configuration")
                self.enabled = False
                return
                
            # Set up connection parameters
            self.base_url = self.config["base_url"]
            self.api_token = self.config["api_token"]
            self.base_id = self.config["base_id"]
            self.headers = {
                "xc-token": self.api_token,
                "accept": "application/json",
                "Content-Type": "application/json"
            }
            
            # Load table schemas from config
            self.table_schemas = self.config.get("table_schemas", {})
            
            # Test connection
            logger.info("Testing NocoDB connection...")
            response = requests.get(
                f"{self.base_url}/api/v1/health",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            logger.info("NocoDB connection successful!")
            
            # Set nocodb reference in storage
            if storage:
                storage.nocodb = self
                logger.info("NocoDB reference set in storage")
            
            logger.info("NocoDB storage initialized successfully!")
            
        except Exception as e:
            logger.error(f"Failed to initialize NocoDB storage: {str(e)}")
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.error(f"Traceback: {traceback.format_exc()}")
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
        logger.info("Initializing NocoDB tables...")
        try:
            # Get existing tables using the correct API endpoint
            response = requests.get(
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables",
                headers=self.headers
            )
            response.raise_for_status()
            
            # Get existing tables from response
            response_json = response.json()
            existing_tables = {table["title"] for table in response_json.get('list', [])}
            
            # Create missing tables
            for schema in self.table_schemas.values():
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
                    
                    response = requests.post(
                        f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables",
                        headers=self.headers,
                        json=table_data
                    )
                    response.raise_for_status()
                    logger.info(f"Created table: {schema['title']}")
                else:
                    logger.info(f"Table already exists: {schema['title']}")
                    
            logger.info("Table initialization completed!")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize tables: {str(e)}")
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def create_table(self, schema):
        """Create a table in NocoDB."""
        try:
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
            
            response = requests.post(
                f"{self.base_url}/api/v1/db/meta/projects/{self.base_id}/tables",
                headers=self.headers,
                json=table_data
            )
            response.raise_for_status()
            logger.info(f"Created table: {schema['title']}")
            
        except Exception as e:
            logger.error(f"Failed to create table {schema['title']}: {str(e)}")
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.error(f"Traceback: {traceback.format_exc()}")
            raise

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
                "User Id": user_id,
                "Username": username,
                "Interaction Type": interaction_type,
                "Success": success,
                "Timestamp": datetime.now().isoformat()
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

    def _store_interaction(self, interaction_data):
        """Store interaction data in NocoDB."""
        try:
            table_config = self.table_schemas.get('interacted_users')
            if not table_config or 'table_name' not in table_config:
                raise ValueError("Table name not found. Make sure tables are initialized.")
            
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_config['table_name']}"
            response = requests.post(url, headers=self.headers, json=interaction_data)
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store interaction: {str(e)}")
            return False

    def _store_filter(self, user_id: str, filter_type: str):
        """Store filter data in NocoDB."""
        try:
            table_config = self.table_schemas.get('history_filters_users')
            if not table_config or 'table_name' not in table_config:
                raise ValueError("Table name not found. Make sure tables are initialized.")
            
            filter_data = {
                "User Id": user_id,
                "Filtered At": datetime.now().isoformat(),
                "Filter Type": filter_type
            }
            
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_config['table_name']}"
            response = requests.post(url, headers=self.headers, json=filter_data)
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store filter: {str(e)}")
            return False

    def store_filtered_user(self, user_id: str, filter_type: str) -> bool:
        """Store a filtered user in NocoDB"""
        if not self.enabled:
            return False

        stored = self._store_filter(user_id, filter_type)
        if stored:
            logger.info(f"NocoDB: Successfully stored filter for user {user_id}")
            return True
        else:
            logger.warning(f"NocoDB: Failed to store filter for user {user_id}")
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
                    self._store_interaction({
                        "User Id": user_id,
                        "Username": interaction_data.get('username', ''),
                        "Interaction Type": interaction_data.get('interaction_type', ''),
                        "Success": interaction_data.get('success', False),
                        "Timestamp": interaction_data.get('timestamp', '')
                    })
                    
        except Exception as e:
            logger.error(f"Failed to process history file {path_to_file}: {str(e)}")

    def before_job(self, plugin, plugin_name):
        """Initialize NocoDB before each job"""
        if self.args.use_nocodb:
            logger.info("NocoDB: Initializing storage")
            config_path = os.path.join('accounts', plugin.configs.username, 'nocodb.yml')
            
            # Check if config file exists
            if not os.path.exists(config_path):
                logger.error(f"NocoDB: Config file not found at {config_path}")
                return
                
            # Load config and initialize
            success = self.load_config(config_path)
            if success:
                logger.info("NocoDB: Successfully initialized")
                logger.debug(f"NocoDB: Using base URL: {self.base_url}")
            else:
                logger.warning("NocoDB: Failed to initialize")

    def _handle_follow(self, user_id, username, success=True):
        """Handle follow event"""
        logger.info(f"Follow event: user_id={user_id}, username={username}, success={success}")
        self._store_interaction({
            "User Id": user_id,
            "Username": username,
            "Interaction Type": "follow",
            "Success": success,
            "Timestamp": datetime.now().isoformat()
        })

    def _handle_like(self, user_id, username, count=1, success=True):
        """Handle like event"""
        logger.info(f"Like event: user_id={user_id}, username={username}, count={count}, success={success}")
        self._store_interaction({
            "User Id": user_id,
            "Username": username,
            "Interaction Type": "like",
            "Success": success,
            "Timestamp": datetime.now().isoformat()
        })

    def _handle_watch(self, user_id, username, count=1, success=True):
        """Handle watch event"""
        logger.info(f"Watch event: user_id={user_id}, username={username}, count={count}, success={success}")
        self._store_interaction({
            "User Id": user_id,
            "Username": username,
            "Interaction Type": "watch",
            "Success": success,
            "Timestamp": datetime.now().isoformat()
        })

    def _handle_comment(self, user_id, username, count=1, success=True):
        """Handle comment event"""
        logger.info(f"Comment event: user_id={user_id}, username={username}, count={count}, success={success}")
        self._store_interaction({
            "User Id": user_id,
            "Username": username,
            "Interaction Type": "comment",
            "Success": success,
            "Timestamp": datetime.now().isoformat()
        })

    def _handle_pm(self, user_id, username, success=True):
        """Handle PM event"""
        logger.info(f"PM event: user_id={user_id}, username={username}, success={success}")
        self._store_interaction({
            "User Id": user_id,
            "Username": username,
            "Interaction Type": "pm",
            "Success": success,
            "Timestamp": datetime.now().isoformat()
        })

    def get_user_interactions(self, user_id: str) -> list:
        """Get interaction records for a specific user.
        
        Args:
            user_id: User ID to get interactions for
        
        Returns:
            List of interaction records for the user
        """
        try:
            table_config = self.table_schemas.get('interacted_users')
            if not table_config or 'table_name' not in table_config:
                raise ValueError("Table name not found. Make sure tables are initialized.")
            
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_config['table_name']}"
            params = {
                "filter[User Id][eq]": user_id
            }
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('list', [])
            
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
            table_config = self.table_schemas.get('history_filters_users')
            if not table_config or 'table_name' not in table_config:
                raise ValueError("Table name not found. Make sure tables are initialized.")
            
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_config['table_name']}"
            params = {
                "filter[User Id][eq]": user_id
            }
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('list', [])
            
        except Exception as e:
            logger.error(f"Failed to get filtered user: {str(e)}")
            return None

    def get_filtered_users(self, filter_type: str = None, user_id: str = None) -> list:
        """Get filtered users from history with optional filters.
        
        Args:
            filter_type: Optional filter type to filter by
            user_id: Optional user ID to filter by
            
        Returns:
            List of filtered user records
        """
        try:
            table_config = self.table_schemas.get('history_filters_users')
            if not table_config or 'table_name' not in table_config:
                raise ValueError("Table name not found. Make sure tables are initialized.")
            
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_config['table_name']}"
            params = {}
            if filter_type:
                params["filter[Filter Type][eq]"] = filter_type
            if user_id:
                params["filter[User Id][eq]"] = user_id
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('list', [])
            
        except Exception as e:
            logger.error(f"Failed to get filtered users: {str(e)}")
            return None

    def get_interacted_users(self, user_id: str = None) -> list:
        """Get interacted users with optional user_id filter.
        
        Args:
            user_id: Optional user ID to filter by
            
        Returns:
            List of interacted user records
        """
        try:
            table_config = self.table_schemas.get('interacted_users')
            if not table_config or 'table_name' not in table_config:
                raise ValueError("Table name not found. Make sure tables are initialized.")
            
            url = f"{self.base_url}/api/v1/db/data/noco/{self.base_id}/{table_config['table_name']}"
            params = {}
            if user_id:
                params["filter[User Id][eq]"] = user_id
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('list', [])
            
        except Exception as e:
            logger.error(f"Failed to get interacted users: {str(e)}")
            return None
