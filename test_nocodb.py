import os
import logging
import uuid
from datetime import datetime
import pytest
import requests
import yaml
from GramAddict.plugins.nocodb_storage import NocoDBStorage
from GramAddict.core.storage import Storage

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('logs/test_nocodb.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MockDevice:
    def __init__(self):
        pass

class Args:
    def __init__(self):
        self.username = "quecreate"
        self.use_nocodb = True
        self.init_db = True
        self.config = os.path.join('accounts', 'quecreate', 'config.yml')

class MockConfig:
    def __init__(self):
        self.args = Args()
        self.username = "quecreate"

def load_config():
    config_path = os.path.join('accounts', 'quecreate', 'config.yml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

@pytest.fixture
def storage():
    """Fixture to provide NocoDBStorage instance."""
    storage_instance = NocoDBStorage()
    storage_instance.args = Args()
    
    device = MockDevice()
    configs = MockConfig()
    storage = Storage("quecreate")  # Use Storage class instead of dict
    sessions = {}
    filters = {}
    plugin_name = "nocodb_storage"
    
    storage_instance.run(device, configs, storage, sessions, filters, plugin_name)
    return storage_instance

def test_api_connection(storage):
    """Test API connection to NocoDB."""
    logger.info("Testing API connection...")
    try:
        url = f"{storage.base_url}/api/v1/db/meta/projects/{storage.base_id}/tables"
        response = requests.get(url, headers=storage.headers)
        response.raise_for_status()
        logger.info("API connection successful!")
        assert True
    except Exception as e:
        logger.error(f"API connection failed: {str(e)}")
        assert False

def test_table_initialization(storage):
    """Test table initialization."""
    logger.info("Testing table initialization...")
    try:
        result = storage.init_tables()
        assert result is True
    except Exception as e:
        logger.error(f"Table initialization failed: {str(e)}")
        assert False

def test_interaction_storage(storage):
    """Test storing an interaction."""
    logger.info("Testing interaction storage...")
    
    interaction_data = {
        "User Id": "test_user_123",
        "Username": "test_username",
        "Full Name": "Test User",
        "Profile URL": "https://instagram.com/test_username",
        "Interaction Type": "test_interaction",
        "Success": True,
        "Timestamp": datetime.now().isoformat(),
        "Session ID": str(uuid.uuid4()),
        "Job Name": "test_job",
        "Target": "test_target",
        "Session Start Time": datetime.now().isoformat(),
        "Session End Time": datetime.now().isoformat()
    }
    
    logger.info(f"Storing interaction for user {interaction_data['Username']}")
    try:
        storage._store_interaction(interaction_data)
        assert True
    except Exception as e:
        logger.error(f"Failed to store interaction: {str(e)}")
        assert False

def test_filter_storage(storage):
    """Test storing filter data."""
    logger.info("Testing filter storage...")
    try:
        storage._store_filter("test_user_123", "test_username", "test_filter")
        assert True
    except Exception as e:
        logger.error(f"Failed to store filter: {str(e)}")
        assert False

def test_user_interactions(storage):
    """Test interaction retrieval for a user."""
    logger.info("Testing user interactions retrieval...")
    try:
        interactions = storage.get_user_interactions("test_user_123")
        assert isinstance(interactions, list)
    except Exception as e:
        logger.error(f"Failed to get user interactions: {str(e)}")
        assert False

def test_filtered_user(storage):
    """Test filter retrieval for a user."""
    logger.info("Testing filtered user retrieval...")
    try:
        filter_record = storage.get_filtered_user("test_user_123")
        assert isinstance(filter_record, dict) or filter_record is None
    except Exception as e:
        logger.error(f"Failed to get filtered user: {str(e)}")
        assert False

def main():
    """Main test function."""
    logger.info("="*80)
    logger.info("Starting NocoDB Storage Tests")
    logger.info("="*80)
    
    # Initialize storage
    storage = NocoDBStorage()
    storage.args = Args()
    
    device = MockDevice()
    configs = MockConfig()
    storage_dict = Storage("quecreate")  
    sessions = {}
    filters = {}
    plugin_name = "nocodb_storage"
    
    storage.run(device, configs, storage_dict, sessions, filters, plugin_name)
    
    # Run tests
    test_api_connection(storage)
    test_table_initialization(storage)
    test_interaction_storage(storage)
    test_filter_storage(storage)
    test_user_interactions(storage)
    test_filtered_user(storage)

if __name__ == "__main__":
    main()
