import os
import logging
import uuid
from datetime import datetime
import pytest
import requests
from GramAddict.plugins.nocodb_storage import NocoDBStorage

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

@pytest.fixture
def storage():
    """Fixture to provide NocoDBStorage instance."""
    config_path = os.path.join('accounts', 'quecreate', 'nocodb.yml')
    storage_instance = NocoDBStorage(config_path)
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
        result = storage._init_tables()
        assert result is True
    except Exception as e:
        logger.error(f"Table initialization failed: {str(e)}")
        assert False

def test_interaction_storage(storage):
    """Test storing an interaction."""
    logger.info("Testing interaction storage...")
    
    interaction_data = {
        'id': str(uuid.uuid4()),
        'user_id': 'test_user_123',
        'username': 'test_username',
        'interaction_type': 'test_interaction',
        'success': True,
        'created_at': datetime.now().isoformat()
    }
    
    logger.info(f"Storing interaction for user {interaction_data['username']}")
    result = storage._store_interaction(interaction_data)
    assert result is True

def test_filter_storage(storage):
    """Test storing a filter."""
    logger.info("Testing filter storage...")
    
    user_id = 'test_user_123'
    filter_type = 'test_filter'
    
    logger.info(f"Storing filter for user {user_id}")
    result = storage._store_filter(user_id, filter_type)
    assert result is True

def test_user_interactions(storage):
    """Test interaction retrieval for a user."""
    logger.info("Testing interaction retrieval for user test_username")
    interactions = storage.get_user_interactions("test_username")
    assert interactions is not None
    logger.info(f"Found {len(interactions)} interactions")

def test_filtered_user(storage):
    """Test filter retrieval for a user."""
    logger.info("Testing filter retrieval for user test_user_123")
    filter_record = storage.get_filtered_user("test_user_123")
    assert filter_record is not None
    logger.info("Found filter record")

def main():
    """Main test function."""
    logger.info("="*80)
    logger.info("Starting NocoDB Storage Tests")
    logger.info("="*80)
    
    # Initialize storage
    config_path = os.path.join('accounts', 'quecreate', 'nocodb.yml')
    storage = NocoDBStorage(config_path)
    
    # Run tests
    test_api_connection(storage)
    test_table_initialization(storage)
    test_interaction_storage(storage)
    test_filter_storage(storage)
    test_user_interactions(storage)
    test_filtered_user(storage)

if __name__ == "__main__":
    main()
