import os
import logging
import uuid
from datetime import datetime
from GramAddict.plugins.nocodb_storage import NocoDBStorage
from enum import Enum, auto

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('logs/test_session.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FollowButtonText(Enum):
    FOLLOW = auto()
    FOLLOWING = auto()
    REQUESTED = auto()

def test_session():
    try:
        session_id = str(uuid.uuid4())
        logger.info(f"Starting test session with ID: {session_id}")
        
        # Initialize NocoDB storage
        config_path = os.path.join('accounts', 'quecreate', 'nocodb.yml')
        nocodb_storage = NocoDBStorage(config_path)

        # Initialize tables
        logger.info("Initializing NocoDB tables...")
        if not nocodb_storage._init_tables():
            logger.error("Failed to initialize tables")
            return False

        # Test interaction tracking
        logger.info("Testing interaction storage...")
        interaction_data = {
            'id': session_id,
            'user_id': 'test_user_123',
            'username': 'test_username',
            'interaction_at': datetime.now().isoformat(),
            'session_id': session_id,
            'job_name': 'test_job',
            'target': 'test_target',
            'followed': True,
            'is_requested': False,
            'scraped': False,
            'liked_count': 2,
            'watched_count': 1,
            'commented_count': 0,
            'pm_sent': False,
            'success': True
        }
        
        logger.info(f"Storing interaction for user {interaction_data['username']}")
        nocodb_storage._store_interaction(interaction_data)

        # Test filter tracking
        logger.info("Testing filter storage...")
        user_id = 'test_user_123'
        filter_type = 'test_filter'
        
        logger.info(f"Storing filter for user {user_id}")
        nocodb_storage._store_filter(user_id, filter_type)

        # Test data retrieval
        logger.info("Testing data retrieval...")
        
        # Test interaction retrieval
        interactions = nocodb_storage.get_user_interactions("test_username")
        if interactions:
            logger.info(f"Found {len(interactions)} interactions for test_username")
        else:
            logger.warning("No interactions found for test_username")
            
        # Test filter retrieval
        filters = nocodb_storage.get_filtered_user("test_user_123")
        if filters:
            logger.info(f"Found {len(filters)} filter records for test_user_123")
        else:
            logger.warning("No filter records found for test_user_123")

        logger.info("Session test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Test session failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_session()
