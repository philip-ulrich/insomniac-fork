"""Plugin for testing NocoDB record creation functionality."""

import logging
import uuid
from datetime import datetime
from GramAddict.core.plugin_loader import Plugin

logger = logging.getLogger(__name__)

class NocoDBRecordCreationTest(Plugin):
    """Test plugin specifically for verifying NocoDB record creation functionality"""

    def __init__(self):
        super().__init__()
        self.description = "Test NocoDB record creation functionality"
        self.arguments = [
            {
                "arg": "--test-nocodb-records",
                "help": "run tests for NocoDB record creation",
                "action": "store_true",
                "operation": True
            }
        ]

    def run(self, device, configs, storage, sessions, filters, plugin):
        """Run the NocoDB record creation tests"""
        if not configs.args.test_nocodb_records:
            print("NocoDBRecordCreationTest disabled - --test-nocodb-records flag not set")
            return

        print("\n=== Starting NocoDB Record Creation Tests ===\n")

        # Check NocoDB initialization
        if not hasattr(storage, 'nocodb') or storage.nocodb is None:
            print(" ERROR: NocoDB storage not initialized. Make sure to use --use-nocodb flag")
            return

        nocodb = storage.nocodb
        if not nocodb.enabled:
            print(" ERROR: NocoDB storage is disabled")
            return

        print(" NocoDB storage initialized successfully")

        # Generate test data
        test_user_id = str(uuid.uuid4())
        test_username = "test_user_" + test_user_id[:8]
        current_time = datetime.now().isoformat()
        
        print(f"\nTest Data:")
        print(f"- User ID: {test_user_id}")
        print(f"- Username: {test_username}")
        print(f"- Timestamp: {current_time}")
        
        # Test 1: Create interaction record
        print("\nTest 1: Creating interaction record...")
        try:
            nocodb.after_interact(
                user_id=test_user_id,
                username=test_username,
                interaction_type="test_interaction",
                success=True
            )
            print(" Interaction record creation attempted")
        except Exception as e:
            print(f" Error creating interaction record: {str(e)}")

        # Test 2: Create filter record
        print("\nTest 2: Creating filter record...")
        try:
            nocodb.store_filtered_user(
                user_id=test_user_id,
                filter_type="test_filter"
            )
            print(" Filter record creation attempted")
        except Exception as e:
            print(f" Error creating filter record: {str(e)}")

        # Test 3: Verify interaction record exists
        print("\nTest 3: Verifying interaction record...")
        try:
            interactions = nocodb.get_user_interactions(test_user_id)
            if interactions:
                print(f" Found {len(interactions)} interaction(s) for test user")
                for i, interaction in enumerate(interactions, 1):
                    print(f"\nInteraction {i} details:")
                    for key, value in interaction.items():
                        print(f"  {key}: {value}")
            else:
                print(" No interactions found for test user")
        except Exception as e:
            print(f" Error verifying interaction record: {str(e)}")

        # Test 4: Verify filter record exists
        print("\nTest 4: Verifying filter record...")
        try:
            filter_record = nocodb.get_filtered_user(test_user_id)
            if filter_record:
                print(" Found filter record for test user")
                print("\nFilter record details:")
                for key, value in filter_record.items():
                    print(f"  {key}: {value}")
            else:
                print(" No filter record found for test user")
        except Exception as e:
            print(f" Error verifying filter record: {str(e)}")

        # Summary
        print("\n=== Test Summary ===")
        print("-------------------")
        print(f"Test User ID: {test_user_id}")
        print(f"Test Username: {test_username}")
        print(f"Test Timestamp: {current_time}")
        print(f"Interaction Records Found: {len(interactions) if 'interactions' in locals() and interactions else 0}")
        print(f"Filter Record Found: {'Yes' if 'filter_record' in locals() and filter_record else 'No'}")
        print("\n=== NocoDB Record Creation Tests Completed ===\n")
