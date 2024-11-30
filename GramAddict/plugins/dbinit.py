import logging
import time
from GramAddict.core.plugin_loader import Plugin

logger = logging.getLogger(__name__)

class DBInitPlugin(Plugin):
    """Initialize NocoDB tables at session start"""

    def __init__(self):
        super().__init__()
        self.description = "Initializes NocoDB tables if they don't exist"
        self.arguments = [
            {
                "arg": "--init-db",
                "help": "initialize database tables if they don't exist",
                "nargs": None,
                "metavar": None,
                "default": False,
                "action": "store_true",
                "operation": True
            }
        ]

    def run(self, device, configs, storage, sessions, filters, plugin_name):
        logger.info("="*80)
        logger.info("Starting database initialization...")
        
        if not configs.args.init_db:
            logger.info("Database initialization skipped - --init-db flag not set")
            return
            
        # Check if nocodb plugin is enabled and initialized
        from GramAddict.plugins.nocodb_storage import NocoDBStorage
        nocodb = None
        
        # Wait for NocoDB plugin to initialize (max 30 seconds)
        max_retries = 30
        retry_count = 0
        while retry_count < max_retries:
            for plugin in configs.plugins:
                if isinstance(plugin, NocoDBStorage) and plugin.enabled:
                    nocodb = plugin
                    break
            if nocodb:
                break
            logger.info("Waiting for NocoDB plugin to initialize...")
            time.sleep(1)
            retry_count += 1
                
        if not nocodb:
            logger.warning("NocoDB plugin not found or not enabled. Please enable it with --use-nocodb")
            return
            
        logger.info("Initializing NocoDB tables...")
        try:
            # Initialize tables
            nocodb.init_tables()
            logger.info("Database tables initialization completed successfully!")
            logger.info("You can now use NocoDB to store interaction data.")
            logger.info("="*80)
            
        except Exception as e:
            logger.error(f"Failed to initialize database tables: {str(e)}")
            raise e
