import logging
from typing import Optional
from .base_plugin import BasePlugin

logger = logging.getLogger(__name__)

class PluginLoader:
    """Loads and manages plugins"""
    
    def __init__(self):
        self.plugins = {}
        
    def load_sync_plugin(self) -> Optional[BasePlugin]:
        """Load the sync plugin"""
        try:
            # Here you would typically load your sync plugin
            # For now, we'll return None as a placeholder
            logger.info("Loading sync plugin...")
            return None
        except Exception as e:
            logger.error(f"Failed to load sync plugin: {e}")
            return None
            
    def cleanup_plugins(self):
        """Cleanup all loaded plugins"""
        for plugin in self.plugins.values():
            try:
                plugin.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up plugin: {e}")
        self.plugins.clear()
