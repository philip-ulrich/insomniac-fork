from abc import ABC, abstractmethod

class BasePlugin(ABC):
    """Base class for all plugins"""
    
    @abstractmethod
    def initialize(self):
        """Initialize the plugin"""
        pass
    
    @abstractmethod
    def cleanup(self):
        """Cleanup resources"""
        pass
    
    @abstractmethod
    def sync_data(self):
        """Sync data with external storage"""
        pass
