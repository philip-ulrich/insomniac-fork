from datetime import datetime
from typing import Dict, List, Optional

class Interaction:
    """Represents an interaction with a user"""
    def __init__(self, username: str, interaction_type: str, timestamp: datetime = None):
        self.username = username
        self.interaction_type = interaction_type
        self.timestamp = timestamp or datetime.now()

class HistoryManager:
    """Manages interaction history"""
    def __init__(self):
        self.interactions: Dict[str, List[Interaction]] = {}
        
    def add_interaction(self, account: str, interaction: Interaction):
        """Add an interaction to history"""
        if account not in self.interactions:
            self.interactions[account] = []
        self.interactions[account].append(interaction)
        
    def get_interactions(self, account: str, interaction_type: Optional[str] = None) -> List[Interaction]:
        """Get interactions for an account"""
        account_interactions = self.interactions.get(account, [])
        if interaction_type:
            return [i for i in account_interactions if i.interaction_type == interaction_type]
        return account_interactions
        
    def clear_history(self, account: str, interaction_type: Optional[str] = None):
        """Clear history for an account"""
        if interaction_type:
            self.interactions[account] = [
                i for i in self.interactions.get(account, [])
                if i.interaction_type != interaction_type
            ]
        else:
            self.interactions[account] = []
