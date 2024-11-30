from datetime import datetime
from typing import Dict, List, Optional

class Interaction:
    """Represents an interaction with a user"""
    def __init__(self, username: str, interaction_type: str, timestamp: datetime = None, error: bool = False, duration: Optional[float] = None):
        self.username = username
        self.interaction_type = interaction_type
        self.timestamp = timestamp or datetime.now()
        self.error = error
        self.duration = duration

class HistoryManager:
    """Manages interaction history"""
    def __init__(self):
        self.interactions: Dict[str, List[Interaction]] = {}
        
    def add_interaction(self, account: str, interaction: Interaction):
        """Add an interaction to history"""
        if account not in self.interactions:
            self.interactions[account] = []
        self.interactions[account].append(interaction)
        
    def get_interactions(self, account: str, interaction_type: Optional[str] = None, start_time: Optional[datetime] = None) -> List[Interaction]:
        """Get interactions for an account"""
        account_interactions = self.interactions.get(account, [])
        filtered_interactions = account_interactions

        if interaction_type:
            filtered_interactions = [i for i in filtered_interactions if i.interaction_type == interaction_type]
        
        if start_time:
            filtered_interactions = [i for i in filtered_interactions if i.timestamp >= start_time]
            
        return filtered_interactions
        
    def clear_history(self, account: str, interaction_type: Optional[str] = None):
        """Clear history for an account"""
        if interaction_type:
            self.interactions[account] = [
                i for i in self.interactions.get(account, [])
                if i.interaction_type != interaction_type
            ]
        else:
            self.interactions[account] = []
