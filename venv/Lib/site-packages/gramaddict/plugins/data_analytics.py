import logging
import sys

from GramAddict.core.plugin_loader import Plugin

logger = logging.getLogger(__name__)


class DataAnalytics(Plugin):
    """Generates a PDF analytics report of current username session data"""

    def __init__(self):
        super().__init__()
        self.description = (
            "Generates a PDF analytics report of current username session data"
        )
        self.arguments = [
            {
                "arg": "--analytics",
                "help": "generates a PDF analytics report of current username session data",
                "action": "store_true",
                "operation": True,
            }
        ]

    def run(self, device, configs, storage, sessions, plugin):
        logger.warning(
            """Analytics have been removed due to a problem in some OS with loading matplotlib.
                            I'll rewrite and improve the report using other libraries.
                            In the meantime you can keep using analytics tool by replacing this file (Gramaddict/plugins/data_analytics.py)
                            with the one inside the release 2.0.8 https://github.com/GramAddict/bot/releases/tag/2.0.8
                            For see where this file is located in your machine, just write 'pip shown GramAddict' and you will get the path.
                            """
        )
        modulename = "matplotlib"
        if modulename not in sys.modules:
            logger.error(
                f"You can't use {plugin} without installing {modulename}. Type that in console: 'pip3 install gramaddict[analytics]'"
            )
            return
