import json
import logging
import os
import sys

from atomicwrites import atomic_write

from GramAddict.core.storage import ACCOUNTS

logger = logging.getLogger(__name__)


class PersistentList(list):
    filename = None
    encoder = None

    def __init__(self, filename, encoder):
        self.filename = filename
        self.encoder = encoder
        super().__init__()

    def persist(self, directory):
        if directory is None:
            return

        if not os.path.exists(f"{ACCOUNTS}/{directory}"):
            os.makedirs(f"{ACCOUNTS}/{directory}")

        path = f"{ACCOUNTS}/{directory}/{self.filename}.json"

        if os.path.exists(path):
            with open(path) as json_file:
                try:
                    json_array = json.load(json_file)
                except Exception as e:
                    logger.error(
                        f"Please check {json_file.name}, it contains this error: {e}"
                    )
                    sys.exit(0)
            os.remove(path)
        else:
            json_array = []

        json_array += (self.encoder.default(self.encoder, item) for item in self)

        # Remove duplicates
        json_object = {}
        for item in json_array:
            item_id = item.get("id")
            if item_id is None:
                raise Exception("Items in PersistentList must have id property!")
            json_object[item_id] = item
        json_array = list(json_object.values())

        with atomic_write(path, overwrite=True, encoding="utf-8") as outfile:
            json.dump(json_array, outfile, indent=4, sort_keys=False)
