import os
import shutil

backup_dir = "config-backup/quecreate"
if not os.path.exists(backup_dir):
    os.makedirs(backup_dir)

files_to_backup = [
    "accounts/quecreate/config.yml",
    "accounts/quecreate/filters.yml",
    "accounts/quecreate/nocodb.yml"
]

for file in files_to_backup:
    if os.path.exists(file):
        shutil.copy2(file, os.path.join(backup_dir, os.path.basename(file)))
        print(f"Backed up {file}")
