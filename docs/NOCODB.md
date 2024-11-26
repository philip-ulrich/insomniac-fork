# NocoDB Integration Guide

## Overview
This document contains important information about integrating with NocoDB in the Instagram Automation project.

## Table Schema Conventions

### Important Notes
1. Column names in NocoDB are case-sensitive and may contain spaces
2. The actual column names in the database must be used exactly as they appear in NocoDB
3. Always verify column names through the NocoDB UI or API before implementing filters

### Current Table Schemas

#### history_filters_users
| Column Name  | Type     | Notes |
|-------------|----------|-------|
| User Id     | string   | Note the space and capitalization |
| Filter Type | string   | Note the space and capitalization |
| filtered_at | datetime | |

#### interacted_users
| Column Name      | Type    | Notes |
|-----------------|---------|-------|
| user_id         | string  | |
| username        | string  | |
| interaction_at  | datetime| |
| session_id      | string  | |
| job_name        | string  | |
| target          | string  | |
| followed        | boolean | |
| is_requested    | boolean | |
| scraped         | boolean | |
| liked_count     | integer | |
| watched_count   | integer | |
| commented_count | integer | |
| pm_sent         | boolean | |
| success         | boolean | |

## API Filter Format
```python
# Single filter
params["where"] = f"({field},{op},{value})"

# Multiple filters with AND
params["where"] = "~and".join([f"({field},{op},{value})", f"({field2},{op2},{value2})"])
```

## Common Issues
1. **422 Unprocessable Entity Errors**
   - Often caused by incorrect column names in filter queries
   - Double-check exact column names in NocoDB UI
   - Remember that column names are case-sensitive

2. **Filter Not Returning Results**
   - Verify column names match exactly with NocoDB schema
   - Check for spaces in column names (e.g., "User Id" not "user_id")
   - Ensure filter operators are correct (eq, like, in, etc.)

## Testing
1. Always run the test suite before committing changes:
   ```bash
   python test_nocodb.py
   ```
2. Verify both storage and retrieval operations
3. Check logs for any 422 errors or filter mismatches

## Best Practices
1. Use constants for column names to prevent typos
2. Add logging for filter operations to help debug issues
3. Validate column names during plugin initialization
4. Document any schema changes in this file
