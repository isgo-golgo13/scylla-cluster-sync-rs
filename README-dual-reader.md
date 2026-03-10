
# DUAL-READER FILTERING LOGIC (CLIENT SPEC)

## Requirements

### Context
Some customers already reside in AWS pre-migration. The dual-reader needs selective comparison logic.

### Requirement 1: system_domain_id Filtering

**Behavior:**
- Provide a list of `system_domain_id` values that SHOULD be compared between source and target
- For requests containing a `system_domain_id` IN this list: read from BOTH source and target, compare results
- For requests containing a `system_domain_id` NOT in this list: read from SOURCE ONLY, return data without comparison

**Config Example:**
```yaml
dual_reader:
  filter:
    # List of system_domain_ids to compare (already migrated to AWS)
    compare_system_domain_ids:
      - "domain-abc-123"
      - "domain-def-456"
      - "domain-ghi-789"
    
    # Column name to extract system_domain_id from queries
    system_domain_id_column: "system_domain_id"
```

**Logic:**
```
IF query contains system_domain_id IN compare_list:
    result_source = query(source)
    result_target = query(target)
    compare(result_source, result_target)
    log_discrepancy_if_any()
    return result_source  # or result_target based on config
ELSE:
    result_source = query(source)
    return result_source  # No comparison, source only
```

### Requirement 2: Table Comparison Whitelist

**Behavior:**
- Provide a list of `keyspace.table_name` that SHOULD be compared
- Tables NOT in this list: read from SOURCE ONLY, no comparison
- Some tables contain source-system-specific data that won't be migrated

**Config Example:**
```yaml
dual_reader:
  filter:
    # Tables to compare (will be migrated)
    compare_tables:
      - "acls_keyspace.group_acls"
      - "acls_keyspace.user_acls"
      - "assets_keyspace.assets"
      - "assets_keyspace.asset_versions"
      - "collections_keyspace.collections"
    
    # Tables NOT in this list are read from source only
    # Example excluded: "audit_keyspace.audit_logs" (source-specific)
```

**Logic:**
```
IF table IN compare_tables:
    proceed_with_comparison()  # Subject to system_domain_id filter above
ELSE:
    result_source = query(source)
    return result_source  # No comparison, source only
```

### Combined Logic Flow

```
1. Parse incoming query to extract:
   - keyspace.table_name
   - system_domain_id (if present in query/parameters)

2. Check table whitelist:
   IF table NOT IN compare_tables:
       -> Read SOURCE only, return result
   
3. Check system_domain_id:
   IF system_domain_id NOT IN compare_system_domain_ids:
       -> Read SOURCE only, return result
   
4. Both conditions met - perform dual read:
   result_source = query(source)
   result_target = query(target)
   compare_and_log(result_source, result_target)
   return result_source
```