{
    "name" : "pidev",
    "replicationClass" : "NetworkTopologyStrategy",
    "replicationFactors" : {
        "DEV" : 1
    },
    "definitions" : [
         {
             "name": "key_lookup",
             "allowNullPrimaryKeyInserts": true,
             "fields": [
                 {"name": "id", "type": "varchar"},
                 {"name": "key", "type": "uuid"}
             ],
             "indexes": [
             ]
         },
         {
             "name": "visit",
             "allowNullPrimaryKeyInserts": true,
             "fields": [
                 {"name": "account_id", "type": "uuid"},
                 {"name": "external_id", "type": "bigint"},
                 {"name": "visitor_id", "type": "uuid"},
                 {"name": "external_visitor_id", "type": "bigint"},
                 {"name": "prospect_id", "type": "uuid"},
                 {"name": "is_filtered", "type": "boolean"},
                 {"name": "visitor_page_view_count", "type": "int"},
                 {"name": "first_visitor_page_view_id", "type": "uuid"},
                 {"name": "external_first_visitor_page_view_id", "type": "bigint"},
                 {"name": "first_visitor_page_view_at", "type": "timestamp"},
                 {"name": "last_visitor_page_view_id", "type": "uuid"},
                 {"name": "external_last_visitor_page_view_id", "type": "bigint"},
                 {"name": "last_visitor_page_view_at", "type": "timestamp"},
                 {"name": "duration_in_seconds", "type": "int"},
                 {"name": "is_complete", "type": "boolean"},
                 {"name": "last_synced_at", "type": "timestamp"},
                 {"name": "crm_fid", "type": "varchar"},
                 {"name": "campaign_parameter", "type": "varchar"},
                 {"name": "medium_parameter", "type": "varchar"},
                 {"name": "source_parameter", "type": "varchar"},
                 {"name": "content_parameter", "type": "varchar"},
                 {"name": "term_parameter", "type": "varchar"},
                 {"name": "updated_at", "type": "timestamp"}
             ],
             "indexes": [
                {
                    "key": "account_id:visitor_id",
                    "shardingStrategy": {"type": "ShardingStrategyMonthly"}
                },
                {
                    "key": "account_id:prospect_id",
                    "shardingStrategy": {"type": "ShardingStrategyMonthly"}
                }
             ]
         },
        {
            "name": "visitor",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "account_id", "type": "uuid"},
                {"name": "external_id", "type": "bigint"},
                {"name": "campaign_id", "type": "uuid"},
                {"name": "prospect_id", "type": "uuid"},
                {"name": "profile_id", "type": "uuid"},
                {"name": "visitor_whois_id", "type": "uuid"},
                {"name": "visitor_page_view_count", "type": "int"},
                {"name": "paid_search_ad_id", "type": "uuid"},
                {"name": "ip_address", "type": "varchar"},
                {"name": "hostname", "type": "varchar"},
                {"name": "campaign_parameter", "type": "varchar"},
                {"name": "medium_parameter", "type": "varchar"},
                {"name": "source_parameter", "type": "varchar"},
                {"name": "content_parameter", "type": "varchar"},
                {"name": "term_parameter", "type": "varchar"},
                {"name": "is_filtered", "type": "boolean"},
                {"name": "is_identified", "type": "boolean"},
                {"name": "is_archived", "type": "boolean"},
                {"name": "updated_at", "type": "timestamp"}
            ],
            "indexes": [
                {
                    "key": "account_id",
                    "shardingStrategy": {"type": "ShardingStrategyHourly"}
                },
                {
                    "key": "account_id:prospect_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                }
            ]
        },
        {
            "name": "object_audit",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "external_id", "type": "bigint"},
                {"name": "account_id", "type": "uuid"},
                {"name": "object_type", "type": "varchar"},
                {"name": "object_id", "type": "uuid"},
                {"name": "source_type", "type": "varchar"},
                {"name": "source_id", "type": "uuid"},
                {"name": "user_id", "type": "uuid"},
                {"name": "type", "type": "varchar"},
                {"name": "changes", "type": "varchar"}
            ],
            "indexes" : [
                {
                    "key": "account_id:object_id:object_type",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                }
            ]
        },
        {
            "name": "visitor_audit",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "external_id", "type": "bigint"},
                {"name": "account_id", "type": "uuid"},
                {"name": "audit_object_type", "type": "varchar"},
                {"name": "audit_object_id", "type": "uuid"},
                {"name": "visitor_id", "type": "uuid"},
                {"name": "prospect_id", "type": "uuid"},
                {"name": "source_type", "type": "varchar"},
                {"name": "source_id", "type": "uuid"},
                {"name": "user_id", "type": "uuid"},
                {"name": "type", "type": "varchar"},
                {"name": "change_object_type", "type": "varchar"},
                {"name": "change_object_id", "type": "uuid"},
                {"name": "change_value_type", "type": "varchar"},
                {"name": "change_value_id", "type": "uuid"},
                {"name": "change_value", "type": "varchar"},
                {"name": "change_old_value_type", "type": "varchar"},
                {"name": "change_old_value_id", "type": "uuid"},
                {"name": "change_old_value", "type": "varchar"},
                {"name": "external_visitor_id", "type": "bigint"}
            ],
            "indexes" : [
                {
                    "key": "account_id:visitor_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:prospect_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                }
            ]
        },
        {
            "name": "visitor_page_view",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "external_id", "type": "bigint"},
                {"name": "account_id", "type": "uuid"},
                {"name": "visitor_id", "type": "uuid"},
                {"name": "visit_id", "type": "uuid"},
                {"name": "prospect_id", "type": "uuid"},
                {"name": "campaign_id", "type": "uuid"},
                {"name": "visitor_referrer_id", "type": "uuid"},
                {"name": "url", "type": "varchar"},
                {"name": "title", "type": "varchar"},
                {"name": "points", "type": "int"},
                {"name": "is_filtered", "type": "boolean"},
                {"name": "duration_in_seconds", "type": "int"},
                {"name": "last_synced_at", "type": "timestamp"},
                {"name": "crm_fid", "type": "varchar"},
                {"name": "external_visitor_id", "type": "bigint"},
                {"name": "external_visit_id", "type": "bigint"}
            ],
            "indexes" : [
                {
                    "key": "account_id",
                    "shardingStrategy": {"type": "ShardingStrategyHourly"}
                },
                {
                    "key": "account_id:visitor_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:visit_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:prospect_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                }
            ]
        },
        {
            "name": "analytics_request",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "account_id", "type": "uuid"},
                {"name": "visitor_id", "type": "uuid"},
                {"name": "r_visitor_id", "type": "uuid"},
                {"name": "r_visitor_page_view_id", "type": "uuid"},
                {"name": "campaign_id", "type": "uuid"},
                {"name": "url", "type": "varchar"},
                {"name": "last_page_view_url", "type": "varchar"},
                {"name": "page_title", "type": "varchar"},
                {"name": "pi_points", "type": "int"},
                {"name": "is_priority_page", "type": "boolean"},
                {"name": "is_pi_form", "type": "boolean"},
                {"name": "referrer", "type": "varchar"},
                {"name": "ip_address", "type": "varchar"},
                {"name": "is_new_visitor", "type": "boolean"},
                {"name": "campaign_parameter", "type": "varchar"},
                {"name": "medium_parameter", "type": "varchar"},
                {"name": "source_parameter", "type": "varchar"},
                {"name": "content_parameter", "type": "varchar"},
                {"name": "term_parameter", "type": "varchar"},
                {"name": "pi_email", "type": "varchar"},
                {"name": "pi_list_email", "type": "varchar"},
                {"name": "pi_profile_id", "type": "varchar"},
                {"name": "pi_ad_id", "type": "varchar"},
                {"name": "pi_short_code", "type": "varchar"}
            ],
            "indexes" : [
                {
                    "key": "account_id",
                    "shardingStrategy": {"type": "ShardingStrategyHourly"}
                }
            ]
        },
        {
            "name": "interaction",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "account_id", "type": "uuid"},
                {"name": "visitor_id", "type": "uuid"},
                {"name": "prospect_id", "type": "uuid"},
                {"name": "is_filtered", "type": "boolean"},
                {"name": "is_prospect_head", "type": "boolean"},
                {"name": "is_visitor_head", "type": "boolean"},
                {"name": "last_synced_at", "type": "timestamp"},
                {"name": "crm_fid", "type": "varchar"},
                {"name": "object_uuid", "type": "uuid"},
                {"name": "object_type", "type": "int"},
                {"name": "is_processed", "type": "boolean"}
            ],
            "indexes": [
                {
                    "key": "account_id:visitor_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:prospect_id",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:visitor_id:is_visitor_head",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:prospect_id:is_prospect_head",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "account_id:is_processed",
                    "shardingStrategy": {"type": "ShardingStrategyHourly"}
                }
            ]
        },
        {
            "name": "transition_miss",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "account_id", "type": "uuid"},
                {"name": "method_name", "type": "varchar"},
                {"name": "rhombus_value", "type": "varchar"},
                {"name": "mysql_value", "type": "varchar"},
                {"name": "object_type", "type": "varchar"},
                {"name": "hint", "type": "varchar"},
                {"name": "arguments", "type": "varchar"}
            ],
            "indexes": [
                {
                    "key": "account_id:object_type:method_name",
                    "shardingStrategy": {"type": "ShardingStrategyHourly"}
                }
            ]
        }
    ]
}
