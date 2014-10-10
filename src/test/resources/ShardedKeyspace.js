{
    "name" : "pifunctional",
    "replicationClass" : "SimpleStrategy",
    "replicationFactors" : {
        "replication_factor" : 1
    },
    "definitions" : [
        {
            "name": "object1",
            "fields": [
                {"name": "account_id", "type": "uuid"},
                {"name": "user_id", "type": "uuid"},
                {"name": "field1", "type": "varchar"},
                {"name": "created_at", "type": "timestamp"}
            ],
            "indexes" : [
                {
                    "key": "account_id:user_id",
                    "shardingStrategy": {"type": "ShardingStrategyMonthly"}
                }
            ]
        },
        {
            "name": "object2",
            "fields": [
                {"name": "account_id", "type": "uuid"},
                {"name": "user_id", "type": "uuid"},
                {"name": "field1", "type": "varchar"},
                {"name": "created_at", "type": "timestamp"}
            ],
            "indexes" : [
                {
                    "key": "account_id:user_id",
                    "shardingStrategy": {"type": "ShardingStrategyWeekly"}
                }
            ]
        }
    ]
}


