{
    "name" : "functional",
    "replicationClass" : "SimpleStrategy",
    "replicationFactors" : {
        "replication_factor" : 1
    },
    "definitions" : [
        {
            "name": "testtype",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "foreignid", "type": "bigint"},
                {"name": "type", "type": "int"},
                {"name": "instance", "type": "bigint"},
                {"name": "filtered", "type": "int"},
                {"name": "data1", "type": "varchar"},
                {"name": "data2", "type": "varchar"},
                {"name": "data3", "type": "varchar"}
            ],
            "indexes" : [
                {
                    "key": "foreignid",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "type:instance",
                    "shardingStrategy": {"type": "ShardingStrategyMonthly"}
                },
                {
                    "key": "foreignid:type:instance",
                    "shardingStrategy": {"type": "ShardingStrategyMonthly"}
                }
            ]
        },
        {
            "name": "customkey",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "id", "type": "varchar"},
                {"name": "data1", "type": "varchar"}
            ],
            "indexes" : [
                {
                    "key": "data1",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                }
            ]
        }
    ]
}


