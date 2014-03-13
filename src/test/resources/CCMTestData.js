{
    "name" : "ccm",
    "replicationClass" : "NetworkTopologyStrategy",
    "replicationFactors" : {
        "DC1" : 2,
        "DC2" : 2
    },
    "definitions" : [
        {
            "name": "simpletype",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "field0", "type": "varchar"},
                {"name": "field1", "type": "varchar"},
                {"name": "field2", "type": "varchar"},
                {"name": "field3", "type": "varchar"},
                {"name": "field4", "type": "varchar"},
                {"name": "field5", "type": "varchar"},
                {"name": "field6", "type": "varchar"},
                {"name": "field7", "type": "varchar"},
                {"name": "field8", "type": "varchar"},
                {"name": "field9", "type": "varchar"},
                {"name": "field10", "type": "varchar"},
                {"name": "field11", "type": "varchar"},
                {"name": "field12", "type": "varchar"}

            ],
            "indexes" : [
                {
                    "key": "field1",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "field2",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "field3",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "field4",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "field5",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "field6",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
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


