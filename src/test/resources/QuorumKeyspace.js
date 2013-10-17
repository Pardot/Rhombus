{
    "name" : "quorumKeyspace",
    "replicationClass" : "NetworkTopologyStrategy",
    "replicationFactors" : {
        "DC1" : 2,
        "DC2" : 2
    },
    "consistencyLevel" : "QUORUM",
    "definitions" : [
        {
            "name": "simple",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "index_1", "type": "varchar"},
                {"name": "index_2", "type": "varchar"},
                {"name": "value", "type": "varchar"}
            ],
            "indexes" : [
                {
                    "key": "index_1",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                },
                {
                    "key": "index_2",
                    "shardingStrategy": {"type": "ShardingStrategyNone"}
                }
            ]
        }
    ]
}
