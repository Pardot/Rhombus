{
    "name" : "sstablewriter_test",
    "replicationClass" : "SimpleStrategy",
    "replicationFactors" : {
        "replication_factor" : 1
    },
    "definitions" : [
        {
            "name": "non_uuid_pk",
            "allowNullPrimaryKeyInserts": true,
            "fields": [
                {"name": "id", "type": "varchar"},
                {"name": "value_1", "type": "varchar"},
                {"name": "value_2", "type": "varchar"},
                {"name": "created_at", "type": "varchar"}
            ],
            "indexes" : []
        }
    ]
}


