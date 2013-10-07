package com.pardot.rhombus.util;

import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.shardingstrategy.TimebasedShardingStrategy;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Rob Righter
 * Date: 10/4/13
 */
public class ShardStrategySerializer extends JsonSerializer<TimebasedShardingStrategy> {
	@Override
	public void serialize(TimebasedShardingStrategy value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
	{
		Map<String,String> ret = Maps.newHashMap();
		ret.put("type",value.getClass().getName().replaceFirst("com.pardot.rhombus.cobject.shardingstrategy.", ""));
		jsonGenerator.writeObject(ret);
	}

}