package com.pardot.rhombus.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: Rob Righter
 * Date: 10/4/13
 */
public class MapToListSerializer extends JsonSerializer<Map<String, Object >>
{
	public MapToListSerializer(){
		super();
	}

	@Override
	public void serialize(Map<String, Object> value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
	{
		List<Object> ret = Lists.newArrayList();
		for(String key: value.keySet()){
			ret.add(value.get(key));
		}
		jsonGenerator.writeObject(ret);
	}
}
