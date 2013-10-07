package com.pardot.rhombus.util;

import com.google.common.collect.Lists;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

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
