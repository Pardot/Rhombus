package com.pardot.rhombus.util;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;

import java.io.IOException;

/**
 * User: Rob Righter
 * Date: 10/4/13
 */
public class CDataTypeSerializer extends JsonSerializer<CField.CDataType> {
	@Override
	public void serialize(CField.CDataType value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
	{
		jsonGenerator.writeObject(value.toString());
	}
}
