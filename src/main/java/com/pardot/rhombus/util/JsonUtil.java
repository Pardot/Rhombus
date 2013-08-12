package com.pardot.rhombus.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.primitives.*;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 4/23/13
 */
public class JsonUtil {

	public static <T> T objectFromJsonResource(Class<T> objectClass, ClassLoader resourceClassLoader, String resourceLocation) throws IOException {
		ObjectMapper om = new ObjectMapper();
		InputStream inputStream = resourceClassLoader.getResourceAsStream(resourceLocation);
		T returnObject = om.readValue(inputStream, objectClass);
		inputStream.close();
		return returnObject;
	}

	public static List<Map<String, Object>> rhombusMapFromResource(ClassLoader resourceClassLoader, String resourceLocation) throws IOException {
		ObjectMapper om = new ObjectMapper();
		InputStream inputStream = resourceClassLoader.getResourceAsStream(resourceLocation);
		MapContainer mc = om.readValue(inputStream, MapContainer.class);
		inputStream.close();
		return mc.getValues();
	}

	public static SortedMap<String, Object> rhombusMapFromJsonMap(Map<String, Object> jsonMap, CDefinition definition) {
		SortedMap<String, Object> rhombusMap = Maps.newTreeMap();
		for(CField field : definition.getFields().values()) {
			Object jsonValue = jsonMap.get(field.getName());
			if(jsonValue != null) {
				rhombusMap.put(field.getName(), typedObjectFromValueAndField(jsonValue, field));
			}
		}
		return rhombusMap;
	}

	protected static Object typedObjectFromValueAndField(Object jsonValue, CField field) throws IllegalArgumentException {
		if(jsonValue == null) {
			return null;
		}
        try {
            switch(field.getType()) {
                case ASCII:
                case VARCHAR:
                case TEXT:
                    String parsedString = String.valueOf(jsonValue);
                    if (parsedString == null) {
                        throw new IllegalArgumentException();
                    } else {
                        return parsedString;
                    }
                case BIGINT:
                case COUNTER:
                    return longFromNumber(jsonValue);
                case BLOB:
                    throw new IllegalArgumentException();
                case BOOLEAN:
                    if (String.class.isAssignableFrom(jsonValue.getClass())) {
                        return Boolean.valueOf((String)jsonValue);
                    } else if(Boolean.class.isAssignableFrom(jsonValue.getClass())){
                        return jsonValue;
                    }else {
                        throw new IllegalArgumentException();
                    }
                case DECIMAL:
                    if (String.class.isAssignableFrom(jsonValue.getClass())) {
                        return new BigDecimal((String)jsonValue);
                    } else if(Integer.class.isAssignableFrom(jsonValue.getClass())){
                        return BigDecimal.valueOf((Integer)jsonValue);
                    } else if(Long.class.isAssignableFrom(jsonValue.getClass())){
                        return BigDecimal.valueOf((Long)jsonValue);
                    } else if(Float.class.isAssignableFrom(jsonValue.getClass())){
                        return BigDecimal.valueOf((Float)jsonValue);
                    } else if(Double.class.isAssignableFrom(jsonValue.getClass())) {
                        return BigDecimal.valueOf((Double)jsonValue);
                    }else {
                        throw new IllegalArgumentException();
                    }
                case DOUBLE:
                    if (String.class.isAssignableFrom(jsonValue.getClass())) {
                        Double parsedNumber = Doubles.tryParse((String) jsonValue);
                        if (parsedNumber != null) {
                            return parsedNumber;
                        }
                    } else if(Integer.class.isAssignableFrom(jsonValue.getClass())){
                        return Double.valueOf((Integer)jsonValue);
                    } else if(Long.class.isAssignableFrom(jsonValue.getClass())){
                        return Double.valueOf((Long)jsonValue);
                    } else if(Double.class.isAssignableFrom(jsonValue.getClass())){
                        return jsonValue;
                    }
                    else if(Float.class.isAssignableFrom(jsonValue.getClass())){
                        return Double.valueOf((Float)jsonValue);
                    }
                    throw new IllegalArgumentException();
                case FLOAT:
                    if (String.class.isAssignableFrom(jsonValue.getClass())) {
                        Float parsedNumber = Floats.tryParse((String) jsonValue);
                        if (parsedNumber != null) {
                            return parsedNumber;
                        }
                    } else if(Integer.class.isAssignableFrom(jsonValue.getClass())){
                        return Float.valueOf((Integer)jsonValue);
                    } else if(Long.class.isAssignableFrom(jsonValue.getClass())){
                        return Float.valueOf((Long)jsonValue);
                    } else if(Double.class.isAssignableFrom(jsonValue.getClass())){
                        return ((Double)jsonValue).floatValue();
                    }
                    else if(Float.class.isAssignableFrom(jsonValue.getClass())){
                        return jsonValue;
                    }
                    throw new IllegalArgumentException();
                case INT:
                    return intFromNumber(jsonValue);
                case TIMESTAMP:
                    if(Date.class.isAssignableFrom(jsonValue.getClass())) {
                        return jsonValue;
                    } else if(Integer.class.isAssignableFrom(jsonValue.getClass())){
                        return new Date((Integer)jsonValue);
                    } else if(Long.class.isAssignableFrom(jsonValue.getClass())) {
                        return new Date((Long)jsonValue);
                    } else {
                        throw new IllegalArgumentException();
                    }
                case UUID:
                case TIMEUUID:
                    if(UUID.class.isAssignableFrom(jsonValue.getClass())) {
                        return jsonValue;
                    } else if(String.class.isAssignableFrom(jsonValue.getClass())){
                        return UUID.fromString((String)jsonValue);
                    } else {
                        throw new IllegalArgumentException();
                    }
                case VARINT:
                    if(String.class.isAssignableFrom(jsonValue.getClass())) {
                        return new BigInteger((String)jsonValue);
                    }
                    return BigInteger.valueOf(longFromNumber(jsonValue));
                default:
                    return null;
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unable to convert "+ jsonValue + " of type "+jsonValue.getClass()+" to C* type " + field.getType().toString());
        }
	}

	private static Long longFromNumber(Object number) {
		if(Boolean.class.isAssignableFrom(number.getClass())) {
			return ((Boolean)number ? 1L : 0L);
        } else if(String.class.isAssignableFrom(number.getClass())) {
            Long parsedNumber = Longs.tryParse((String) number);
            if (parsedNumber != null) {
                return parsedNumber;
            }
        } else if(Double.class.isAssignableFrom(number.getClass())) {
            return ((Double)number).longValue();
        } else if(Float.class.isAssignableFrom(number.getClass())) {
            return ((Float)number).longValue();
		} else if(Integer.class.isAssignableFrom(number.getClass())) {
			return ((Integer)number).longValue();
		} else if(Long.class.isAssignableFrom(number.getClass())) {
			return (Long)number;
        }
        throw new IllegalArgumentException();
	}

	private static Integer intFromNumber(Object number) {
		if(Boolean.class.isAssignableFrom(number.getClass())) {
			return ((Boolean)number ? 1 : 0);
        } else if(String.class.isAssignableFrom(number.getClass())) {
            Integer parsedNumber = Ints.tryParse((String) number);
            if (parsedNumber != null) {
                return parsedNumber;
            }
        } else if(Double.class.isAssignableFrom(number.getClass())) {
            return ((Double)number).intValue();
        } else if(Float.class.isAssignableFrom(number.getClass())) {
            return ((Float)number).intValue();
        } else if(Long.class.isAssignableFrom(number.getClass())) {
            return ((Long)number).intValue();
		} else if(Integer.class.isAssignableFrom(number.getClass())) {
			return (Integer)number;
        }
        throw new IllegalArgumentException();
	}

}
