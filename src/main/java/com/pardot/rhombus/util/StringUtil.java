package com.pardot.rhombus.util;

import java.lang.Object;import java.lang.String;import java.lang.StringBuilder;
import java.util.List;
import java.util.Map;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 8/14/13
 */
public class StringUtil {

	public static String detailedMapToString(Map map) {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		for(Object key : map.keySet()) {
			sb.append("\t");
			sb.append(String.valueOf(key));
			sb.append(": (");
			Object value = map.get(key);
			sb.append((value==null)? "NULL":value.getClass().getName());
			sb.append(") ");
			sb.append((value==null)? "NULL":String.valueOf(value));
			sb.append("\n");
		}
		sb.append("}");
		return sb.toString();
	}

	public static String detailedListToString(List list) {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		for(Object value : list) {
			sb.append("\t");
			sb.append("(");
			sb.append((value==null)? "NULL":value.getClass().getName());
			sb.append(") ");
			sb.append((value==null)? "NULL":String.valueOf(value));
			sb.append("\n");
		}
		sb.append("}");
		return sb.toString();
	}
}
