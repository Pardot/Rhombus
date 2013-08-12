package com.pardot.rhombus;

import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CObjectParseException;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class JsonUtilTest extends JsonUtil{

    @Test
    public void typedObjectFromStringAndVarchar() throws CObjectParseException {
        CField field = new CField("test", "varchar");
        String jsonValue = "123456789";
        String expected = "123456789";

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromIntegerAndVarchar() throws CObjectParseException {
        CField field = new CField("test", "varchar");
        Integer jsonValue = 123456789;
        String expected = "123456789";

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromDateAndVarchar() throws CObjectParseException {
        CField field = new CField("test", "varchar");
        Date jsonValue = new Date(1376079901000L);
        String expected = "Fri Aug 09 16:25:01 EDT 2013";

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result.toString());
    }

    @Test
    public void typedObjectFromIntegerAndBigint() throws CObjectParseException {
        CField field = new CField("test", "bigint");
        Integer jsonValue = 1234567890;
        Long expected = 1234567890L;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLongAndBigint() throws CObjectParseException {
        CField field = new CField("test", "bigint");
        Long jsonValue = 123456789012345L;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(jsonValue, result);
    }

    @Test
    public void typedObjectFromFloatAndBigint() throws CObjectParseException {
        CField field = new CField("test", "bigint");
        Float jsonValue = 1234.56f;
        Long expected = 1234L;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromDoubleAndBigint() throws CObjectParseException {
        CField field = new CField("test", "bigint");
        Double jsonValue = 1234567890.1234567;
        Long expected = 1234567890L;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLegitStringAndBigint() throws CObjectParseException {
        CField field = new CField("test", "bigint");
        String jsonValue = "1234567890";
        Long expected = 1234567890L;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromCrapStringAndBigint() throws CObjectParseException {
        CField field = new CField("test", "bigint");
        String jsonValue = "I like ponies";

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromIntegerAndBoolean() throws CObjectParseException {
        CField field = new CField("test", "boolean");
        Integer jsonValue = 1234;

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }

    @Test
    public void typedObjectFromTrueStringAndBoolean() throws CObjectParseException {
        CField field = new CField("test", "boolean");
        String jsonValue = "true";
        Boolean expected = true;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromNumericalStringAndBoolean() throws CObjectParseException {
        CField field = new CField("test", "boolean");
        String jsonValue = "1";
        Boolean expected = false;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLegitStringAndDecimal() throws CObjectParseException {
        CField field = new CField("test", "decimal");
        String jsonValue = "1234567890.12345678";
        BigDecimal expected = new BigDecimal("1234567890.12345678");

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromCrapStringAndDecimal() throws CObjectParseException {
        CField field = new CField("test", "decimal");
        String jsonValue = "I like ponies";

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }

    @Test
    public void typedObjectFromIntAndDecimal() throws CObjectParseException {
        CField field = new CField("test", "decimal");
        Integer jsonValue = 1234567890;
        BigDecimal expected = BigDecimal.valueOf(jsonValue);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLongAndDecimal() throws CObjectParseException {
        CField field = new CField("test", "decimal");
        Long jsonValue = 123456789012345L;
        BigDecimal expected = BigDecimal.valueOf(jsonValue);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromFloatAndDecimal() throws CObjectParseException {
        CField field = new CField("test", "decimal");
        Float jsonValue = 1234.56f;
        BigDecimal expected = BigDecimal.valueOf(jsonValue);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromDoubleAndDecimal() throws CObjectParseException {
        CField field = new CField("test", "decimal");
        Double jsonValue = 1234567890.1234567;
        BigDecimal expected = BigDecimal.valueOf(jsonValue);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLegitStringAndDouble() throws CObjectParseException {
        CField field = new CField("test", "double");
        String jsonValue = "1234567890.12345678";
        double expected = 1234567890.12345678;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromCrapStringAndDouble() throws CObjectParseException {
        CField field = new CField("test", "double");
        String jsonValue = "I like ponies";

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }

    @Test
    public void typedObjectFromIntAndDouble() throws CObjectParseException {
        CField field = new CField("test", "double");
        Integer jsonValue = 1234567890;
        double expected = 1234567890;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLongAndDouble() throws CObjectParseException {
        CField field = new CField("test", "double");
        Long jsonValue = 123456789012345L;
        double expected = 123456789012345L;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromFloatAndDouble() throws CObjectParseException {
        CField field = new CField("test", "double");
        Float jsonValue = 1234.56f;
        double expected = 1234.56;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertTrue(Double.class.isAssignableFrom(result.getClass()));
        assertEquals(expected, (Double)result, jsonValue * (1E-6));
    }

    @Test
    public void typedObjectFromDoubleAndDouble() throws CObjectParseException {
        CField field = new CField("test", "double");
        Double jsonValue = 1234567890.1234567;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(jsonValue, result);
    }

    @Test
     public void typedObjectFromLegitStringAndFloat() throws CObjectParseException {
        CField field = new CField("test", "float");
        String jsonValue = "1234567890.12345678";
        float expected = 1234567940f;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromCrapStringAndFloat() throws CObjectParseException {
        CField field = new CField("test", "float");
        String jsonValue = "I like ponies";

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }

    @Test
    public void typedObjectFromIntAndFloat() throws CObjectParseException {
        CField field = new CField("test", "float");
        Integer jsonValue = 1234567890;
        float expected = 1234567890f;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLongAndFloat() throws CObjectParseException {
        CField field = new CField("test", "float");
        Long jsonValue = 123456789012345L;
        float expected = 123456789012345f;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromFloatAndFloat() throws CObjectParseException {
        CField field = new CField("test", "float");
        float jsonValue = 1234.56f;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(jsonValue, result);
    }

    @Test
    public void typedObjectFromDoubleAndFloat() throws CObjectParseException {
        CField field = new CField("test", "float");
        double jsonValue = 1234.5678901234567;
        float expected = 1234.5678901234567f;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertTrue(Float.class.isAssignableFrom(result.getClass()));
        assertEquals(expected, (Float)result, jsonValue * (1E-6));
    }

    @Test
    public void typedObjectFromBooleanAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        Boolean jsonValue = true;
        Integer expected = 1;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLegitStringAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        String jsonValue = "1234567890";
        Integer expected = 1234567890;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromCrapStringAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        String jsonValue = "I like ponies";

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }

    @Test
    public void typedObjectFromIntAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        Integer jsonValue = 1234567890;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(jsonValue, result);
    }

    @Test
    public void typedObjectFromLongAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        Long jsonValue = 1234567L;
        Integer expected = 1234567;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromFloatAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        Float jsonValue = 1234.56f;
        Integer expected = 1234;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromDoubleAndInteger() throws CObjectParseException {
        CField field = new CField("test", "int");
        double jsonValue = 1234.5678901234567;
        Integer expected = 1234;

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromIntAndTimestamp() throws CObjectParseException {
        CField field = new CField("test", "timestamp");
        Integer jsonValue = 1376079900;
        Date expected = new Date(jsonValue);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLongAndTimestamp() throws CObjectParseException {
        CField field = new CField("test", "timestamp");
        Long jsonValue = 1376079900000L;
        Date expected = new Date(jsonValue);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromDateAndTimestamp() throws CObjectParseException {
        CField field = new CField("test", "timestamp");
        Date jsonValue = new Date(1376079900000L);

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(jsonValue, result);
    }

    @Test
    public void typedObjectFromStringAndUUID() throws CObjectParseException {
        CField field = new CField("test", "uuid");
        UUID expected = UUID.randomUUID();
        String jsonValue = expected.toString();

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(result, result);
    }

    @Test
    public void typedObjectFromUUIDAndUUID() throws CObjectParseException {
        CField field = new CField("test", "uuid");
        UUID jsonValue = UUID.randomUUID();

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(jsonValue, result);
    }

    @Test
    public void typedObjectFromIntegerAndVarint() throws CObjectParseException {
        CField field = new CField("test", "varint");
        Integer jsonValue = 1234567890;
        BigInteger expected = new BigInteger("1234567890");

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLongAndVarint() throws CObjectParseException {
        CField field = new CField("test", "varint");
        Long jsonValue = 1234567890123456789L;
        BigInteger expected = new BigInteger("1234567890123456789");

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromFloatAndVarint() throws CObjectParseException {
        CField field = new CField("test", "varint");
        Float jsonValue = 1234.56f;
        BigInteger expected = new BigInteger("1234");

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromDoubleAndVarint() throws CObjectParseException {
        CField field = new CField("test", "varint");
        Double jsonValue = 1234567890.1234567;
        BigInteger expected = new BigInteger("1234567890");

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test
    public void typedObjectFromLegitStringAndVarint() throws CObjectParseException {
        CField field = new CField("test", "varint");
        String jsonValue = "1234567890123456789012345";
        BigInteger expected = new BigInteger("1234567890123456789012345");

        Object result = JsonUtil.typedObjectFromValueAndField(jsonValue, field);
        assertEquals(expected, result);
    }

    @Test(expected=IllegalArgumentException.class)
    public void typedObjectFromCrapStringAndVarint() throws CObjectParseException {
        CField field = new CField("test", "varint");
        String jsonValue = "I like ponies";

        JsonUtil.typedObjectFromValueAndField(jsonValue, field);
    }
    
}
