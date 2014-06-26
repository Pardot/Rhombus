package com.pardot.rhombus.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.util.UUID;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/6/13
 *
 UUID                   = time-low "-" time-mid "-"
 						  time-high-and-version "-"
 						  clock-seq-and-reserved
 						  clock-seq-low "-" node
 time-low               = 4hexOctet
 time-mid               = 2hexOctet
 time-high-and-version  = 2hexOctet
 clock-seq-and-reserved = hexOctet
 clock-seq-low          = hexOctet
 node                   = 6hexOctet
 hexOctet               = hexDigit hexDigit
 hexDigit =
 "0" / "1" / "2" / "3" / "4" / "5" / "6" / "7" / "8" / "9" /
 "a" / "b" / "c" / "d" / "e" / "f" /
 "A" / "B" / "C" / "D" / "E" / "F"

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                          time_low                             |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |       time_mid                |         time_hi_and_version   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |clk_seq_hi_res |  clk_seq_low  |         node (0-1)            |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                         node (2-5)                            |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

 time-low "-" time-mid "-" time-high-and-version "-" clock-seq-and-reserved + clock-seq-low "-" node

 */
public class UuidUtil {

	/**
	 * Generate a type 3 namespace uuid from an integer namespace and name
     * Our primary objective is to store and retrieve our name and namespace, and we don't care about collisions,
     * so we'll just shove the name and namespace values straight in rather than hashing them
     *
     * We will allow 4 bytes for the namespace and 8 bytes for the name. To avoid conflicts with reserved
     * version/variant bits, we will put the namespace data in the time_low field, 6 least significant bytes of the name data
     * in the node field, and the remaining 2 most significant name bytes in time_mid
     *
	 * @param namespace Integer representing the namespace
	 * @param name Long representing the name
	 * @return Type 3 UUID built from the namespace and name
	 */
	public static UUID namespaceUUID(Integer namespace, Long name) {
		//Create our msb and lsb return buffers
		ByteBuffer msb = ByteBuffer.allocate(8);
        msb.order(ByteOrder.BIG_ENDIAN);
		ByteBuffer lsb = ByteBuffer.allocate(8);
        msb.order(ByteOrder.BIG_ENDIAN);

        // Insert the 4 byte namespace into the time_low field
        msb.putInt(0, namespace);

        // Slice off the most significant two bytes of name
        char nameHigh = (char)(name >>> 48);
        // Push the 2 MSB bytes of name into the time_mid field
        msb.putChar(4, nameHigh);

        //Set the four most significant bits of the time_hi_and_version field to the 4 bit version number
        //(00110000 = 48)
        msb.put(7, (byte) 48);

        // Grab the least significant six bytes of name
        char nameMid = (char)(name >>> 32);
        int nameLow = (int)(long)name;
        // Shove them into the node field
        lsb.putChar(2, nameMid);
        lsb.putInt(4, nameLow);

        //Set the two most significant bits to 01 (01000000 = 64)
        lsb.put(0, (byte)64);

		return new UUID(msb.getLong(), lsb.getLong());
	}

	/**
	 * Retrieve the integer namespace from a namespace uuid generated using this class
	 * @param uuid UUID generated using
	 * @return Namespace retrieved from the UUID
	 */
	public static Integer namespaceFromUUID(UUID uuid) {
        return (int)(uuid.getMostSignificantBits() >>> 32);
	}

	/**
	 * Retrieve the integer name from a namespace uuid generated using this class
	 * @param uuid UUID generated using
	 * @return Name retrieved from the UUID
	 */
	public static Long nameFromUUID(UUID uuid) {
        char msb = (char)((uuid.getMostSignificantBits() >>> 16) & 0xffff);
        long out = uuid.getLeastSignificantBits() & 0xffffffffffffL;
        return out | ((long)msb << 48);
	}

	static final long NUMBER_OF_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	public static DateTime getDateFromUUID(UUID uuid) {
		return new DateTime(convertUUIDToJavaMillis(uuid), DateTimeZone.UTC);
	}

	public static Long convertUUIDToJavaMillis(UUID uuid){
		return (uuid.timestamp() - NUMBER_OF_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}


}
