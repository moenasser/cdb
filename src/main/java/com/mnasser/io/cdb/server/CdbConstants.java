package com.mnasser.io.cdb.server;

/**
 * Constants used by CdbLookupService protocol
 * 
 * @author mnasser
 */
public interface CdbConstants {
	
	/*Separates each part of the request*/
	public static final byte DELIM_REQ   = '\t';
	
	/*Separates map_name from map_type in mapinfo request part*/
	public static final byte DELIM_MAP   = '|';
	
	public static final byte ACT_QUERY_ASCII   = 'q'; /*query ascii (nl-terminated) */
	public static final byte ACT_QUERY   = 'Q';   /*query binary*/
	public static final byte ACT_QUERY_ALL = 'M'; /*query binary mode*/
	public static final byte ACT_KILL    = 'k'; /*kill map*/
	public static final byte ACT_STAT    = 's'; /*map stats*/
	public static final byte ACT_UPDATE  = 'u'; /*rebuild/upload map*/
	
	/** Acknowledgment.  4 byte INT size of payload plus payload follows */
	public static final byte rACK = "a".getBytes()[0];
	
	/** Negative acknowledgment: CDB does not contain this key*/
	public static final byte rNAK = "n".getBytes()[0];
	
	/** Server side Exception occurred */
	public static final byte rEXP = "x".getBytes()[0];
}
