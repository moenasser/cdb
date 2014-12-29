package com.mnasser.io.cdb.client;

import java.util.Arrays;


/**
 * Based on what CdbLookupService returns to remote clients,
 * we can process the result in a certain way.
 * @author mnasser
 *
 */
public class ResultPolicy {

	protected byte rcode;
	
	public byte[] processResult( byte[] res ){
		if( res == null )
			return processNull(res);
		
		if( res.length == 0 )
			return processEmpty(res);
		
		rcode = res[0];
		
		if( rcode == CdbConstants.rACK )
			return processACK(res);
			
		if( rcode == CdbConstants.rNAK )
			return processNAK(res);
		
		if( rcode == CdbConstants.rEXP )
			return processEXP(res);
		
		return processUnknown(res);
	}
	
	protected byte[] processNull   (byte[] res) {
		throw new RuntimeException("RemoteLookup got NULL when looking for a response!"); 
	}
	
	protected byte[] processEmpty  (byte[] res) { return null; }
	protected byte[] processNAK    (byte[] res) { return null; }
	protected byte[] processACK    (byte[] res) {
		return Arrays.copyOfRange(res, 1, res.length);
	}
	
	protected byte[] processEXP    (byte[] res) {
		throw new RuntimeException("RemoteLookup exception occured! "+ new String(res));
	}
	protected byte[] processUnknown(byte[] res) {
		throw new RuntimeException("RemoteLookup got UNKNOWN result occured! "+ new String(res));		
	}
}
