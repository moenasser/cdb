package com.mnasser.io.cdb.client;

import static com.mnasser.io.cdb.client.CdbConstants.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import com.mnasser.io.ByteArrayReader;
import com.mnasser.io.ByteBuilder;

/**
 * Client Side Lookup class which speaks to CDB Map Lookup Server and queries against it for key-values.
 * <p>
 * Not thread safe.
 * 
 * @author mnasser
 */
public class CdbMapLookup implements MapLookup {

	private final String host;
	private final int port;
	
	private Socket s;
    private BufferedOutputStream out= null;
	private ByteArrayReader in = null;

	private ResultPolicy rp = new ResultPolicy();
	
	public CdbMapLookup(String host, int port){
		this.host = host;
		this.port = port;
	}

	public String getHost(){ return host; }
	public int getPort(){ return port; }

	
	public synchronized void connect(){
		try{

			//s = new Socket(host, port);
			SocketAddress sa = new InetSocketAddress(host, port); 
			s = new Socket();
			s.connect(sa, 6001); // 6 seconds
			
			
			in = new ByteArrayReader(s.getInputStream());
			out = new BufferedOutputStream(s.getOutputStream());
			
		}catch(UnknownHostException e){
			throw new RuntimeException(e);
		}catch(IOException e){
			throw new RuntimeException(e);
		}
	}
	
	public boolean isConnected(){
		return ( s != null && s.isConnected() );
	}

	public synchronized void close(){
		try {
			out.close();
			in.close();
			s.close();
		} catch (IOException e) {
			throw new RuntimeException("Failed to close connection : ", e);
		}
	}
	
	/**
	 * Queries remote connection and returns binary result.
	 * </p>
	 * The server will expect a '\n' terminated query string (meaning non-binary keys),
	 * but will respond with value length before value not terminated by '\n'.
	 * Query MUST be a properly constructed binary query string (starts with 'Q').
	 * @param q
	 * @param includeNL
	 * @return
	 * @throws IOException
	 */
	public byte[] queryBinary(byte[] prefix, byte[] key, byte action)  throws IOException {
		
		out.write(action);
		out.write(DELIM_REQ);
		out.write(prefix);
		out.write(key);
		out.write('\n');
		out.flush();
		
		binB.clear();
		binB.append( (byte)in.read() );
		
		if( binB.byteAt(0) == CdbConstants.rACK ){
			int len = byteArrayToInt(in.read(4));
			binB.append( in.read(len) );
			in.read();	// trailing newline from server
		}else{
			binB.append( in.readLine(false) ); // throw out the NL in binary mode
		}
		
		return rp.processResult( binB.getContent() );
	}

	
	private ByteBuilder binB = new ByteBuilder(); // byte buffer for binary query

	
	
	/**
	 * Given array of 4 bytes will return int value they hold 
	 * @param b
	 * @return
	 */
	static int byteArrayToInt(byte[] b){
		return ( b[0] & 0xFF ) 
			| ( (b[1] & 0xFF) << 8 )
			| ( (b[2] & 0xFF) << 16 )
			| ( (b[3] & 0xFF) << 24 ) ;
	}


	private static ByteBuilder psb = new ByteBuilder();
	
	/**
	 * Builds out the fist portion of a query request for this map
	 * @param mapDir
	 * @param mapType
	 * @return
	 */
	static byte[] buildQueryPrefix(String mapDir, int mapType){
		psb.clear();
		psb.append( mapDir.getBytes() )
		.append( DELIM_MAP )
		.append( Integer.toString(mapType).getBytes() )	   /* string representation */
		.append( DELIM_REQ );
		return psb.getContent();
	}
	
	/* (non-Javadoc)
	 * @see com.proclivitysystems.cdb.lookup.MapLookup#lookup(com.proclivitysystems.cdb.lookup.MapInfo, byte[])
	 */
	public byte[] lookup(MapInfo mi, byte[] key)  throws IOException {
		return queryBinary( mi.getQueryPrefix(), key, ACT_QUERY );
	}
	
	/* (non-Javadoc)
	 * @see com.proclivitysystems.cdb.lookup.MapLookup#lookupAll(com.proclivitysystems.cdb.lookup.MapInfo, byte[])
	 */
	public byte[] lookupAll(MapInfo mi, byte[] key) throws IOException{
		return queryBinary( mi.getQueryPrefix(), key, ACT_QUERY_ALL );
	}


	
	/**
	 * Builds a properly formed Update Request as per protocol.
	 * Format is 'u\tMAP_NAME|MAP_TYPE\tDFS_PATH\t(full|incremental)'
	 * @param mi
	 * @param path
	 * @param full
	 * @return
	 */
	public static byte[] buildUpdateRequest(MapInfo mi, String path, boolean full){
		ByteBuilder bb = new ByteBuilder();
		bb.append(ACT_UPDATE).append(DELIM_REQ).append(mi.getQueryPrefix());
		bb.append(path.getBytes())
		  .append( DELIM_REQ )
		  .append( (full)? "full".getBytes() : "incremental".getBytes() )
		  .append( (byte)'\n');
		return bb.getContent();
	}
	
	/* (non-Javadoc)
	 * @see com.proclivitysystems.cdb.lookup.MapLookup#updateMap(com.proclivitysystems.cdb.lookup.MapInfo, java.lang.String, boolean)
	 */
	public byte[] updateMap(MapInfo mi, String path, boolean full) throws IOException {
		out.write( buildUpdateRequest(mi, path, full));
		out.flush();
		return rp.processResult(in.readLine());
	}

	
	
	public static void main(String[] args) throws Exception {
		
		MapLookup[] lookups = new MapLookup[10];
		
		for( int ii = 0, len = lookups.length; ii < len ; ii++) {
			lookups[ii] = new CdbMapLookup(args[0], Integer.parseInt(args[1]));
			System.out.println("Loaded "+ ii);
			lookups[ii].connect();
			byte[] res = lookups[ii].lookup(new MapInfo("JCREW_EMAIL", 10), "moe.nasser@gmail.com".getBytes());
			if( res == null ) System.out.println("Got Nothing back.");
			else System.out.println("Value for moe.nasser@gmail.com: " + new String(res));
		}
		
		new Thread(){
			
			public Thread setRs(MapLookup[] rs){
				this.rs = rs;
				return this;
			}
			
			int count = 0;
			
			MapLookup[] rs;
			@SuppressWarnings("static-access")
			public void run() {
				while( true ){
					System.out.println("Still waiting for processing...");
					if ( done() ){
						System.out.println("We're all connected to something");
					}
					try{
						this.sleep(2000);
					}catch(InterruptedException ie){
					}
					
					count++;
					if( count > 10 ) break;
				}
			};
			
			private boolean done(){
				for( MapLookup r : rs){
					if ( ! r.isConnected() ) 
						return false;
				}
				return true;
			}
			
		}.setRs(lookups).start();
		
		
		
	}
}
