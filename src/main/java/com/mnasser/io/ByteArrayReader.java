package com.mnasser.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Streams in data and exposes a readLine() method which returns lines of text
 * as a byte[] w/o ever using Strings.
 * 
 * @author mnasser
 */
public class ByteArrayReader implements Closeable{

	// i/o
	private InputStream in = null;
	private ByteBuilder bb = new MutableByteBuilder(1024);
	private byte[] temp = new byte[bb.getCapacity()];
	
	public ByteArrayReader(InputStream is) {
		in  = is;
	}
	public ByteArrayReader(FileInputStream is) {
		in = new BufferedInputStream(is);
	}
	
	/**
	 * Returns access to the original input stream
	 * @return
	 */
	public InputStream getInputStream(){
		return in;
	}
	
	/**
	 * Fills ByteBuilder to capacity. 
	 * @return
	 * @throws Exception
	 */
	private int fill() throws IOException {
		int space = bb.space();
		if( space == 0 ) return 0;
		
		int got = in.read(bb.getContent(), bb.length(), space);
		if ( got != -1 ) bb.incrLength(got);
		
		//int got = in.read(temp, 0, Math.min(space,temp.length));
		//if ( got != -1 ) bb.append(temp, got);
		
		return got;
	}

	
	/**
	 * Reads bytesize number of bytes into byte builder.
	 * @param bytesize
	 * @return number of bytes read
	 * @throws IOException
	 */
	private int _read(int bytesize) throws IOException {
		int got = in.read(temp,0, ((bytesize>temp.length) ? temp.length:bytesize) );
		if ( got != -1 ) bb.append(temp, got);
		return got;
	}
	/**
	 * Reads upto temp buffer size bytes into temp buffer
	 * Expands byte builder by number of bytes read
	 * @return number of bytes read
	 * @throws Exception
	 */
	private int _read() throws IOException {
		return _read(temp.length);
	}
	/**
	 * Returns the next num bytes from input stream.
	 * Blocks if there isn't enough data.
	 * @return byte array of size <code>num</code>
	 * @throws IOException if the stream is closed or -1 bytes is 
	 * read from stream.
	 */
	public byte[] read(int num) throws IOException {
		while ( bb.length() < num ) {
			if( _read(num-bb.length()) == -1 ){
				throw new RuntimeException("Asked to read "+num+" bytes, but stream returned -1!");
			}
		}
		
		byte[] read_bytes = bb.subSequence(0, num);
		bb.delete(0, num);
		
		return read_bytes;
	}
	
	
	/**
	 * Returns the next byte from the input stream.
	 * Blocks if there isn't enough data.
	 * @return next byte read as an int 
	 * @throws IOException if the stream is closed or -1 bytes is 
	 * read from stream.
	 */
	public int read() throws IOException {
		if( ! bb.isEmpty() ){
			byte r_b = bb.byteAt(0);
			bb.delete(0, 1);
			return r_b;
		}
		return in.read();
	}

	
	
	private int LF=0;
	private int n=0;
	private int r=0;
	private int got=0;
	private int lf_size=0;
	private byte[] res = null;
	
	private int total_lines = 0;
	private long total_bytes = 0;
	
	private final byte NL = (byte)'\n';
	
	/**
	 * Returns byte[] representing the next line of text not including a line terminator. 
	 * Keeps filling internal ByteBuilder until a new line is available
	 * @return
	 * @throws Exception
	 */
	public byte[] readLine() throws IOException{
		return readLine(false);
	}
	/**
	 * Returns byte[] representing the next line of text. 
	 * Keeps filling internal ByteBuilder until a new line is available.
	 * @param appendNL Whether or not to include a proper line terminator in result.
	 * @return A set of bytes including from 
	 * @throws Exception
	 */
	public byte[] readLine(boolean appendNL) throws IOException{
		// if bb isn't full, fill it.
		// if bb doesn't have \n or \r - grow it
		// grab everything before \n or \r
		// downsize bb and return line
		
		LF = 0;
		got = 0;
		
		if( ! bb.isEmpty() && bb.hasRemaining() ){
			got = fill(); // fill
		}
		
		
		n = 0; r = 0;
		do {
			n = bb.indexOf((byte)'\n', LF);
			if ( n == -1 ){
				r = bb.indexOf((byte)'\r', LF);
				if ( r == -1 ) {
					got = _read(); // keep reading until linefeed or EOF
				}
			}
		}while( n == -1 && r == -1 && got != -1 );
		
		
		lf_size = 1;
		
		if( n != -1 ){ // check for \r before \n
			LF = n;
			if( n-1 > 0 && bb.byteAt(n-1) == (byte)'\r' ){
				LF = n-1;
				lf_size++;
			}
		}else if ( r != -1 ){
			LF = r;
		}else if ( got == -1 ){
			// EOF no more data - return what you got if anything
			if( bb.isEmpty() )
				return null;
			else{
				LF = bb.length();
				if( appendNL ) bb.append((byte)'\n'); // no NL found - set one 
				//appendNL = false;
			}
		}

		
		if( appendNL ) bb.setByteAt(LF,NL);
		res = bb.subSequence(0, LF );
		bb.delete(0, LF+lf_size);
		
		total_lines++;
		total_bytes += LF+lf_size+1;
		
		return res;
	}
	
	/**
	 * Current number of lines read so far.
	 * @return
	 */
	public int linesRead(){
		return total_lines;
	}
	/**
	 * Current number of bytes read so far.
	 * @return
	 */
	public long bytesRead(){
		return total_bytes;
	}
	
	@Override
	public void close() throws IOException{
		in.close();
	}
}
