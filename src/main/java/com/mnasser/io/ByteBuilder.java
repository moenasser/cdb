package com.mnasser.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a modifiable sequence of bytes. 
 * <p>
 * Acts in much the same way StringBuilder does as a sequence of characters.</br>
 * Automatically grows the backing data structure as needed.
 * <p>
 * NOTE: Uses Arrays.copyOf() to keep internal data structure immutable, but 
 * shouldn't be considered thread safe.
 * @author mnasser
 *
 */
public class ByteBuilder implements Comparable<ByteBuilder>{

	protected byte[] b;
	protected int capacity;
	protected int position = 0;
	
	public ByteBuilder() {
		this(256);
	}
	public ByteBuilder(int initCapacity){
		capacity = initCapacity;
		b = new byte[capacity];
	}
	public ByteBuilder(byte[] bb){
		b = Arrays.copyOf(bb, ( bb.length < 256 )? 256 : bb.length );
		capacity = b.length;
		position = bb.length; // original byte array length
	}
	public ByteBuilder(ByteBuilder bb){
		position = bb.position;
		b = Arrays.copyOf(bb.b, bb.length());
		capacity = bb.position;
	}

	/**
	 * Replaces the contents of this byte builder's backing store with the 
	 * contents of the given byte array.
	 * @param bb
	 */
	public void reset(byte[] bb){
		b = Arrays.copyOf(bb, bb.length);
		capacity = b.length;
		position = b.length;
	}
	
	/**
	 * If there is space in the backing store for more data before needing to
	 * expand the capacity of the backing store.
	 * @return True if, and only if, there is any space remaining between the 
	 * current position and the end of the last. False otherwise.
	 */
	public boolean hasRemaining(){
		return position < capacity;
	}
	
	/**
	 * Adds a byte to this byte array builder 
	 * @param bite
	 */
	public ByteBuilder append(byte bite){
		if( position >= capacity)
			expandCapacity(position);
		b[position++] = bite;
		return this;
	}
	/**
	 * Adds a byte[] to this byte array builder 
	 * @param bite
	 */
	public ByteBuilder append(byte[] bb){
		if( position + bb.length >= capacity)
			expandCapacity(position + bb.length);
		for( byte bite : bb ){
			b[position++] = bite;
		}
		return this;
	}
	/**
	 * Appends the first <code>length</code> number of bytes from <code>bb</code> to this
	 * byte builder.
	 * @param bb byte array whose content is to be append to this sequence of bytes
	 * @param length Number of bytes first 
	 * @return
	 */
	public ByteBuilder append(byte[] bb, int length) {
		if( position + length > capacity){
			expandCapacity(position + length);
		}
		for( int ii=0; ii < length; ii++ ){
			b[position++] = bb[ii];
		}
		return this;
	}

	protected void expandCapacity(int minimumCapacity){
		int newCapacity = getNextCapacitySize(b.length);
		if (minimumCapacity > newCapacity) {
			newCapacity = minimumCapacity;
		}
        if (newCapacity < 0) {
            newCapacity = Integer.MAX_VALUE;
        }
        b = Arrays.copyOf(b, newCapacity);
        capacity = newCapacity;
	}
	static int getNextCapacitySize(int len){
		if( len < 256 )
			return len * 2;   //power of 2
		return len + len / 2; // grow by 50% instead?
	}
	
	public byte byteAt(int i) {
		if( i > position ) throw new IndexOutOfBoundsException("Trying to get an index ("+i+") greater than length : " + position);
		return b[i];
	}
	
	public void setByteAt(int i, byte bite){
		if( i > position ) throw new IndexOutOfBoundsException("Trying to set an index ("+i+") greater than length : " + position);
		b[i] = bite;
	}
	
	/**
	 * Returns index of first occurance of given byte in this byte array.
	 * @param bite
	 * @return
	 */
	public int indexOf(byte bite) {
		return indexOf(bite, 0);
	}
	/**
	 * Returns index first occurance of given byte starting from the given index
	 * inclusive
	 * @param bite
	 * @param start
	 * @return
	 */
	public int indexOf(byte bite, int start) {
		if( start >= position ) return -1;
		for( int i = start; i < position; i++){
			if ( b[i] == bite ) return i;
		}
		return -1;
	}
	
	/**
	 * Splits the contents of this byte array around all occurances of the given
	 * delimiter.
	 * @param delim
	 * @return List of byte[]
	 */
	public List<byte[]> split(byte delim){

		List<byte[]> lb = new ArrayList<byte[]>();
		
		int idx = 0, offset = 0;
		while( (idx = indexOf(delim,offset)) != -1 ){
			lb.add(Arrays.copyOfRange(b, offset, idx));
			offset = idx + 1;
		}
		if( offset < position)
			lb.add(Arrays.copyOfRange(b, offset, position));
		return lb;
	}

	/**
	 * Gets a copy of the current contents of the array
	 * @return
	 */
	public byte[] getContent(){
		return Arrays.copyOf(b, position);
	}
	/**
	 * Gets the current contents of the array and resets the array to the beginning.
	 * @return Copy of the current byte array upto position
	 */
	public void clear(){
		position = 0;
		/*
		int p = position;
		return Arrays.copyOf(b, p);
		*/
	}
	
	/** Current size in bytes in this byte builder **/
	public int length(){
		return position;
	}
	
	public int getCapacity(){
		return capacity;
	}
	
	/**
	 * True if this sequence of bytes has zero data, false otherwise.
	 * @return
	 */
	public boolean isEmpty(){
		return position == 0;
	}

	/**
	 * Amount you can fill this builder by before it needs to grow its backing store.
	 * @return size in bytes this builder can append data before reaching capacity.
	 */
	public int space() {
		return capacity - position;
	}
	
	/**
	 * Currently not implemented, but left for subclasses to extend as needed.
	 * Increments the current length of the sequence.
	 * This may be needed if you wish to manually fill the content 
	 * of a byte array returned using <code>getContent()</code>.
	 * @param p size in bytes to increment the current length.
	 * @throws UnsupportedOperationException. Subclass may overwrite as desired. 
	 * @see MutableByteBuilder.getContent()
	 */
	public void incrLength(int p) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return new String(b, 0, position);
	}
	
	/**
	 * Compares if the byte array is logically equivalent to the ASCII string
	 * given.
	 * @param bb byte array in question
	 * @param s string of only ascii characters to compare to
	 */
	public static boolean asciiCompare(byte[] bb, String s){
		if( bb.length != s.length() ) return false;
		char[] cc = s.toCharArray();
		for( int ii = 0, len = bb.length; ii < len ; ii++){
			if( (byte)cc[ii] != bb[ii] )
				return false;
		}
		return true;
	}
	
	public boolean asciiCompare(String s){
		if( position != s.length() ) return false;
		char[] cc = s.toCharArray();
		for( int ii = 0; ii < position; ii++){
			if( (byte)cc[ii] != b[ii] )
				return false;
		}
		return true;
	}

	/**
	 * Convenience method to test for equality of two byte arrays.
	 * @param left
	 * @param right
	 * @return
	 */
	public static boolean equals(byte[] left, byte[] right){
		if( left.length != right.length )return false;
		for( int ii = 0, len = left.length; ii < len; ii++){
			if( left[ii] != right[ii] )
				return false;
		}
		return true;
	}
	
	public boolean equals(byte[] bb){
		if( position != bb.length )return false;
		for( int ii = 0; ii < position; ii++){
			if( bb[ii] != b[ii] )
				return false;
		}
		return true;
	}
	
	@Override
	public boolean equals(Object obj) {
		if( obj instanceof ByteBuilder ){
			ByteBuilder other = (ByteBuilder)obj;
			return equals(other.b);
		}
		if( obj instanceof byte[] ){
			return equals((byte[]) obj);
		}
		return false;
	}
	/**
	 * Removes `len` bytes starting from the start index. 
	 * Shifts all bytes 'len' positions starting at the given start index
	 * effectively deleteing them.
	 * @param idx Start index 
	 * @param len Number of bytes to remove inclusive of the start index
	 * @return
	 */
	public ByteBuilder delete(int idx, int len) {
		if( idx >= position ) return this;
		if( idx + len >= position ){
			position = idx;
			return this;
		}
		for( ; (idx+len)<position ; idx++){
			b[idx] = b[idx + len];
		}
		position = idx;
		return this;
	}
	/**
	 * Deletes everything beyond the index given inclusive.  
	 * @param i
	 * @return 
	 */
	public ByteBuilder truncate(int i){
		if( i < position ) position = i;
		return this;
	}
	
	/**
	 * Slices out the the desired byte range. 
	 * @param off
	 * @param len
	 * @return byte array that corresponds to the byte range pruned. 
	 *
	public byte[] prune(int idx, int len){
		byte[] res = subSequence(idx, idx+len);
		delete(idx, len);
		return res;
	}
	**/
	
	/**
	 * Converts this byte array as a character array.
	 * @return
	 */
	public char[] asCharArray(){
		char[] c = new char[position];
		for(int ii = 0; ii < position;ii++)
			c[ii] = (char)b[ii];
		return c;
	}
	/**
	 * Returns a range of this sequence of bytes.  
	 * @param from start index
	 * @param to end index
	 * @return copy of range of this sequence of bytes starting at <code>'from'</code>
	 * (inclusive) and ending at <code>'to'</code> (exclusive).
	 * @throws IndexOutOfBoundsException if from or to is larger than the backing store.
	 */
	public byte[] subSequence(int from, int to){
		if( from > position || to > position )
			throw new IndexOutOfBoundsException("Attempt to access index greater than " + position);
		
		return Arrays.copyOfRange(b, from, to);
	}
	
	private int hash;
	@Override
	public int hashCode() {
		int h = hash;
		if( h == 0 ){
		    int off = 0;
		    int len = position;
		    
		    for (int i = 0; i < len; i++) {
		        h = 31*h + b[off++];
		    }
		}
	    return h;
	}
	
	/**
	 * Produces a hash code for a given byte array 
	 * @param bb
	 * @return
	 */
	public static int hashCode(byte[] bb){
		return hashCode(bb,0,bb.length);
	}
	
	/**
	 * Produces a hash code for a given byte array using len bytes from offset
	 * @param bb
	 * @return
	 */
	public static int hashCode(byte[] bb, int offset, int len){
		int h = 0;
	    int off = offset;
	    for (int i = 0; i < len; i++) {
	        h = 31*h + bb[off++];
	    }
	    return h;
	}
	
	@Override
	public int compareTo(ByteBuilder o) {
		if( this.position < o.position ) return -1;
		if( this.position > o.position ) return 1;
		
		for( int ii = 0; ii < this.position; ii++){
			if( this.b[ii] < o.b[ii] )
				return -1;
			else if ( this.b[ii] > o.b[ii] )
				return 1;
		}
		
		return 0;
	}
	public int compareTo(byte[] o) {
		if( position < o.length ) return -1;
		if( position > o.length ) return 1;
		
		for( int ii = 0; ii < position; ii++){
			if( b[ii] < o[ii] )
				return -1;
			else if ( b[ii] > o[ii] )
				return 1;
		}
		
		return 0;
	}
	
	/**
	 * Compares two byte arrays to each other from left to right.
	 * @param l 
	 * @param r
	 * @return -1, 0, +1 if the left array is smaller, equal to or bigger than
	 * the right array. 
	 */
	public static int compareTo(byte[] l, byte[] r){
		if( l.length < r.length ) return -1;
		if( l.length > r.length ) return 1;
		
		for( int ii = 0, len = l.length; ii < len; ii++){
			if( l[ii] < r[ii] )
				return -1;
			else if ( l[ii] > r[ii] )
				return 1;
		}
		return 0;
	}
}
