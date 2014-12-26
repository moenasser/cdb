package com.mnasser.io;

import com.mnasser.io.ByteBuilder;

/**
 * Reuses internal backing store whenever possible
 * <strong>NOT THREAD SAFE.</strong>
 * @author mnasser
 */
public class MutableByteBuilder extends ByteBuilder {
	
	public MutableByteBuilder(){ super();}
	public MutableByteBuilder(int size){ super(size); }
	public MutableByteBuilder(byte[] original){ super(original); }
	
	/**
	 * Returns the entire backing store - not a copy of the 
	 * representative sequence of bytes.
	 * If you augment this backing store - you must manually
	 * increment the internal length using incrLength(). 
	 * 
	 * @return The backing store for this sequence of bytes by 
	 *  reference (NOT A COPY)
	 * @see MutableByteBuilder.incrLength()
	 */
	@Override
	public byte[] getContent() {
		return this.b;
	}
	/**
	 * Increments the current length of the sequence.
	 * This is needed if you are manually filling the content 
	 * of the backing store using <code>getContent()</code>.
	 * @param p
	 * @see MutableByteBuilder.getContent()
	 */
	@Override
	public void incrLength(int p) {
		if( position + p > capacity ) throw new IndexOutOfBoundsException( (position + p) + " > " + capacity);
		position += p;
	}
	
	//Seems there is no significant speed advantage in doing it this way Arrays.copyOf() is just fine
	@Override
	public void reset(byte[] bb) {
		int p = 0;
		if ( b.length >= bb.length ){
			for(byte bite : bb ){
				b[p] = bite;
				p++;
			}
		}else{
			b = bb;
			p = b.length;
		}
		//b = bb;
		position = p;
		capacity = b.length;
	}
	
}