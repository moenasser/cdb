package com.mnasser.io.cdb;

/**
 * Copyright (c) 2013, Mohamed Nasser <moe.nasser@gmail.com>
 * All rights reserved.
 * 
 * This was adapted from M.A.Miller's legacy (Java 1.3) random access file 
 * implementation which was unfortunately dirt slow.
 * 
 * This was augmented to use Java NIO's much much faster mapped byte buffer
 * which takes advantage of an operating system's use of mmap() linux system 
 * call. You are basically piggy-backing on the os' swap file ability.   
 * 
 * Redistribution and use are permitted as per Miller's copyright below.
 */
/*
 * Copyright (c) 2000-2001, Michael Alyn Miller <malyn@strangeGizmo.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of Michael Alyn Miller nor the names of the
 *    contributors to this software may be used to endorse or promote
 *    products derived from this software without specific prior written
 *    permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

/**
 * Cdb implements a Java interface to D.&nbsp;J.&nbsp;Bernstein's CDB
 * database.
 *
 * @author		Mohamed Nasser: Updated to use mmap() - much faster. 
 * @author		Originally by: Michael Alyn Miller <malyn@strangeGizmo.com>
 * @version		2.0
 */
public class Cdb {
	/** The mmap() buffer for the CDB file. */
	private MappedByteBuffer mmfile_ = null;

	/** The slot pointers, cached here for efficiency as we do not have
	 * mmap() to do it for us.  These entries are paired as (pos, len) 
	 * tuples. 
	 * MNasser - We do now but let's keep this since the code relies on it.*/
	private int[] slotTable_ = null;


	/** The number of hash slots searched under this key. */
	private int loop_ = 0;

	/** The hash value for the current key. */
	private int khash_ = 0;


	/** The number of hash slots in the hash table for the current key. */
	private int hslots_ = 0;

	/** The position of the hash table for the current key */
	private int hpos_ = 0;


	/** The position of the current key in the slot. */
	private int kpos_ = 0;


	private final String filepath;
	private final long filesize;
	
	/**
	 * Creates an instance of the Cdb class and loads the given CDB
	 * file.
	 *
	 * @param filepath The path to the CDB file to open.
	 * @exception java.io.IOException if the CDB file could not be
	 *  opened.
	 */
	public Cdb(String filepath) throws IOException {
		/* Open the CDB file. */
		this.filepath = filepath;
		FileChannel fc = new RandomAccessFile(filepath, "r").getChannel();
		this.filesize = fc.size();
		mmfile_ = fc.map(MapMode.READ_ONLY, 0, filesize  );
		fc.close();


		/* Read and parse the slot table.  We do not throw an exception
		 * if this fails; the file might empty, which is not an error. */
		try {
			/* Read the table. */
			byte[] table = new byte[2048];
			mmfile_.get(table);

			/* Create and parse the table. */
			slotTable_ = new int[256 * 2];

			int offset = 0;
			for (int i = 0; i < 256; i++) {
				int pos = table[offset++] & 0xff
					| ((table[offset++] & 0xff) <<  8)
					| ((table[offset++] & 0xff) << 16)
					| ((table[offset++] & 0xff) << 24);

				int len = table[offset++] & 0xff
					| ((table[offset++] & 0xff) <<  8)
					| ((table[offset++] & 0xff) << 16)
					| ((table[offset++] & 0xff) << 24);

				slotTable_[i << 1] = pos;
				slotTable_[(i << 1) + 1] = len;
			}
		} catch (BufferUnderflowException ignored) {
			slotTable_ = null;
		}
	}

	@Override
	public String toString() {
		return "CDB|"+filesize +"|"+getName();
	}
	public String getName(){
		return new File(filepath).getName();
	}
	public String getPath(){
		return filepath;
	}
	
	/**
	 * Closes the CDB database.
	 */
	public final void close() {
		/* MNasser - Unused mmap() files get GC'ed. */
		mmfile_ = null;
		System.gc();	/*GC is stupid with mmap. So force the issue */
	}


	/**
	 * Computes and returns the hash value for the given key.
	 *
	 * @param key The key to compute the hash value for.
	 * @return The hash value of <code>key</code>.
	 */
	static final int hash(byte[] key) {
		/* Initialize the hash value. */
		long h = 5381;

		/* Add each byte to the hash value. */
		for (int i = 0; i < key.length; i++ ) {
//			h = ((h << 5) + h) ^ key[i];
			long l = h << 5;
			h += (l & 0x00000000ffffffffL);
			h = (h & 0x00000000ffffffffL);

			int k = key[i];
			k = (k + 0x100) & 0xff;

			h = h ^ k;
		}

		/* Return the hash value. */
		return (int)(h & 0x00000000ffffffffL);
	}


	/**
	 * Prepares the class to search for the given key.
	 *
	 * @param key The key to search for.
	 */
	public final void findstart(byte[] key) {
		loop_ = 0;
	}

	/**
	 * Finds the first record stored under the given key.
	 *
	 * @param key The key to search for.
	 * @return The record store under the given key, or
	 *  <code>null</code> if no record with that key could be found.
	 */
	public final synchronized byte[] find(byte[] key) {
		findstart(key);
		return findnext(key);
	}
	
	/**
	 * Returns all records stored under the given key.
	 * 
	 * @param key ArrayList of byte[] values.
	 * @return
	 * @author mnasser
	 */
	public final synchronized List<byte[]> findAll(byte[] key) {
		findstart(key);
		List<byte[]> values= new ArrayList<byte[]>();
		byte[] b;
		while( (b = findnext(key)) != null){
			values.add(b);
		}
		return values;
	}


	/**
	 * Reads the next 4 bytes of the buffer as an int.
	 * Note: Progresses the file pointer.
	 * 
	 * @param m the mapped byte buffer 
	 * @return next 4 bytes in buffer as an int.
	 */
	private int getIntFromMMap(MappedByteBuffer m) {
		return 	( m.get() & 0xFF ) 
				| ( (m.get() & 0xFF) << 8 )
				| ( (m.get() & 0xFF) << 16 )
				| ( (m.get() & 0xFF) << 24 ) ;
	}

	/**
	 * Finds the next record stored under the given key.
	 *
	 * @param key The key to search for.
	 * @return The next record store under the given key, or
	 *  <code>null</code> if no record with that key could be found.
	 */
	public final synchronized byte[] findnext(byte[] key) {
		/* There are no keys if we could not read the slot table. */
		if (slotTable_ == null)
			return null;

		/* Locate the hash entry if we have not yet done so. */
		if (loop_ == 0) {
			/* Get the hash value for the key. */
			int u = hash(key);

			/* Unpack the information for this record. */
			int slot = u & 255;
			hslots_ = slotTable_[(slot << 1) + 1];
			if (hslots_ == 0)
				return null;
			hpos_ = slotTable_[slot << 1];

			/* Store the hash value. */
			khash_ = u;

			/* Locate the slot containing this key. */
			u >>>= 8;
			u %= hslots_;
			u <<= 3;
			kpos_ = hpos_ + u;
		}

		/* Search all of the hash slots for this key. */
		try {
			while (loop_ < hslots_) {
				/* Read the entry for this key from the hash slot. */
				mmfile_.position(kpos_);
				
				int mh = getIntFromMMap(mmfile_);
				int mpos = getIntFromMMap(mmfile_);
				
				if (mpos == 0)
					return null;

				/* Advance the loop count and key position.  Wrap the
				 * key position around to the beginning of the hash slot
				 * if we are at the end of the table. */
				loop_ += 1;

				kpos_ += 8;
				if (kpos_ == (hpos_ + (hslots_ << 3)))
					kpos_ = hpos_;

				/* Ignore this entry if the hash values do not match. */
				if (mh != khash_)
					continue;

				/* Get the length of the key and data in this hash slot
				 * entry. */
				mmfile_.position(mpos);
				

				int mklen = getIntFromMMap(mmfile_);
				if (mklen != key.length)
					continue;

				int mdlen = getIntFromMMap(mmfile_);

				/* Read the key stored in this entry and compare it to
				 * the key we were given. */
				boolean match = true;
				byte[] k = new byte[mklen];
				mmfile_.get(k);
				
				for (int i = 0; i < k.length; i++) {
					if (k[i] != key[i]) {
						match = false;
						break;
					}
				}

				/* No match; check the next slot. */
				if (!match)
					continue;

				/* The keys match, return the data. */
				byte[] d = new byte[mdlen];
				mmfile_.get(d);
				return d;
			}
		} catch (BufferUnderflowException ignored) {
			return null;
		}

		/* No more data values for this key. */
		return null;
	}


	/**
	 * Returns an Enumeration containing a CdbElement for each entry in
	 * the constant database.
	 *
	 * @param filepath The CDB file to read.
	 * @return An Enumeration containing a CdbElement for each entry in
	 *  the constant database.
	 * @exception java.io.IOException if an error occurs reading the
	 *  constant database.
	 */
	public static Enumeration<CdbElement> elements(final String filepath)
		throws IOException
	{
		/* Open the data file. */
		final InputStream in
			= new BufferedInputStream(
				new FileInputStream(
					filepath));

		/* Read the end-of-data value. */
		final int eod = (in.read() & 0xff)
				| ((in.read() & 0xff) <<  8)
				| ((in.read() & 0xff) << 16)
				| ((in.read() & 0xff) << 24);

		/* Skip the rest of the hashtable. */
		in.skip(2048 - 4);

		/* Return the Enumeration. */
		return new Enumeration<CdbElement>() {
			/* Current data pointer. */
			int pos = 2048;

			/* Finalizer. */
			protected void finalize() {
				try { in.close(); } catch (Exception ignored) {}
			}


			/* Returns <code>true</code> if there are more elements in
			 * the constant database (pos < eod); <code>false</code>
			 * otherwise. */
			public boolean hasMoreElements() {
				return pos < eod;
			}

			/* Returns the next data element in the CDB file. */
			public synchronized CdbElement nextElement() {
				try {
					/* Read the key and value lengths. */
					int klen = readLeInt(); pos += 4;
					int dlen = readLeInt(); pos += 4;

					/* Read the key. */
					byte[] key = new byte[klen];
					for (int off = 0; off < klen; /* below */) {
						int count = in.read(key, off, klen - off);
						if (count == -1)
							throw new IllegalArgumentException(
								"invalid cdb format");
						off += count;
					}
					pos += klen;

					/* Read the data. */
					byte[] data = new byte[dlen];
					for (int off = 0; off < dlen; /* below */) {
						int count = in.read(data, off, dlen - off);
						if (count == -1)
							throw new IllegalArgumentException(
								"invalid cdb format");
						off += count;
					}
					pos += dlen;

					/* Return a CdbElement with the key and data. */
					return new CdbElement(key, data);
				} catch (IOException ioException) {
					throw new IllegalArgumentException(
						"invalid cdb format");
				}
			}


			/* Reads a little-endian integer from <code>in</code>. */
			private int readLeInt() throws IOException {
				return (in.read() & 0xff)
					| ((in.read() & 0xff) <<  8)
					| ((in.read() & 0xff) << 16)
					| ((in.read() & 0xff) << 24);
			}
		};
	}
	public static void main(String[] args) {
		System.out.println(112317 % 8);
		System.out.println(-112317 % 8);
		System.out.println( (-112317 % 8) + 8 );
		
	}
}