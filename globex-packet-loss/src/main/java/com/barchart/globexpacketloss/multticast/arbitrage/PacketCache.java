package com.barchart.globexpacketloss.multticast.arbitrage;

import java.nio.ByteBuffer;

class PacketCache {

	private final BufferHolder[] elements;
	private int count;

	PacketCache(int cacheSize) {
		if (cacheSize <= 0) {
			throw new IllegalArgumentException("CacheSize must be positive.  Not: " + cacheSize);
		}
		this.count = 0;
		this.elements = new BufferHolder[cacheSize];
		initElements();
	}

	private void initElements() {
		for (int i = 0; i < elements.length; i++) {
			elements[i] = new BufferHolder();
		}
	}

	public void put(long seq, ByteBuffer buffer) {
		int index = getIndex(seq);
		BufferHolder holder = elements[index];
		if (holder.seq != seq) {
			count++;
		}
		holder.seq = seq;
		holder.buffer = buffer;
	}

	public ByteBuffer remove(long seq) {
		int index = getIndex(seq);
		BufferHolder holder = elements[index];
		if (holder.seq == seq) {
			count--;
			holder.seq = Long.MIN_VALUE;
			return holder.buffer;
		} else {
			return null;
		}
	}
	
	public int getCount() {
		return count;
	}
	
	public int getCacheSize() {
		return elements.length;
	}

	private int getIndex(long seq) {
		return (int) (seq % elements.length);
	}

	private static final class BufferHolder {

		ByteBuffer buffer;

		long seq;

		BufferHolder() {
			this.seq = 0L;
		}

	}

}
