package com.barchart.globexpacketloss.multticast;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;

public abstract class PoolingMulticastReceiver implements MulticastReceiver {

	private final ByteBuffer[] buffers;

	private int index;

	public PoolingMulticastReceiver(int bufferCount, int bufferSize, ByteOrder byteOrder) {
		this.buffers = new ByteBuffer[bufferCount];
		for (int i = 0; i < buffers.length; i++) {
			buffers[i] = ByteBuffer.allocateDirect(bufferSize).order(byteOrder);
		}
		this.index = 0;
	}

	@Override
	public final void receiveFrom(DatagramChannel channel) throws Exception {
		ByteBuffer buffer = buffers[index];
		buffer.clear();
		index = (index + 1) % buffers.length;
		channel.receive(buffer);
		buffer.flip();
		receiveByteBuffer(buffer);
	}

	protected abstract void receiveByteBuffer(ByteBuffer buffer) throws Exception;

}
