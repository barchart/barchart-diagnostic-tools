package com.barchart.globexpacketloss;

import java.nio.ByteBuffer;

public interface Receiver {
	
	public void receive(ByteBuffer buffer) throws Exception;
	
}
