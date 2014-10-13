package com.barchart.globexpacketloss.multticast;

import java.nio.channels.DatagramChannel;

public interface MulticastReceiver {

	public void receiveFrom(DatagramChannel channel) throws Exception;
	
}
