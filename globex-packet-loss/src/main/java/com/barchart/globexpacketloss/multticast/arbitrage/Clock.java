package com.barchart.globexpacketloss.multticast.arbitrage;

public final class Clock {

	private long time;

	public void update() {
		this.time = System.currentTimeMillis();
	}

	public long getTime() {
		return time;
	}
	
}
