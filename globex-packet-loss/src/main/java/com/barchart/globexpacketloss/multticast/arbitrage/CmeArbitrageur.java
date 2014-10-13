package com.barchart.globexpacketloss.multticast.arbitrage;

import java.nio.ByteBuffer;

public abstract class CmeArbitrageur {

	private static final long INT_MASK = 0xffffffffL;

	private final Statistics stats;

	private final PacketCache packetCache;

	private long expectedSequenceNumber;

	private final Clock clock;

	private long lastGoodPacketTime;

	public CmeArbitrageur(Clock clock, int cacheSize) {
		this.clock = clock;
		this.packetCache = new PacketCache(cacheSize);
		this.stats = new Statistics();
		this.expectedSequenceNumber = Long.MIN_VALUE;
	}

	protected abstract void dispatch(ByteBuffer buffer) throws Exception;

	protected abstract void reportPacketLoss(long firstMissingSequence, int missingCount) throws Exception;

	protected abstract void reportIdleChannel(long millisSinceLastMessage);

	public long receiveOnAFeed(ByteBuffer buffer) throws Exception {
		long seq = buffer.getInt(0) & INT_MASK;
		stats.aFeedReceived(seq);
		return handlePacket(seq, buffer);
	}

	public long receiveOnBFeed(ByteBuffer buffer) throws Exception {
		long seq = buffer.getInt(0) & INT_MASK;
		stats.bFeedReceived(seq);
		return handlePacket(seq, buffer);
	}

	protected void feedCheck() throws Exception {
		// Check for missing inputs on inbound feeds...
		// Check for packet loss..
		checkInboundActivity();
		checkPacketLoss();

	}

	private void checkInboundActivity() {
		stats.getBFeedReceivedCount();
	}

	private void checkPacketLoss() throws Exception {
		int cacheCount = packetCache.getCount();
		if ((cacheCount > packetCache.getCacheSize() / 2) || (cacheCount > 0 && timeSinceLastGoodPacket() > 100)) {
			ByteBuffer buffer;
			final long startPacketLoss = expectedSequenceNumber;
			int lostPacketCount = 0;
			while ((buffer = packetCache.remove(expectedSequenceNumber)) == null) {
				expectedSequenceNumber++;
				lostPacketCount++;
			}
			reportPacketLoss(startPacketLoss, lostPacketCount);
			handleExpectedPacket(expectedSequenceNumber, buffer);
		}

	}

	private long timeSinceLastGoodPacket() {
		return (clock.getTime() - lastGoodPacketTime);
	}

	private long handlePacket(long seq, ByteBuffer buffer) throws Exception {
		if (isExpectedSequence(seq)) {
			handleExpectedPacket(seq, buffer);
		} else if (isFirstPacket()) {
			handleFirstPacket(seq, buffer);
		} else if (isFromTheFuture(seq)) {
			handleFuturePacket(seq, buffer);
		} else {
			// Packet is from the past. A duplicate
			// This is normal for A/B line arbitrage.
			// Drop it.
		}
		checkPacketLoss();
		return seq;
	}

	private void handleExpectedPacket(long seq, ByteBuffer buffer) throws Exception {
		dispatch(buffer);
		stats.combinedFeedReceived(seq);
		expectedSequenceNumber++;
		dispatchAnySavedPackets();
		this.lastGoodPacketTime = clock.getTime();
	}

	private void dispatchAnySavedPackets() throws Exception {
		ByteBuffer buffer;
		while ((buffer = packetCache.remove(expectedSequenceNumber)) != null) {
			dispatch(buffer);
			stats.combinedFeedReceived(expectedSequenceNumber);
			expectedSequenceNumber++;
		}
	}

	private void handleFirstPacket(long seq, ByteBuffer buffer) throws Exception {
		dispatch(buffer);
		stats.combinedFeedReceived(seq);
		expectedSequenceNumber = seq + 1;
	}

	private void handleFuturePacket(long seq, ByteBuffer buffer) {
		packetCache.put(seq, buffer);
	}

	private boolean isFirstPacket() {
		return expectedSequenceNumber == Long.MIN_VALUE;
	}

	private boolean isFromTheFuture(long sequenceNumber) {
		return sequenceNumber > expectedSequenceNumber;
	}

	private boolean isExpectedSequence(long sequenceNumber) {
		return sequenceNumber == expectedSequenceNumber;
	}

	public final Statistics getStatistics() {
		return stats;
	}

}
