package com.barchart.globexpacketloss;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.barchart.globexpacketloss.multticast.MulticastReceiver;
import com.barchart.globexpacketloss.multticast.PoolingMulticastReceiver;
import com.barchart.globexpacketloss.multticast.arbitrage.Clock;
import com.barchart.globexpacketloss.multticast.arbitrage.CmeArbitrageur;
import com.barchart.globexpacketloss.multticast.arbitrage.Statistics;
import com.google.common.net.HostAndPort;

public final class ChannelTracker extends CmeArbitrageur {

	private static final String FORMAT_STRING = "%11s | %9d %9d    %5.3f | %9d %9d    %5.3f | %9d %9d    %5.3f |";

	public static final String HEADER = "            |      A-RX      A-DR      A-% |      B-RX      B-DR      B-% |      C-RX      C-DR      C-% |";

	private static final int MAX_PACKET_SIZE = 1500;

	private static final int POOL_SIZE = 2048;

	private static final int PACKET_CACHE_SIZE = 4096;

	private final int channelId;

	private final boolean packetLossLogging;

	private final MulticastReceiver aFeedReceiver = new PoolingMulticastReceiver(POOL_SIZE, MAX_PACKET_SIZE, ByteOrder.BIG_ENDIAN) {
		@Override
		protected void receiveByteBuffer(ByteBuffer buffer) throws Exception {
			receiveOnAFeed(buffer);
		}
	};

	private final MulticastReceiver bFeedReceiver = new PoolingMulticastReceiver(POOL_SIZE, MAX_PACKET_SIZE, ByteOrder.BIG_ENDIAN) {
		@Override
		protected void receiveByteBuffer(ByteBuffer buffer) throws Exception {
			receiveOnBFeed(buffer);
		}
	};

	private HostAndPort feedAHostAndPort;

	private HostAndPort feedBHostAndPort;

	public ChannelTracker(Clock clock, Integer channelId, HostAndPort feedAHostAndPort, HostAndPort feedBHostAndPort, boolean packetLossLogging) {
		super(clock, PACKET_CACHE_SIZE);
		this.packetLossLogging = packetLossLogging;
		this.channelId = channelId;
		this.feedAHostAndPort = feedAHostAndPort;
		this.feedBHostAndPort = feedBHostAndPort;
	}

	public MulticastReceiver getFeedReceiverA() {
		return aFeedReceiver;
	}

	public MulticastReceiver getFeedReceiverB() {
		return bFeedReceiver;
	}

	public HostAndPort getFeedAHostAndPort() {
		return feedAHostAndPort;
	}

	public HostAndPort getFeedBHostAndPort() {
		return feedBHostAndPort;
	}

	@Override
	public String toString() {
		List<Object> parts = new ArrayList<Object>();
		if (channelId > 0) {
			parts.add(String.format("Channel %3d", channelId));
		} else {
			parts.add("Total      ");
		}
		
		Statistics stats = getStatistics();

		parts.add(stats.getAFeedReceivedCount());
		parts.add(stats.getAFeedMissedCount());
//		parts.add(stats.getAFeedIncidentCount());
		parts.add(stats.getAFeedPercentageMissed());

		parts.add(stats.getBFeedReceivedCount());
		parts.add(stats.getBFeedMissedCount());
//		parts.add(stats.getBFeedIncidentCount());
		parts.add(stats.getBFeedPercentageMissed());

		parts.add(stats.getCombinedFeedReceivedCount());
		parts.add(stats.getCombinedFeedMissedCount());
//		parts.add(stats.getCombinedFeedIncidentCount());
		parts.add(stats.getCombinedFeedPercentageMissed());

		return String.format(FORMAT_STRING, parts.toArray());
	}

	@Override
	protected void dispatch(ByteBuffer buffer) throws Exception {

	}

	@Override
	protected void reportPacketLoss(long firstMissingSequence, int missingCount) throws Exception {
	}

	@Override
	protected void reportIdleChannel(long millisSinceLastMessage) {

	}

	public void reset() {
		getStatistics().reset();
	}

	public int getChannelId() {
		return channelId;
	}
}
