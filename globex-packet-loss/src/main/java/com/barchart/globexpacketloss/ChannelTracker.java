package com.barchart.globexpacketloss;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.net.HostAndPort;
import com.google.common.primitives.UnsignedInts;

public final class ChannelTracker {

	private static final String FORMAT_STRING = "Channel %3d | %9d %9d    %5.3f | %9d %9d    %5.3f | %9d %9d    %5.3f |";

	public static final String HEADER = "            |      A-RX      A-DR      A-% |      B-RX      B-DR      B-% |      C-RX      C-DR      C-% |";

	private final int channelId;

	private final FeedReceiver aFeed;

	private final FeedReceiver bFeed;

	private final PacketSequenceInspector aFeedInspector;

	private final PacketSequenceInspector bFeedInspector;

	private final PacketSequenceInspector combinedFeedInspector;

	public ChannelTracker(Integer channelId, HostAndPort feedAHostAndPort, HostAndPort feedBHostAndPort) {
		this.channelId = channelId;
		this.aFeedInspector = new PacketSequenceInspector();
		this.bFeedInspector = new PacketSequenceInspector();
		this.combinedFeedInspector = new PacketSequenceInspector();
		this.aFeed = new FeedReceiver(feedAHostAndPort, aFeedInspector);
		this.bFeed = new FeedReceiver(feedBHostAndPort, bFeedInspector);
	}

	public Receiver getFeedReceiverA() {
		return aFeed;
	}

	public Receiver getFeedReceiverB() {
		return bFeed;
	}

	public HostAndPort getFeedAHostAndPort() {
		return aFeed.getHostAndPort();
	}

	public HostAndPort getFeedBHostAndPort() {
		return bFeed.getHostAndPort();
	}

	@Override
	public String toString() {
		List<Object> parts = new ArrayList<Object>();
		parts.add(channelId);

		parts.add(aFeedInspector.receivedCount);
		parts.add(aFeedInspector.missedPackets);
		// parts.add(aFeedInspector.incidentCount);
		parts.add(aFeedInspector.getPercentageMissed());

		parts.add(bFeedInspector.receivedCount);
		parts.add(bFeedInspector.missedPackets);
		// parts.add(bFeedInspector.incidentCount);
		parts.add(bFeedInspector.getPercentageMissed());

		parts.add(combinedFeedInspector.receivedCount);
		parts.add(combinedFeedInspector.missedPackets);
		// parts.add(combinedFeedInspector.incidentCount);
		parts.add(combinedFeedInspector.getPercentageMissed());
		return String.format(FORMAT_STRING, parts.toArray());
	}

	private class FeedReceiver implements Receiver {

		private final HostAndPort hostAndPort;
		private final PacketSequenceInspector inspector;

		public FeedReceiver(HostAndPort hostAndPort, PacketSequenceInspector inspector) {
			this.hostAndPort = hostAndPort;
			this.inspector = inspector;
		}

		public HostAndPort getHostAndPort() {
			return hostAndPort;
		}

		@Override
		public void receive(ByteBuffer buffer) throws Exception {
			long sequenceNumber = UnsignedInts.toLong(buffer.getInt());
			inspector.receiveSequenceNumber(sequenceNumber);
			combinedFeedInspector.receiveSequenceNumber(sequenceNumber);
		}

	}

	private final class PacketSequenceInspector {

		private long expectedSequenceNumber;

		private long missedPackets;

		private long incidentCount;

		private int receivedCount;

		public PacketSequenceInspector() {
			this.expectedSequenceNumber = Long.MIN_VALUE;
		}

		public double getPercentageMissed() {
			if (receivedCount == 0) {
				return 0.0;
			} else {
				return (missedPackets / (double) receivedCount) * 100.0;
			}
		}

		private void receiveSequenceNumber(long sequenceNumber) {
			receivedCount++;
			if (sequenceNumber < expectedSequenceNumber) {
				// System.out.println("Old packet: " + sequenceNumber);
				// Old packet, ignore
				return;
			} else if (sequenceNumber > expectedSequenceNumber && expectedSequenceNumber != Long.MIN_VALUE) {
				// System.out.println("Missed packet" + sequenceNumber);
				incidentCount++;
				missedPackets += (sequenceNumber - expectedSequenceNumber);
			}
			expectedSequenceNumber = sequenceNumber + 1;

		}
	}

}
