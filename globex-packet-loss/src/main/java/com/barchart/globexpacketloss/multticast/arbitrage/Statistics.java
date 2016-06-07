package com.barchart.globexpacketloss.multticast.arbitrage;

import org.ietf.jgss.ChannelBinding;

public final class Statistics {

	private final LineStats aFeedStats;

	private final LineStats bFeedStats;

	private final LineStats combinedFeedStats;

	private final int channelId;

	Statistics(int channelId) {
		this.channelId = channelId;
		this.aFeedStats = new LineStats(channelId + "-A");
		this.bFeedStats = new LineStats(channelId + "-A");
		this.combinedFeedStats = new LineStats(channelId + "-C");
	}

	public Statistics(LineStats aStats, LineStats bStats, LineStats cStats) {
		this.channelId = 0;
		this.aFeedStats = aStats;
		this.bFeedStats = bStats;
		this.combinedFeedStats = cStats;
	}

	public void aFeedReceived(long sequenceNumber) {
		aFeedStats.receive(sequenceNumber);
	}

	public void bFeedReceived(long sequenceNumber) {
		bFeedStats.receive(sequenceNumber);
	}

	public void combinedFeedReceived(long sequenceNumber) {
		combinedFeedStats.receive(sequenceNumber);
	}

	private static final class LineStats {

		private long expected;

		private long receivedCount;

		private long oldCount;

		private long incidentCount;

		private long missedCount;

		private final String name;

		LineStats(String name) {
			this.name = name;
			this.expected = Long.MIN_VALUE;
		}

		public void receive(long sequenceNumber) {
//			 System.out.println("Seq num: " + sequenceNumber + ", expected: " + expected);
			receivedCount++;
			if (sequenceNumber == expected) {
				expected++;
			} else if (expected == Long.MIN_VALUE) {
				expected = sequenceNumber + 1;
			} else if (sequenceNumber < expected) {
				oldCount++;
			} else {
				System.out.println("Packet loss on " + name + ". Received: " + sequenceNumber + ", expexted: " + expected + ", missing: " + (sequenceNumber - expected));
				missedCount += (sequenceNumber - expected);
				incidentCount++;
				expected = sequenceNumber + 1;
			}
		}

		public double getPercentageMissed() {
			if (receivedCount == 0) {
				return 0.0;
			} else {
				return (missedCount / (double) receivedCount) * 100.0;
			}
		}

		private void plusEquals(LineStats other) {
			this.expected += other.expected;
			this.receivedCount += other.receivedCount;
			this.oldCount += other.oldCount;
			this.incidentCount += other.incidentCount;
			this.missedCount += other.missedCount;

		}

		public void reset() {
			// leave expected the same
			this.receivedCount = 0L;
			this.oldCount = 0L;
			this.incidentCount = 0L;
			this.missedCount = 0L;
		}

	}

	public long getAFeedReceivedCount() {
		return aFeedStats.receivedCount;
	}

	public long getAFeedMissedCount() {
		return aFeedStats.missedCount;
	}

	public long getAFeedIncidentCount() {
		return aFeedStats.incidentCount;
	}

	public double getAFeedPercentageMissed() {
		return aFeedStats.getPercentageMissed();
	}

	public long getBFeedReceivedCount() {
		return bFeedStats.receivedCount;
	}

	public long getBFeedMissedCount() {
		return bFeedStats.missedCount;
	}

	public long getBFeedIncidentCount() {
		return bFeedStats.incidentCount;
	}

	public double getBFeedPercentageMissed() {
		return bFeedStats.getPercentageMissed();
	}

	public long getCombinedFeedReceivedCount() {
		return combinedFeedStats.receivedCount;
	}

	public long getCombinedFeedMissedCount() {
		return combinedFeedStats.missedCount;
	}

	public long getCombinedFeedIncidentCount() {
		return combinedFeedStats.incidentCount;
	}

	public double getCombinedFeedPercentageMissed() {
		return combinedFeedStats.getPercentageMissed();
	}

	public static Statistics aggregate(Iterable<Statistics> all) {
		LineStats aStats = new LineStats("all-a");
		LineStats bStats = new LineStats("all-b");
		LineStats cStats = new LineStats("all-c");
		for (Statistics stats : all) {
			aStats.plusEquals(stats.aFeedStats);
			bStats.plusEquals(stats.aFeedStats);
			cStats.plusEquals(stats.combinedFeedStats);
		}
		return new Statistics(aStats, bStats, cStats);
	}

	public void plusEquals(Statistics statistics) {
		aFeedStats.plusEquals(statistics.aFeedStats);
		bFeedStats.plusEquals(statistics.bFeedStats);
		combinedFeedStats.plusEquals(statistics.combinedFeedStats);
	}

	public void reset() {
		aFeedStats.reset();
		bFeedStats.reset();
		combinedFeedStats.reset();
	}

}
