package com.barchart.globexpacketloss.multticast.arbitrage;

public final class Statistics {

	private final LineStats aFeedStats;

	private final LineStats bFeedStats;

	private final LineStats combinedFeedStats;

	Statistics() {
		this.aFeedStats = new LineStats();
		this.bFeedStats = new LineStats();
		this.combinedFeedStats = new LineStats();
	}

	public Statistics(LineStats aStats, LineStats bStats, LineStats cStats) {
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

		LineStats() {
			this.expected = Long.MIN_VALUE;
		}

		public void receive(long sequenceNumber) {
			// System.out.println("Seq num: " + sequenceNumber);
			receivedCount++;
			if (sequenceNumber == expected) {
				expected++;
			} else if (expected == Long.MIN_VALUE) {
				expected = sequenceNumber + 1;
			} else if (sequenceNumber < expected) {
				oldCount++;
			} else {
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
			this.expected = 0;
			this.receivedCount = 0;
			this.oldCount = 0;
			this.incidentCount = 0;
			this.missedCount = 0;
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
		LineStats aStats = new LineStats();
		LineStats bStats = new LineStats();
		LineStats cStats = new LineStats();
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
