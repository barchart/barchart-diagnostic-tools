package com.barchart.globexpacketloss;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.barchart.globexpacketloss.multticast.MulticastReceiver;
import com.barchart.globexpacketloss.multticast.arbitrage.Clock;
import com.google.common.collect.HashBasedTable;
import com.google.common.net.HostAndPort;

public final class PacketLossDetector {

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss:SSS");

	private static final int RECEIVE_BUFFER_SIZE = 16 * 1024 * 1024;

	private static final long TIMEOUT = 100;

	private static final int LOG_INTERVAL = 5000;

	private static final long WARMUP_SECONDS = 1;

	private final File configFile;

	private final List<Integer> channelIds;

	private final NetworkInterface bindInterface;

	private final Selector selector;

	private final HashBasedTable<HostAndPort, MulticastReceiver, MembershipKey> membershipTable;

	private final List<ChannelTracker> channelTrackers;

	private final boolean packetLogging;

	private final Clock clock;

	private volatile boolean running = true;

	private long lastLogTime;

	public PacketLossDetector(NetworkInterface bindInterface, File configFile, List<Integer> channelIds, boolean packetLogging) throws Exception {
		this.bindInterface = bindInterface;
		this.configFile = configFile;
		this.channelIds = channelIds;
		this.selector = Selector.open();
		this.membershipTable = HashBasedTable.create();
		this.channelTrackers = new ArrayList<ChannelTracker>();
		this.packetLogging = packetLogging;
		this.clock = new Clock();
	}

	public void start() throws Exception {
		addShutdownHook();
		createTrackers();
		joinTrackers();
		warmup();
		while (running) {
			int numberOfKeys = selector.select(TIMEOUT);
			clock.update();
			if (isTimeToLog()) {
				logTrackers();
			}
			if (numberOfKeys > 0) {
				processSelectedKeys();
			}
		}
		dropTrackers();
	}

	private void warmup() throws IOException {
		System.out.println("Warming up for " + WARMUP_SECONDS + " seconds.");
		long stopWarmup = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(WARMUP_SECONDS);
		while (System.currentTimeMillis() < stopWarmup) {
			selector.select(TIMEOUT);
			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
			ByteBuffer buffer = ByteBuffer.allocate(1500);
			while (iter.hasNext()) {
				SelectionKey key = iter.next();
				DatagramChannel channel = (DatagramChannel) key.channel();
				buffer.clear();
				channel.receive(buffer);
				iter.remove();
			}
		}
		System.out.println("Done warming up.");
	}

	private void createTrackers() throws Exception {
		CmeXmlConfig xmlConfig = CmeXmlConfig.parse(configFile.toURI().toURL());
		for (Integer channelId : channelIds) {
			HostAndPort incrementalFeedA = xmlConfig.getIncrementalFeedA(channelId);
			HostAndPort incrementalFeedB = xmlConfig.getIncrementalFeedB(channelId);
			ChannelTracker channelTracker = new ChannelTracker(clock, channelId, incrementalFeedA, incrementalFeedB, packetLogging);
			channelTrackers.add(channelTracker);
		}
	}

	private void logTrackers() {
		StringBuilder builder = new StringBuilder();
		String dateString = DATE_FORMAT.format(new Date());
		builder.append(dateString + " - " + ChannelTracker.HEADER).append("\n");
		for (ChannelTracker tracker : channelTrackers) {
			builder.append(dateString + " - " + tracker.toString()).append("\n");
		}
		System.out.println(builder.toString());
	}

	private boolean isTimeToLog() {
		long now = System.currentTimeMillis();
		if (now - lastLogTime > LOG_INTERVAL) {
			lastLogTime = now;
			return true;
		} else {
			return false;
		}
	}

	private void joinTrackers() throws IOException {
		for (ChannelTracker tracker : channelTrackers) {
			joinMulticast(tracker.getFeedAHostAndPort(), tracker.getFeedReceiverA());
			joinMulticast(tracker.getFeedBHostAndPort(), tracker.getFeedReceiverB());
		}
	}

	private void dropTrackers() {
		for (ChannelTracker tracker : channelTrackers) {
			leaveMulticast(tracker.getFeedAHostAndPort(), tracker.getFeedReceiverA());
			leaveMulticast(tracker.getFeedBHostAndPort(), tracker.getFeedReceiverB());
		}
	}

	private void joinMulticast(HostAndPort multicastInfo, MulticastReceiver receiver) throws IOException {
		System.out.println("Joining multicast: " + multicastInfo);
		InetAddress group = InetAddress.getByName(multicastInfo.getHostText());
		DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET);
		channel.configureBlocking(false);
		channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);

		channel.setOption(StandardSocketOptions.SO_RCVBUF, RECEIVE_BUFFER_SIZE);

		if (isWindows()) {
			channel.bind(new InetSocketAddress(multicastInfo.getPort()));
		} else {
			channel.bind(new InetSocketAddress(multicastInfo.getHostText(), multicastInfo.getPort()));
		}

		channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, bindInterface);
		MembershipKey membershipKey = channel.join(group, bindInterface);
		membershipTable.put(multicastInfo, receiver, membershipKey);
		SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_READ);
		selectionKey.attach(receiver);

		checkBufferSize(channel);

	}

	private void checkBufferSize(DatagramChannel channel) throws IOException {
		Integer actualReceiveBufferSize = channel.getOption(StandardSocketOptions.SO_RCVBUF);
		if (actualReceiveBufferSize != RECEIVE_BUFFER_SIZE) {
			System.out.println("WARNING: Requested receive buffer of size " + RECEIVE_BUFFER_SIZE + ", but was given " + actualReceiveBufferSize
					+ ".  Check the maximum buffer sizes in the operating system.");
		}
	}

	private void leaveMulticast(HostAndPort multicastInfo, MulticastReceiver receiver) {
		MembershipKey membershipKey = membershipTable.remove(multicastInfo, receiver);
		if (membershipKey != null) {
			membershipKey.drop();
		} else {
			System.err.println("No membership key for " + multicastInfo + ", " + receiver);
		}
	}

	private void addShutdownHook() {
		final Thread currentThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					running = false;
					currentThread.join();
				} catch (InterruptedException e1) {
					System.err.println("Problem shutting down gracefully.");
					e1.printStackTrace();
				}
			}
		});
	}

	private void processSelectedKeys() throws Exception {
		Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
		while (iter.hasNext()) {
			SelectionKey key = iter.next();
			processKey(key);
			iter.remove();
		}
	}

	private void processKey(SelectionKey key) throws Exception {
		if (key.isValid()) {
			DatagramChannel channel = (DatagramChannel) key.channel();
			MulticastReceiver receiver = (MulticastReceiver) key.attachment();
			receiver.receiveFrom(channel);
		}
	}

	private static void printUsage() throws SocketException {
		System.out.println("Usage: PacketLossDetector <bindInterface> <config.xml> <channelIds>");
		System.out.println("Available interfaces:");
		for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
			System.out.println("\t" + iface + " - IPV4: " + getFirstInet4Address(iface));
		}
	}

	private static Inet4Address getFirstInet4Address(NetworkInterface iface) {
		for (InetAddress addr : Collections.list(iface.getInetAddresses())) {
			if (addr instanceof Inet4Address) {
				return (Inet4Address) addr;
			}
		}
		return null;
	}

	private static NetworkInterface getNetworkInterface(String interfaceName) throws SocketException {
		NetworkInterface iface = NetworkInterface.getByName(interfaceName);
		if (iface == null) {
			throw new NullPointerException("No interface: " + interfaceName);
		}
		return iface;
	}

	private static List<Integer> getChannels(String str) {
		List<Integer> list = new ArrayList<Integer>();
		for (String s : str.split(",")) {
			list.add(Integer.parseInt(s));
		}
		Collections.sort(list);
		return list;
	}

	private boolean isWindows() {
		return System.getProperty("os.name").contains("Windows");
	}

	private static boolean checkForPacketLogging(String[] args) {
		if (args.length > 3) {
			return Boolean.valueOf(args[3]);
		}
		return false;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Os: " + System.getProperty("os.name"));
		if (args.length < 3) {
			printUsage();
		} else {
			NetworkInterface bindInterface = getNetworkInterface(args[0]);
			File configFile = new File(args[1]);
			List<Integer> channelIds = getChannels(args[2]);
			System.out.println("Using interface: " + bindInterface);
			boolean packetLogging = checkForPacketLogging(args);
			PacketLossDetector packetLossDetector = new PacketLossDetector(bindInterface, configFile, channelIds, packetLogging);
			packetLossDetector.start();
		}
	}

}
