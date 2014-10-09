Globex Packet Loss Detector
=========================

This utility attempts to detect sequence number gaps in the Globex incremental A/B feeds.

To use:

java -jar globex-packet-loss.jar <network interface to bind to> <globex config.xml file> <comma separated list of chanel ids>

For example:

java -jar  globex-packet-loss.jar eth0 config.xml 7,8,9,10

