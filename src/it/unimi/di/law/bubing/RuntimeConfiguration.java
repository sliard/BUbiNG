package it.unimi.di.law.bubing;

import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import it.unimi.di.law.bubing.frontier.ParsingThread;
import it.unimi.di.law.bubing.parser.Parser;
import it.unimi.di.law.bubing.store.Store;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.Link;
import it.unimi.di.law.warc.filters.Filter;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.lang.ObjectParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.http.conn.DnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import static it.unimi.di.law.bubing.util.HostHash.hostLongHash;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//RELEASE-STATUS: DIST

/** Global data shared by all threads.
 *
 * <p>All BUbiNG components must share a certain number of global variables, such
 * as filters and pool of objects. A single instance of this class is created
 * at agent construction time: it is used to pass around a single reference
 * to global data.
 *
 * <p>All fields in this class are either <code>final</code> or
 * <code>volatile</code>, depending on whether they can be modified at runtime
 * (usually by means of JMX methods in {@link Agent}).
 */

public class RuntimeConfiguration {
	private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeConfiguration.class);

	/** Whether to fetch and use <code>robots.txt</code>. This value cannot be configured and it
	 * requires recompilation from the sources.
	 *
	 * <p>You should be better knowing what you are doing if you change this to false. */
	public static final boolean FETCH_ROBOTS = true;

	/** @see StartupConfiguration#name */
	public final String name;

	/** @see StartupConfiguration#maxInstantSchemeAuthorityPerIP */
	public volatile int maxInstantSchemeAuthorityPerIP;

	/** @see StartupConfiguration#maxVisitStates */
	public volatile int maxVisitStates;

	/** @see StartupConfiguration#maxRequestsPerSchemeAuthority */
	public final int maxRequestsPerSchemeAuthority;

	/** @see StartupConfiguration#internalQueues */
	public final int internalQueues;

	/** @see StartupConfiguration#fetchingThreads */
	public volatile int fetchingThreads;

	/** @see StartupConfiguration#parsingThreads */
	public volatile int parsingThreads;

	/** @see StartupConfiguration#dnsThreads */
	public volatile int dnsThreads;

	/** @see StartupConfiguration#fetchFilter */
	public volatile Filter<URI> fetchFilter;

	/** @see StartupConfiguration#scheduleFilter */
	public volatile Filter<Link> scheduleFilter;

	/** @see StartupConfiguration#parseFilter */
	public volatile Filter<URIResponse> parseFilter;

	/** @see StartupConfiguration#followFilter */
	public volatile Filter<FetchData> followFilter;

	/** @see StartupConfiguration#storeFilter */
	public volatile Filter<FetchData> storeFilter;

	/** @see StartupConfiguration#keepAliveTime */
	public volatile long keepAliveTime;

	/** @see StartupConfiguration#schemeAuthorityDelay */
	public volatile long schemeAuthorityDelay;

	/** @see StartupConfiguration#ipDelay */
	public volatile long ipDelay;

	/** @see StartupConfiguration#crawlRequestTTL */
	public volatile long crawlRequestTTL;

	/** @see StartupConfiguration#blackListedIPv4Addresses */
	public final IntOpenHashSet blackListedIPv4Addresses;

	/** A lock used to access {@link #blackListedIPv4Addresses}. */
	public final ReadWriteLock blackListedIPv4Lock;

	/** The set of hashes of hosts that should be blacklisted.
	 * @see StartupConfiguration#blackListedHosts */
	public final LongOpenHashSet blackListedHostHashes;

	/** A lock used to access {@link #blackListedHostHashes}. */
	public final ReadWriteLock blackListedHostHashesLock;

	/** @see StartupConfiguration#socketTimeout */
	public volatile int socketTimeout;

	/** @see StartupConfiguration#connectionTimeout */
	public volatile int connectionTimeout;

	/** @see StartupConfiguration#dnsTimeout */
	public volatile int dnsTimeout;

	/** @see StartupConfiguration#maximumTimeToFirstByte */
	public int maximumTimeToFirstByte;

	/** @see StartupConfiguration#maximumFetchDuration */
	public int maximumFetchDuration;

	/** @see StartupConfiguration#minimumDownloadSpeed */
	public int minimumDownloadSpeed;

	/** @see StartupConfiguration#fetchDataBufferByteSize */
	public final int fetchDataBufferByteSize;

	/** @see StartupConfiguration#proxyHost */
	public final String proxyHost;

	/** @see StartupConfiguration#proxyPort */
	public final int proxyPort;

	/** @see StartupConfiguration#robotProxyHost */
	public final String robotProxyHost;

	/** @see StartupConfiguration#robotProxyPort */
	public final int robotProxyPort;

	/** @see StartupConfiguration#cookiePolicy */
	public final String cookiePolicy;

	/** @see StartupConfiguration#cookieMaxByteSize */
	public int cookieMaxByteSize;

	/** @see StartupConfiguration#userAgent */
	public final String userAgent;

	/** @see StartupConfiguration#userAgentId */
	public final String userAgentId;

	/** @see StartupConfiguration#userAgentFrom */
	public final String userAgentFrom;

	/** @see StartupConfiguration#robotsExpiration */
	public volatile long robotsExpiration;

	/** @see StartupConfiguration#acceptAllCertificates */
	public volatile boolean acceptAllCertificates;

	/** @see StartupConfiguration#rootDir */
	public final File rootDir;

	/** @see StartupConfiguration#storeDir */
	public final File storeDir;

	/** @see StartupConfiguration#responseCacheDir */
	public final File responseCacheDir;

	/** @see StartupConfiguration#frontierDir */
	public final File frontierDir;

	/** @see StartupConfiguration#responseBodyMaxByteSize */
	public volatile int responseBodyMaxByteSize;

	/** @see StartupConfiguration#digestAlgorithm */
	public final String digestAlgorithm;

	/** @see StartupConfiguration#startPaused */
	public final boolean startPaused;

	/** @see StartupConfiguration#storeClass */
	public final Class<? extends Store> storeClass;

	/** @see StartupConfiguration#maxRecordsPerFile */
	public volatile int maxRecordsPerFile;

	/** @see StartupConfiguration#maxSecondsBetweenDumps */
	public volatile int maxSecondsBetweenDumps;

	/** @see StartupConfiguration#pulsarClientConnection */
	public volatile String pulsarClientConnection;

	/** @see StartupConfiguration#pulsarWARCTopic */
	public volatile String pulsarWARCTopic;

	/** @see StartupConfiguration#pulsarPlainTextTopic */
	public volatile String pulsarPlainTextTopic;

	/** @see StartupConfiguration#pulsarFrontierTopicNumber */
	public volatile int pulsarFrontierTopicNumber;

	/** @see StartupConfiguration#pulsarFrontierNodeNumber */
	public volatile int pulsarFrontierNodeNumber;

	/** @see StartupConfiguration#pulsarFrontierNodeId */
	public volatile int pulsarFrontierNodeId;

	/** @see StartupConfiguration#pulsarFrontierFetchTopic */
	public volatile String pulsarFrontierFetchTopic;

	/** @see StartupConfiguration#pulsarFrontierToCrawlURLsTopic */
	public volatile String pulsarFrontierToCrawlURLsTopic;

	/** @see StartupConfiguration#pulsarFrontierToPromptlyCrawlURLsTopic */
	public volatile String pulsarFrontierToPromptlyCrawlURLsTopic;

	/** @see StartupConfiguration#workbenchMaxByteSize */
	public volatile long workbenchMaxByteSize;

	/** @see StartupConfiguration#virtualizerMaxByteSize */
	public final long virtualizerMaxByteSize;

	/** @see StartupConfiguration#dnsCacheMaxSize */
	public final int dnsCacheMaxSize;

	/** @see StartupConfiguration#dnsPositiveTtl */
	public final long dnsPositiveTtl;

	/** @see StartupConfiguration#dnsNegativeTtl */
	public final long dnsNegativeTtl;

	/** @see StartupConfiguration#crawlIsNew */
	public final boolean crawlIsNew;

	/** @see StartupConfiguration#priorityCrawl */
	public final boolean priorityCrawl;

	/** The parser, instantiated. Parsers used by {@link ParsingThread} instances are obtained by {@linkplain FlyweightPrototype#copy() copying this parsers}. */
	public final ArrayList<Parser<?>> parsers;

	/* Global data not depending on a StartupConfiguration. */

	/* Global data not initialised at startup. */

	/** Whether the crawler is currently paused. When this variable changes to false,
	 *  a <code>notifyAll()</code> is issued on this runtime configuration. */
	public volatile boolean paused;

	/** Whether the crawler is currently being stopping. The change of state to true is stable&mdash;this
	 * variable will never become false again. */
	public volatile boolean stopping;

	/** The DNS resolver used throughout the crawler.
	 * @see StartupConfiguration#dnsResolverClass */
	public final DnsResolver dnsResolver;

	/** A pattern used to identify hosts specified directed via their address in dotted notation. Note the dot at the end.
	 *  It covers both IPv6 addresses (where hexadecimal notation is accepted by default) and IPv4 addresses (where hexadecimal notation
	 *  requires the 0x prefix on every single piece of the address). */
	public static final Pattern DOTTED_ADDRESS = Pattern.compile("(([0-9A-Fa-f]+[:])*[0-9A-Fa-f]+)|((((0x[0-9A-Fa-f]+)|([0-9]+))\\.)*((0x[0-9A-Fa-f]+)|([0-9]+)))");

	/** Converts a string specifying an IPv4 address into an integer. The string can be either a single integer (representing
	 *  the address) or a dot-separated 4-tuple of bytes.
	 *
	 * @param s the string to be converted.
	 * @return the integer representing the IP address specified by s.
	 * @throws ConfigurationException
	 */
	private int handleIPv4(final String s) throws ConfigurationException {
		try {
			if (!RuntimeConfiguration.DOTTED_ADDRESS.matcher(s).matches()) throw new ConfigurationException("Malformed IPv4 " + s + " for blacklisting");
			// Note that since we're sure this is a dotted-notation address, we pass directly through InetAddress.
			final byte[] address = InetAddress.getByName(s).getAddress();
			if (address.length > 4) throw new UnknownHostException("Not IPv4");
			return Ints.fromByteArray(address);
		} catch (final UnknownHostException e) {
			throw new ConfigurationException("Malformed IPv4 " + s + " for blacklisting", e);
		}
	}

	/** Adds a (or a set of) new IPv4 to the black list; the IPv4 can be specified directly or it can be a file (prefixed by
	 *  <code>file:</code>).
	 *
	 * @param spec the specification (an IP address, or a file prefixed by <code>file</code>).
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public void addBlackListedIPv4(final String spec) throws ConfigurationException, FileNotFoundException {
			if (spec.length() == 0) return; // Skip empty specs
			if (spec.startsWith("file:")) {
				final LineIterator lineIterator = new LineIterator(new FastBufferedReader(new InputStreamReader(new FileInputStream(spec.substring(5)), Charsets.ISO_8859_1)));
				while (lineIterator.hasNext()) {
					final MutableString line = lineIterator.next();
					if (line.length() > 0) blackListedIPv4Addresses.add(handleIPv4(line.toString()));
				}
			}
			else blackListedIPv4Addresses.add(handleIPv4(spec));
	}

	/** Adds a (or a set of) new host to the black list; the host can be specified directly or it can be a file (prefixed by
	 *  <code>file:</code>).
	 *
	 * @param spec the specification (a host, or a file prefixed by <code>file</code>).
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public void addBlackListedHost(final String spec) throws ConfigurationException, FileNotFoundException 	{
		if (spec.length() == 0) return; // Skip empty specs
		if (spec.startsWith("file:")) {
			final LineIterator lineIterator = new LineIterator(new FastBufferedReader(new InputStreamReader(new FileInputStream(spec.substring(5)), Charsets.ISO_8859_1)));
			while (lineIterator.hasNext()) {
				MutableString line = lineIterator.next();
				blackListedHostHashes.add(hostLongHash(line.toString().trim()));
			}
		}
		else blackListedHostHashes.add(hostLongHash(spec.trim()));
	}

	public RuntimeConfiguration(final StartupConfiguration startupConfiguration) throws ConfigurationException, IOException {
		try {
			crawlIsNew = startupConfiguration.crawlIsNew;
			priorityCrawl = startupConfiguration.priorityCrawl;
			name = startupConfiguration.name;
			maxInstantSchemeAuthorityPerIP = startupConfiguration.maxInstantSchemeAuthorityPerIP;
			maxVisitStates = startupConfiguration.maxVisitStates;
			maxRequestsPerSchemeAuthority = startupConfiguration.maxRequestsPerSchemeAuthority;
			internalQueues = startupConfiguration.internalQueues;
			fetchingThreads = startupConfiguration.fetchingThreads;
			parsingThreads = startupConfiguration.parsingThreads;
			dnsThreads = startupConfiguration.dnsThreads;
			fetchFilter = startupConfiguration.fetchFilter;
			scheduleFilter = startupConfiguration.scheduleFilter;
			parseFilter = startupConfiguration.parseFilter;
			followFilter = startupConfiguration.followFilter;
			storeFilter = startupConfiguration.storeFilter;
			keepAliveTime = startupConfiguration.keepAliveTime;
			schemeAuthorityDelay = startupConfiguration.schemeAuthorityDelay;
			ipDelay = startupConfiguration.ipDelay;
			crawlRequestTTL = startupConfiguration.crawlRequestTTL;
			startPaused = startupConfiguration.startPaused;
			storeClass = startupConfiguration.storeClass;
			maxRecordsPerFile = startupConfiguration.maxRecordsPerFile;
			maxSecondsBetweenDumps = startupConfiguration.maxSecondsBetweenDumps;
			pulsarClientConnection = startupConfiguration.pulsarClientConnection;
			pulsarWARCTopic = startupConfiguration.pulsarWARCTopic;
			pulsarPlainTextTopic = startupConfiguration.pulsarPlainTextTopic;
			pulsarFrontierTopicNumber = startupConfiguration.pulsarFrontierTopicNumber;
			pulsarFrontierNodeNumber = startupConfiguration.pulsarFrontierNodeNumber;
			pulsarFrontierNodeId = startupConfiguration.pulsarFrontierNodeId;
			pulsarFrontierFetchTopic = startupConfiguration.pulsarFrontierFetchTopic;
			pulsarFrontierToCrawlURLsTopic = startupConfiguration.pulsarFrontierToCrawlURLsTopic;
			pulsarFrontierToPromptlyCrawlURLsTopic = startupConfiguration.pulsarFrontierToPromptlyCrawlURLsTopic;
			workbenchMaxByteSize = startupConfiguration.workbenchMaxByteSize;
			virtualizerMaxByteSize = startupConfiguration.virtualizerMaxByteSize;
			dnsCacheMaxSize = startupConfiguration.dnsCacheMaxSize;
			dnsPositiveTtl = startupConfiguration.dnsPositiveTtl;
			dnsNegativeTtl = startupConfiguration.dnsNegativeTtl;

			try {
				dnsResolver = startupConfiguration.dnsResolverClass.getConstructor().newInstance();
			}
			catch (final Exception e) {
				throw new ConfigurationException(e.getMessage(), e);
			}

			blackListedIPv4Addresses = new IntOpenHashSet();
			for(final String spec : startupConfiguration.blackListedIPv4Addresses) addBlackListedIPv4(spec);
			blackListedIPv4Lock = new ReentrantReadWriteLock();

			blackListedHostHashes = new LongOpenHashSet();
			for(String spec : startupConfiguration.blackListedHosts) addBlackListedHost(spec);
			blackListedHostHashesLock = new ReentrantReadWriteLock();

			socketTimeout = startupConfiguration.socketTimeout;
			connectionTimeout = startupConfiguration.connectionTimeout;
			minimumDownloadSpeed = startupConfiguration.minimumDownloadSpeed;
			maximumFetchDuration = startupConfiguration.maximumFetchDuration;
			maximumTimeToFirstByte = startupConfiguration.maximumTimeToFirstByte;
			dnsTimeout = startupConfiguration.dnsTimeout;
			rootDir = new File(startupConfiguration.rootDir);
			storeDir = StartupConfiguration.subDir(startupConfiguration.rootDir, startupConfiguration.storeDir);
			responseCacheDir = StartupConfiguration.subDir(startupConfiguration.rootDir, startupConfiguration.responseCacheDir);
			frontierDir = StartupConfiguration.subDir(startupConfiguration.rootDir, startupConfiguration.frontierDir);
			fetchDataBufferByteSize = startupConfiguration.fetchDataBufferByteSize;
			proxyHost = startupConfiguration.proxyHost;
			proxyPort = startupConfiguration.proxyPort;
			robotProxyHost = startupConfiguration.robotProxyHost;
			robotProxyPort = startupConfiguration.robotProxyPort;
			cookiePolicy = startupConfiguration.cookiePolicy;
			cookieMaxByteSize = startupConfiguration.cookieMaxByteSize;
			userAgent = startupConfiguration.userAgent;
			userAgentId = startupConfiguration.userAgentId;
			userAgentFrom = startupConfiguration.userAgentFrom;
			robotsExpiration = startupConfiguration.robotsExpiration;
			acceptAllCertificates = startupConfiguration.acceptAllCertificates;
			responseBodyMaxByteSize = startupConfiguration.responseBodyMaxByteSize;
			digestAlgorithm = startupConfiguration.digestAlgorithm;
			parsers = parsersFromSpecs(startupConfiguration.parserSpec); // Try to build parsers just to see if the specs are correct

			// State setup

			paused = startPaused;
		}
		catch (final IllegalArgumentException e) { throw new ConfigurationException(e); }
		catch (final ClassNotFoundException e) { throw new ConfigurationException(e); }
		catch (final IllegalAccessException e) { throw new ConfigurationException(e); }
		catch (final InvocationTargetException e) { throw new ConfigurationException(e); }
		catch (final InstantiationException e) { throw new ConfigurationException(e); }
		catch (final NoSuchMethodException e) { throw new ConfigurationException(e); }
	}

	public void ensureNotPaused() throws InterruptedException {
		if (! paused) return;
		boolean waited = false;
		synchronized(this) {
			while(paused) {
				LOGGER.info("Detected pause--going to wait...");
				waited = true;
				wait();
			}
			if (waited) LOGGER.info("Pause terminated.");
		}
	}

	@Override
	public String toString() {
		final Class<?> thisClass = getClass();
		final TreeMap<String,Object> values = new TreeMap<>();
		for (final Field f : thisClass.getDeclaredFields()) {
			if (ReadWriteLock.class.isAssignableFrom(f.getClass())) continue;
			if ((f.getModifiers() & Modifier.STATIC) != 0) continue;
			try {
				Object o = f.get(this);
				String v = (o != null) ? o.toString() : "null";
				values.put(f.getName(), v.substring(0,Math.min(200,v.length()))); // cut for blacklists
			} catch (final IllegalAccessException e) {
				values.put(f.getName(), "<THIS SHOULD NOT HAPPEN>");
			}
		}
		return values.toString();
	}

	/** Given an array of parser specifications, it returns the corresponding list of parsers (only
	 *  the correct specifications are put in the list.
	 *
	 * @param specs the parser specifications (they will be parsed using {@link ObjectParser}.
	 * @return a list of parsers built according to the specifications (only the parseable items are put in the list).
	 */
	public static ArrayList<Parser<?>> parsersFromSpecs(String[] specs) throws IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, IOException {
		final ArrayList<Parser<?>> parsers = new ArrayList<>();
		for(final String spec : specs) parsers.add(ObjectParser.fromSpec(spec, Parser.class, new String[] { "it.unimi.di.law.bubing.parser" }));
		return parsers;
	}
}
