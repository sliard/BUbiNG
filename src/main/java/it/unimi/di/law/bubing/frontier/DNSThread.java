package it.unimi.di.law.bubing.frontier;

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

import com.exensa.util.compression.HuffmanModel;
import com.exensa.wdl.common.TimeHelper;
import com.google.common.primitives.Ints;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.util.BURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

//RELEASE-STATUS: DIST

/** A thread that continuously dequeues a {@link VisitState} from the {@linkplain Frontier#newVisitStates queue of new visit states} (those that still need a DNS resolution),
 *  resolves its host and puts it on the {@link Workbench}. The number of instances of this thread
 *  is {@linkplain RuntimeConfiguration#dnsThreads configurable}.
 *
 *  <p>Note that <em>only instances of this class manipulate {@linkplain Workbench#address2WorkbenchEntry the map
 *  from IP address hashes to workbench entries}</em>. The map needs to be updated with a get-and-set atomic
 *  operation, so synchronization is explicit.
 */

public final class DNSThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(DNSThread.class);
	/** Whether we should stop (used also to reduce the number of threads). */
	public volatile boolean stop;

	/** A reference to the frontier. */
	private final Frontier frontier;

	/** A DNS thread for the given {@link Frontier}, with an index used to set the thread's name.
	 *
	 * @param frontier the frontier.
	 * @param index the index of this thread
	 */
	public DNSThread(final Frontier frontier, final int index) {
		setName(this.getClass().getSimpleName() + '-' + index);
		this.frontier = frontier;
	}

	@Override
	public void run() {
		Random rng = new Random();
		frontier.runningDnsThreads.incrementAndGet();
		while(! stop) {
			try {
				frontier.rc.ensureNotPaused();

				// Try to get an available unknown host.
				VisitState visitState = frontier.unknownHosts.poll();
				// If none, try to get a new visit state.
				if (visitState == null) visitState = frontier.newVisitStates.poll(1, TimeUnit.SECONDS);

				// If none after one second, try again.
				if (visitState == null) continue;

				// If visitstate has already been scheduled for purge, skip it
				if (visitState.purgeRequired) continue;

				// If Visitstate is entirely expired, skip it
				if (TimeHelper.hasTtlExpired(visitState.lastScheduleInMinutes, Duration.ofMillis(frontier.rc.crawlRequestTTL))) {
					LOGGER.debug("VisitState {} has only expired crawl requests", visitState);
					visitState.schedulePurge(frontier.numberOfDrainedURLs);
					continue;
				}

				final String host = BURL.hostFromSchemeAndAuthority(visitState.schemeAuthority);

				try {
					frontier.workingDnsThreads.incrementAndGet();

					// This is the first point in which DNS resolution happens for new hosts.
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("Resolving host {} with DNS because of URL {}", host, BURL.fromNormalizedSchemeAuthorityAndPathQuery(visitState.schemeAuthority, HuffmanModel.defaultModel.decompress(visitState.firstPath())));
					InetAddress[] addresses = frontier.rc.dnsResolver.resolve(host);
					ArrayList<InetAddress> ipv4Addresses = new ArrayList<InetAddress>();
					for (InetAddress a:addresses) {
						if (a.getAddress().length == 4)
							ipv4Addresses.add(a);
						else
							LOGGER.debug("Got IPV6 for {}", host);
					}
					if (ipv4Addresses.size() <= 0) {
						LOGGER.info("Host {} has no IPv4 address", host);
						throw new UnknownHostException();
					}
					final byte[] address = ipv4Addresses.get(rng.nextInt(ipv4Addresses.size())).getAddress(); // Pick one of the addresses

					if (address.length == 4) {
						Lock lock = frontier.rc.blackListedIPv4Lock.readLock();
						lock.lock();
						try {
							if (frontier.rc.blackListedIPv4Addresses.contains(Ints.fromByteArray(address))) {
								LOGGER.warn("Visit state for host {} was not created and rather scheduled for purge because its IP {} was blacklisted", host, Arrays.toString(address));
								visitState.schedulePurge(frontier.numberOfDrainedURLs);
								continue;
							}
						} finally {
							lock.unlock();
						}
					}

					visitState.lastExceptionClass = null; // In case we had previously set UnknownHostException.class
					// Fetch or create atomically a new workbench entry.
					WorkbenchEntry entry = null;
					int overflowCounter = 0;
					long maxDelay = 0;
					int smallestEntry = 0;
					int smallestEntrySize = Integer.MAX_VALUE;
					do {
						// Try entries until one is not full
						entry = frontier.workbench.getWorkbenchEntry(address, overflowCounter);
						if (entry.size() > 0)
							maxDelay = Math.max(maxDelay, entry.extraDelay);
						if (entry.size() < smallestEntrySize) {
							smallestEntry = overflowCounter;
							smallestEntrySize = entry.size();
						}
						overflowCounter++;
					} while (entry.size() > frontier.rc.maxInstantSchemeAuthorityPerIP * overflowCounter);
					entry.extraDelay = maxDelay;
					if (entry.size() == 0) // it's a new one
						entry.extraDelay = maxDelay; // we set the new entries' delay to the max of all entries
					else
						// We select the one with the least entries
						entry = frontier.workbench.getWorkbenchEntry(address, smallestEntry);
					visitState.nextFetch = System.currentTimeMillis();
					visitState.setWorkbenchEntry(entry);
					frontier.resolvedVisitStates.incrementAndGet();
				}
				catch(UnknownHostException e) {
					LOGGER.info("Unknown host " + host + " for visit state " + visitState);

					if (visitState.lastExceptionClass != UnknownHostException.class) visitState.retries = 0;
					else visitState.retries++;

					visitState.lastExceptionClass = UnknownHostException.class;

					if (visitState.retries < ExceptionHelper.EXCEPTION_TO_MAX_RETRIES.getLong(UnknownHostException.class)) {
						final long delay = ExceptionHelper.EXCEPTION_TO_WAIT_TIME.getLong(UnknownHostException.class) << visitState.retries;
						// Exponentially growing delay
						visitState.nextFetch = System.currentTimeMillis() + delay;
						LOGGER.debug("Will retry DNS resolution of state " + visitState + " with delay " + delay);
						frontier.unknownHosts.add(visitState);
					}
					else {
						frontier.fetchingFailedHostCount.incrementAndGet();
						FetchInfoHelper.drainVisitStateForError(frontier, visitState);
						visitState.schedulePurge(frontier.numberOfDrainedURLs);
						LOGGER.debug("Visit state " + visitState + " killed by " + UnknownHostException.class.getSimpleName());
					}
				}
				finally {
					frontier.workingDnsThreads.decrementAndGet();
				}
			}
			catch (IOException ioException) {
				LOGGER.error("IOException in DNS thread", ioException);
				break; // Kill thread
			}
			catch(Throwable t) {
				LOGGER.error("Unexpected exception", t);
			}
		}
		frontier.runningDnsThreads.decrementAndGet();
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Completed");
	}
}
