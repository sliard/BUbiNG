package it.unimi.di.law.bubing.frontier;

import com.exensa.wdl.common.TimeHelper;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.ByteArrayDiskQueues;
import it.unimi.di.law.bubing.util.ByteArrayDiskQueues.QueueData;
import it.unimi.di.law.bubing.util.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.NoSuchElementException;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

/** A <em>workbench virtualizer</em> based on a {@linkplain Database Berkeley DB} database.
 *
 * <p>An instance of this class acts as a thin layer between the workbench and a set of disk queues, possibly one for
 * each visit state, stored in a {@link Database}. Each queue is associated with a scheme+authority (the key).
 * Values are given by an increasing timestamp (written as a vByte-encoded integer) followed by a path+query.
 *
 * <p>Path+queries are enqueued using the {@link #enqueueCrawlRequest(VisitState, byte[])} method. They can be {@linkplain #dequeueCrawlRequests(VisitState, int) dequeued in batches}
 * (the method uses {@linkplain Cursor cursors}). When a queue is no longer needed, it can be {@linkplain #remove(VisitState) removed}.
 *
 * @author Sebastiano Vigna
 */
public class WorkbenchVirtualizer implements Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(WorkbenchVirtualizer.class);

	/** The underlying set of byte-array disk queues. */
	private final ByteArrayDiskQueues byteArrayDiskQueues;
	/** A reference to the {@link Frontier}. */
	private final Frontier frontier;
	/** The directory containing the virtualizer files. */
	private final File directory;

	/** Creates the virtualizer.
	 *
	 * @param frontier the frontier instantiating this virtualizer.
	 */
	public WorkbenchVirtualizer(final Frontier frontier) {
		this.frontier = frontier;
		directory = new File(frontier.rc.frontierDir, "virtualizer");
		directory.mkdir();
		byteArrayDiskQueues = new ByteArrayDiskQueues(directory);
	}

	/** Dequeues at most the given number of path+queries into the given visit state.
	 *
	 * <p>Note that the path+queries are directly enqueued into the visit state using
	 * {@link VisitState#enqueueCrawlRequest(byte[])}.
	 *
	 * @param visitState the visitState in which path+queries will be moved.
	 * @param maxUrls the maximum number of path+queries to move.
	 * @return the number of actually dequeued path+queries.
	 * @throws IOException
	 */
	public int dequeueCrawlRequests(final VisitState visitState, final int maxUrls) throws IOException {
		if (maxUrls == 0) return 0;
		long available = byteArrayDiskQueues.count(visitState);
		int dequeued = 0;
		try {
			while (available > 0 && dequeued < maxUrls) {
				var cr = byteArrayDiskQueues.dequeue(visitState);
				available--;
				frontier.pathQueriesInDiskQueues.decrementAndGet();
				// We drop expired CR here, because otherwise it could take forever to drain the queue
				// (the disk queue is dequeued small amount by small amount)
				if (!TimeHelper.hasTtlExpired(FetchInfoHelper.getCrawlRequestScheduleTime(cr), Duration.ofMillis(frontier.rc.crawlRequestTTL))) {
					visitState.enqueueCrawlRequest(cr);
					dequeued++;
				} else
					frontier.numberOfExpiredURLs.incrementAndGet();
			}
		} catch (NoSuchElementException e) {
			// Exception can occur when two threads concurrently empty the queues
			var eTip = new Exception(); // Only to get current stacktrace
			LOGGER.error("Disk queue of {} has been emptied under our feet, concurrency problem", visitState);
			LOGGER.error("StackTrace", eTip);
		}
		return dequeued;
	}

	/** Returns the number of path+queries associated with the given visit state.
	 *
 	 * @param visitState the visitState whose path+queries are to be counted.
	 * @return the number of path+queries associated with the given visit state.
	 */
	public long count(VisitState visitState) {
		return byteArrayDiskQueues.count(visitState);
	}

	/** Returns the number of visit states on disk.
	 *
	 * @return the number of visit states on disk.
	 */
	public int onDisk() {
		return byteArrayDiskQueues.numKeys();
	}

	/** Removes all path+queries associated with the given visit state.
	 *
 	 * @param visitState the visitState whose path+queries are to be removed.
	 * @throws IOException
	 */
	public void remove(VisitState visitState) throws IOException {
		synchronized (byteArrayDiskQueues) {
				frontier.pathQueriesInDiskQueues.updateAndGet(operand -> operand - count(visitState));
				byteArrayDiskQueues.remove(visitState);
		}
	}

	/** Enqueues the given URL as a path+query associated to the scheme+authority of the given visit state.
	 *
 	 * @param visitState the visitState to which the URL must be added.
	 * @param crawlRequest a {@link com.exensa.wdl.protobuf.frontier.MsgFrontier.CrawlRequest crawl request contaning URL, schedule time and various constraints}.
	 * @throws IOException
	 */
	public void enqueueCrawlRequest(VisitState visitState, final byte[] crawlRequest) throws IOException {
		byteArrayDiskQueues.enqueue(visitState,  crawlRequest, 0, crawlRequest.length);
		frontier.pathQueriesInDiskQueues.incrementAndGet();
	}

	/** Performs a garbage collection if the space used is below a given threshold, reaching a given target ratio.
	 *
	 * @param threshold if {@link ByteArrayDiskQueues#ratio()} is below this value, a garbage collection will be performed.
	 * @param targetRatio passed to {@link ByteArrayDiskQueues#count(Object)}.
	 */
	public void collectIf(final double threshold, final double targetRatio) throws IOException {
		if (byteArrayDiskQueues.ratio() < threshold) {
			LOGGER.info("Starting collection...");
			synchronized(byteArrayDiskQueues) {
				byteArrayDiskQueues.collect(targetRatio);
			}
			LOGGER.info("Completed collection.");
		}
	}

	@Override
	public void close() throws IOException {
		final ObjectOutputStream oos = new ObjectOutputStream(new FastBufferedOutputStream(new FileOutputStream(new File(directory, "metadata"))));
		byteArrayDiskQueues.close();
		writeMetadata(oos);
	}

	@Override
	public String toString() {
		return "URLs on disk: " + byteArrayDiskQueues.size64() + "; fill ratio: " + byteArrayDiskQueues.ratio();
	}

	private void writeMetadata(final ObjectOutputStream oos) throws IOException {
		oos.writeLong(byteArrayDiskQueues.size);
		oos.writeLong(byteArrayDiskQueues.appendPointer);
		oos.writeLong(byteArrayDiskQueues.used);
		oos.writeLong(byteArrayDiskQueues.allocated);
		oos.writeInt(byteArrayDiskQueues.buffers.size());
		oos.writeInt(byteArrayDiskQueues.key2QueueData.size());
		final ObjectIterator<Reference2ObjectMap.Entry<Object, QueueData>> fastIterator = byteArrayDiskQueues.key2QueueData.reference2ObjectEntrySet().fastIterator();
		for(int i = byteArrayDiskQueues.key2QueueData.size(); i-- != 0;) {
			final Reference2ObjectMap.Entry<Object, QueueData> next = fastIterator.next();
			final VisitState visitState = (VisitState)next.getKey();
			// TODO: temporary, to catch serialization bug
			if (visitState == null) {
				LOGGER.error("Map iterator returned null key");
				continue;
			}
			else if (visitState.schemeAuthority == null) LOGGER.error("Map iterator returned visit state with null schemeAuthority");
			else Util.writeVByte(visitState.schemeAuthority.length, oos);
			oos.write(visitState.schemeAuthority);
			oos.writeObject(next.getValue());
		}

		oos.close();
	}

	public void readMetadata() throws IOException, ClassNotFoundException {
		final ObjectInputStream ois = new ObjectInputStream(new FastBufferedInputStream(new FileInputStream(new File(directory, "metadata"))));
		byteArrayDiskQueues.size = ois.readLong();
		byteArrayDiskQueues.appendPointer = ois.readLong();
		byteArrayDiskQueues.used = ois.readLong();
		byteArrayDiskQueues.allocated = ois.readLong();
		final int n = ois.readInt();
		byteArrayDiskQueues.buffers.size(n);
		byteArrayDiskQueues.files.size(n);
		final VisitStateSet schemeAuthority2VisitState = frontier.distributor.schemeAuthority2VisitState;
		byte[] schemeAuthority = new byte[1024];
		for(int i = ois.readInt(); i-- != 0;) {
			final int length = Util.readVByte(ois);
			if (schemeAuthority.length < length) schemeAuthority = new byte[length];
			ois.readFully(schemeAuthority, 0, length);
			final VisitState visitState = schemeAuthority2VisitState.get(schemeAuthority, 0, length);
			// This can happen if the serialization of the visit states has not been completed.
			if (visitState != null) byteArrayDiskQueues.key2QueueData.put(visitState, (QueueData)ois.readObject());
			else LOGGER.error("No visit state found for " + Util.toString(schemeAuthority));
		}

		ois.close();
	}
}
