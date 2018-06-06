package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.util.*;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//RELEASE-STATUS: DIST

/** A thread that takes care of pouring the content of {@link Frontier#receivedURLs} into the {@link Frontier} itself (via the
 *  {@link Frontier#enqueue(it.unimi.dsi.fastutil.bytes.ByteArrayList)} method). The {@link #run()} method performs a busy polling on the {@link Frontier#receivedURLs}
 *  queue, at exponentially spaced time intervals (but anyway not less infrequently than 1s).
 */
public final class DiscoveredURLReceiveThread extends Thread implements MessageListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveredURLReceiveThread.class);

	/** A reference to the frontier. */
	private final Frontier frontier;

	private final PulsarClient pulsarClient;
	private final List<Consumer> pulsarConsumers;
	private final int topicNumber;
	private final long[] topicReceiveCounts;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating this thread.
	 */
	public DiscoveredURLReceiveThread(final Frontier frontier) throws PulsarClientException {
		setName(this.getClass().getSimpleName());
		/* BEGIN PULSAR BLOCK */
		ClientConfiguration conf = new ClientConfiguration();
		this.topicReceiveCounts = new long[frontier.rc.pulsarFrontierTopicNumber];
		this.frontier = frontier;
		this.topicNumber = frontier.rc.pulsarFrontierTopicNumber;
		pulsarClient = PulsarClient.create(frontier.rc.pulsarClientConnection,conf);

		pulsarConsumers = new ArrayList<Consumer>(topicNumber);
		List<CompletableFuture<Consumer>> asyncConsumers = new ArrayList<CompletableFuture<Consumer>>(topicNumber);
		for (int i=0; i<topicNumber; i++) {
			ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
			consumerConfig.setSubscriptionType(SubscriptionType.Failover);

			consumerConfig.setConsumerName(Integer.toString(MurmurHash3_128.murmurhash3_x86_32(frontier.rc.name, 0, frontier.rc.name.length(), i)));
			consumerConfig.setMessageListener(this);
			asyncConsumers.add(pulsarClient.subscribeAsync(frontier.rc.pulsarFrontierDiscoveredURLsTopic+"-"+Integer.toString(i), "urlReceiveSubscription", consumerConfig));
		}
		for (int i=0; i<topicNumber; i++) {
			try {
				Consumer c = asyncConsumers.get(i).get();
				pulsarConsumers.add(c);
			} catch (InterruptedException e) {
				LOGGER.error("Error while getting Pulsar consumer",e);
			} catch (ExecutionException e) {
				LOGGER.error("Error while getting Pulsar consumer",e);
			}
		}

		/* END PULSAR BLOCK*/

	}

	/** When set to true, this thread will complete its execution. */
	public volatile boolean stop;

	@Override
	public void run() {
		try {
			final ByteArrayDiskQueue receivedURLs = frontier.receivedURLs;
			for(;;) {
				for(int round = 0; frontier.rc.paused || receivedURLs.isEmpty() || ! frontier.agent.isConnected() || frontier.agent.getAliveCount() == 0; round++) {
					if (stop) return;
					Thread.sleep(1 << Math.min(10, round));
				}
				receivedURLs.dequeue();
				if (LOGGER.isTraceEnabled()) LOGGER.trace("Dequeued URL {} from the message queue", Util.toString(receivedURLs.buffer()));
				frontier.numberOfReceivedURLs.incrementAndGet();
				frontier.enqueueLocal(receivedURLs.buffer(),true);
			}
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception ", t);
		}
		/* BEGIN PULSAR BLOCK */

		for (Consumer c: pulsarConsumers) {
			try {
				c.close();
			} catch (PulsarClientException e) {
				e.printStackTrace();
			}
		}
		try {
			pulsarClient.close();
		} catch (PulsarClientException e) {
			e.printStackTrace();
		}
		/* END PULSAR BLOCK*/
		LOGGER.info("Completed");
	}
	/* BEGIN PULSAR BLOCK */
	@Override
	public void received(Consumer consumer, Message message) {
		long receivedURLs = frontier.numberOfReceivedURLs.incrementAndGet();

		byte[] messageData = message.getData();
		final int startOfHost = BURL.startOfHost(messageData);
		final long hash = MurmurHash3.hash(messageData, startOfHost, BURL.lengthOfHost(messageData, startOfHost));
		final int topicId = (int)((hash & 0x7fffffffffffffffl) % topicNumber);

		topicReceiveCounts[topicId] ++;

		if ((receivedURLs & 0x0fffff)==0)
			LOGGER.info("Topics received :  {}", Arrays.toString(topicReceiveCounts));

		try {
			// The received discovered URL will go through the sieve now
			frontier.enqueueLocal(new ByteArrayList(messageData), true);
		} catch (IOException e) {
			LOGGER.error("Error while enqueueing message from Pulsar",e);
		} catch (InterruptedException e) {
			LOGGER.error("Error while enqueueing message from Pulsar",e);
		}
		try {
			consumer.acknowledge(message);
		} catch (PulsarClientException e) {
			LOGGER.error("Error while acknowledging message to Pulsar",e);
		}
	}
	/* END PULSAR BLOCK*/

}
