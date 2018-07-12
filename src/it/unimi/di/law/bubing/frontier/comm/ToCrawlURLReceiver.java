package it.unimi.di.law.bubing.frontier.comm;

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

import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.frontier.Frontier;
import it.unimi.di.law.bubing.util.*;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//RELEASE-STATUS: DIST

/** A set of Pulsar Receivers
 */
public final class ToCrawlURLReceiver implements MessageListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(ToCrawlURLReceiver.class);

	/** A reference to the frontier. */
	private final Frontier frontier;

	private final PulsarClient pulsarClient;
	private final List<Consumer> pulsarConsumers;
	private final int topicNumber;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating this thread.
	 */
	public ToCrawlURLReceiver(final Frontier frontier) throws PulsarClientException {
		ClientConfiguration conf = new ClientConfiguration();
		this.frontier = frontier;
		this.topicNumber = frontier.rc.pulsarFrontierTopicNumber;
		pulsarClient = PulsarClient.create(frontier.rc.pulsarClientConnection, conf);

		pulsarConsumers = new ArrayList<Consumer>(topicNumber);
		List<CompletableFuture<Consumer>> asyncConsumers = new ArrayList<CompletableFuture<Consumer>>(topicNumber);
		for (int i = 0; i < topicNumber; i++) {
			ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
			consumerConfig.setSubscriptionType(SubscriptionType.Failover);
			consumerConfig.setConsumerName(Integer.toString(MurmurHash3_128.murmurhash3_x86_32(frontier.rc.name, 0, frontier.rc.name.length(), i)));
			consumerConfig.setMessageListener(this);
			asyncConsumers.add(pulsarClient.subscribeAsync(frontier.rc.pulsarFrontierToCrawlURLsTopic + "-" + Integer.toString(i), "urlReceiveSubscription", consumerConfig));
		}
		for (int i = 0; i < topicNumber; i++) {
			try {
				Consumer c = asyncConsumers.get(i).get();

				pulsarConsumers.add(c);
			} catch (InterruptedException e) {
				LOGGER.error("Error while getting Pulsar consumer", e);
			} catch (ExecutionException e) {
				LOGGER.error("Error while getting Pulsar consumer", e);
			}
		}
	}

	public void stop() {
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
		LOGGER.info("Completed");
	}

	@Override
	public void received(Consumer consumer, Message message) {
		final MsgFrontier.CrawlRequest crawlRequest;
		try {
			crawlRequest = MsgFrontier.CrawlRequest.parseFrom(message.getData());
			frontier.quickReceivedToCrawlURLs.put(crawlRequest); // Will block until not full
		} catch (InvalidProtocolBufferException e) {
			LOGGER.error("Error while parsing message from Pulsar",e);
		} catch (InterruptedException e) {
			LOGGER.error("Error while enqueueing message from Pulsar",e);
		} finally {
			try {
				consumer.acknowledge(message);
			} catch (PulsarClientException e) {
				LOGGER.error("Error while acknowledging message to Pulsar", e);
			}
		}
	}
}
