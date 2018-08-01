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
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//RELEASE-STATUS: DIST

/** A set of Pulsar Receivers
 */
public final class CrawlRequestsReceiver implements MessageListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlRequestsReceiver.class);

	/** A reference to the frontier. */
	private final Frontier frontier;

	private final PulsarClient pulsarClient;
	private final CompletableFuture<Consumer> pulsarConsumer;
	private final int topicNumber;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating this thread.
	 */
	public CrawlRequestsReceiver(final Frontier frontier, int topicNumber, PulsarClient pulsarClient) throws PulsarClientException {

		this.pulsarClient = pulsarClient;
		this.frontier = frontier;
		this.topicNumber = frontier.rc.pulsarFrontierTopicNumber;

		LOGGER.info("Receiver for crawl requests topic crawl-{} [started]", topicNumber);


		ConsumerBuilder consumerBuilder = pulsarClient.newConsumer()
				.subscriptionType(SubscriptionType.Failover)
				.acknowledgmentGroupTime(500, TimeUnit.MILLISECONDS)
				.messageListener(this)
				.subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
				.subscriptionName("toCrawlSubscription");
		CompletableFuture<Consumer> c = consumerBuilder
					.consumerName(UUID.randomUUID().toString()+frontier.rc.name)
					.topic(frontier.rc.pulsarFrontierToCrawlURLsTopic + "-" + Integer.toString(topicNumber))
					.subscribeAsync();
		pulsarConsumer = c;
	}

	public void stop() {

		try {
			pulsarConsumer.get(1, TimeUnit.SECONDS).close();
			LOGGER.info("Receiver for crawl requests topic crawl-{} [stopped]", topicNumber);
		} catch (PulsarClientException e) {
			LOGGER.error("Failed to stop receiver for crawl requests topic crawl-{}", topicNumber);
			e.printStackTrace();
		} catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }


  }

	@Override
	public void received(Consumer consumer, Message message) {
		try {
			final MsgFrontier.CrawlRequest crawlRequest;
			crawlRequest = MsgFrontier.CrawlRequest.parseFrom(message.getData());
			if (LOGGER.isTraceEnabled())
				LOGGER.trace("Received url {} to crawl", crawlRequest.toString());
			frontier.quickReceivedCrawlRequests.put(crawlRequest); // Will block until not full
			frontier.numberOfReceivedURLs.addAndGet(1);
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
