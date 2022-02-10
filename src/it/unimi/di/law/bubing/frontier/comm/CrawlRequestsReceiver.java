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

import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.common.TimeHelper;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.frontier.Frontier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

//RELEASE-STATUS: DIST

/** A set of Pulsar Receivers
 */
public final class CrawlRequestsReceiver implements MessageListener<byte[]>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlRequestsReceiver.class);

	/** A reference to the frontier. */
	private final Frontier frontier;
	private final int topic;

	private long messageCount;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating this thread.
	 */
	public CrawlRequestsReceiver( final Frontier frontier, final int topic ) {
		this.frontier = frontier;
		this.topic = topic;
		this.messageCount = 0;
	}

	@Override
	public void received( final Consumer<byte[]> consumer, final Message<byte[]> message ) {
		try {
			final MsgFrontier.CrawlRequest crawlRequest = MsgFrontier.CrawlRequest.parseFrom( message.getData() );
			if ( LOGGER.isTraceEnabled() )
				LOGGER.trace( "Received url {} to crawl", Serializer.URL.Key.toString(crawlRequest.getUrlKey()) );
			if (!TimeHelper.hasTtlExpired(crawlRequest.getCrawlInfo().getScheduleTimeMinutes(), Duration.ofMillis(frontier.rc.crawlRequestTTL))) {
				// We cannot block indefinitely because pulsar doesn't like it (topics become unconsumable)
				if (frontier.receivedCrawlRequests.offer(crawlRequest)) {
					frontier.numberOfReceivedURLs.incrementAndGet();
					messageCount++;
				} else
					frontier.numberOfDroppedURLs.incrementAndGet();
			}
			if (messageCount == 1000)
				LOGGER.warn("PULSAR Consumer for topic {} is active", topic);
			// We still ACK even if the message was not actually put into a queue
			consumer.acknowledge( message );
		}
		catch ( InvalidProtocolBufferException e ) {
			LOGGER.error( String.format("Error while parsing message for topic %d",topic), e );
		}
		//catch ( InterruptedException e ) {
		//	LOGGER.error( String.format("Interrupted while processing message for topic %d",topic), e );
		//}
		catch ( PulsarClientException e ) {
		  LOGGER.error( String.format("While acknowledging message for topic %d",topic), e );
    }
		catch ( Throwable t) {
			LOGGER.error( String.format("While processing message for topic %d",topic), t );
			throw (t);
		}
	}
}
