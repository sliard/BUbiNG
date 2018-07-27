package it.unimi.di.law.bubing.frontier.comm;

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

import it.unimi.di.law.bubing.frontier.Frontier;

import java.util.concurrent.*;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;

/** A thread that takes care of sending the content of {@link Frontier#quickToSendDiscoveredURLs} with submit(). */

public final class FetchInfoSendThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchInfoSendThread.class);
    /** A reference to the frontier. */
    private final Frontier frontier;


    private final PulsarClient pulsarClient; // PULSAR
    private final CompletableFuture<Producer>[] pulsarProducers; // PULSAR

    /** Creates the thread.
     *
     * @param frontier the frontier instantiating the thread.
     */
    public FetchInfoSendThread(final Frontier frontier) throws PulsarClientException {
        setName(this.getClass().getSimpleName());
        setPriority(Thread.MAX_PRIORITY); // This must be done quickly
        this.frontier = frontier;
        ClientBuilder clientBuilder = PulsarClient.builder()
            .ioThreads(16)
            .listenerThreads(16)
            .connectionsPerBroker(4)
            .enableTls(false)
            .enableTlsHostnameVerification(false)
            .statsInterval(10, TimeUnit.MINUTES)
            .serviceUrl(frontier.rc.pulsarClientConnection);

        this.pulsarClient = clientBuilder.build();
        ProducerBuilder producerBuilder = pulsarClient.newProducer()
            .enableBatching(true)
            .batchingMaxMessages(1024)
            .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
            .blockIfQueueFull(true)
            .sendTimeout(30000, TimeUnit.MILLISECONDS)
            .compressionType(CompressionType.LZ4)
            .producerName(frontier.rc.name);

        pulsarProducers = new CompletableFuture[frontier.rc.pulsarFrontierTopicNumber];

        for (int i=0; i<frontier.rc.pulsarFrontierTopicNumber; i++) {
          pulsarProducers[i] = producerBuilder.topic(frontier.rc.pulsarFrontierDiscoveredURLsTopic + "-"+Integer.toString(i)).createAsync();
          LOGGER.info("Producer for {} is created", frontier.rc.pulsarFrontierDiscoveredURLsTopic + "-"+Integer.toString(i));
        }
    }

    /** When set to true, this thread will complete its execution. */
    public volatile boolean stop;

    @Override
    public void run() {
        try {
          final ArrayBlockingQueue<MsgCrawler.FetchInfo> quickToSendURLs = frontier.quickToSendDiscoveredURLs;
          int timeout = 1;
          while (!stop) {
            MsgCrawler.FetchInfo fetchInfo = quickToSendURLs.poll(1, TimeUnit.SECONDS);

            while (fetchInfo != null) {
              try {
                final int topic = PulsarHelper.getTopic(fetchInfo.getUrl(), pulsarProducers.length);
                pulsarProducers[topic].get(timeout, TimeUnit.SECONDS).sendAsync(fetchInfo.toByteArray());
                frontier.numberOfSentURLs.addAndGet(1);
                fetchInfo = null;
              } catch (InterruptedException e1) {
                e1.printStackTrace();
              } catch (ExecutionException e1) {
                e1.printStackTrace();
              } catch (TimeoutException e1) {
                e1.printStackTrace();
              }
              timeout = Math.max(60,timeout * 2);
            }
          }

        } catch (Throwable t) {
            LOGGER.error("Unexpected exception ", t);
        }

        /* BEGIN PULSAR */
        for (CompletableFuture<Producer> p: pulsarProducers) {
            try {
                p.get(1,TimeUnit.SECONDS).closeAsync();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (ExecutionException e) {
              e.printStackTrace();
            } catch (TimeoutException e) {
              e.printStackTrace();
            }
        }
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        /* END PULSAR */

        LOGGER.info("Completed");
    }
}
