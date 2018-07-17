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
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.MurmurHash3;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;

/** A thread that takes care of sending the content of {@link Frontier#quickToSendDiscoveredURLs} with submit(). */

public final class DiscoveredURLSendThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveredURLSendThread.class);
    /** A reference to the frontier. */
    private final Frontier frontier;


    private final PulsarClient pulsarClient; // PULSAR
    private final List<Producer> pulsarProducers; // PULSAR

    /** Creates the thread.
     *
     * @param frontier the frontier instantiating the thread.
     */
    public DiscoveredURLSendThread(final Frontier frontier) throws PulsarClientException {
        setName(this.getClass().getSimpleName());
        setPriority(Thread.MAX_PRIORITY); // This must be done quickly

        // START PULSAR BLOCK
        ClientConfiguration conf = new ClientConfiguration();
        this.frontier = frontier;
        conf.setIoThreads(8);
        conf.setListenerThreads(8);
        pulsarClient = PulsarClient.create(frontier.rc.pulsarClientConnection,conf);
        pulsarProducers = new ArrayList<>(frontier.rc.pulsarFrontierTopicNumber);
        List<CompletableFuture<Producer>> asyncProducers = new ArrayList<CompletableFuture<Producer>>(frontier.rc.pulsarFrontierTopicNumber);
        for (int i=0; i<frontier.rc.pulsarFrontierTopicNumber; i++) {
            ProducerConfiguration producerConfig = new ProducerConfiguration();
            producerConfig.setBatchingEnabled(true);
            producerConfig.setBatchingMaxMessages(1024);
            producerConfig.setBatchingMaxPublishDelay(100,TimeUnit.MILLISECONDS);
            producerConfig.setBlockIfQueueFull(true);
            producerConfig.setSendTimeout(30000, TimeUnit.MILLISECONDS);
            producerConfig.setCompressionType(CompressionType.LZ4);
            producerConfig.setProducerName(frontier.rc.name);
            asyncProducers.add(pulsarClient.createProducerAsync(frontier.rc.pulsarFrontierDiscoveredURLsTopic + "-"+Integer.toString(i),  producerConfig));
        }
        for (int i=0; i<frontier.rc.pulsarFrontierTopicNumber; i++) {
            try {
                Producer p = asyncProducers.get(i).get();
                pulsarProducers.add(p);
            } catch (InterruptedException e) {
                LOGGER.error("Error while getting Pulsar consumer",e);
            } catch (ExecutionException e) {
                LOGGER.error("Error while getting Pulsar consumer",e);
            }
        }
        // END PULSAR BLOCK
    }

    /** When set to true, this thread will complete its execution. */
    public volatile boolean stop;

    @Override
    public void run() {
        try {
            final ArrayBlockingQueue<MsgCrawler.FetchInfo> quickToSendURLs = frontier.quickToSendDiscoveredURLs;
            while( !stop ) {
                final MsgCrawler.FetchInfo fetchInfo = quickToSendURLs.poll(1, TimeUnit.SECONDS);
                if ( fetchInfo != null ) {
                    final int topic = PulsarHelper.getTopic( fetchInfo.getUrl(), pulsarProducers.size() );
                    pulsarProducers.get( topic ).sendAsync( fetchInfo.toByteArray() );
                }
            }
        }
        catch (Throwable t) {
            LOGGER.error("Unexpected exception ", t);
        }

        /* BEGIN PULSAR */
        for (Producer p: pulsarProducers) {
            try {
                p.close();
            } catch (PulsarClientException e) {
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
