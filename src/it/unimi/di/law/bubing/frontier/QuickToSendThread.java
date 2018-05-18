package it.unimi.di.law.bubing.frontier;

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

import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.BubingJob;
import it.unimi.di.law.bubing.util.ByteArrayDiskQueue;
import it.unimi.di.law.bubing.util.MurmurHash3;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.jai4j.NoSuchJobManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A thread that takes care of sending the content of {@link Frontier#quickToSendURLs} with submit().
 * The {@link #run()} method waits on the {@link Frontier#quickReceivedURLs} queue, checking that {@link #stop} becomes true every second. */

public final class QuickToSendThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuickToSendThread.class);
    /** A reference to the frontier. */
    private final Frontier frontier;
    //private final PulsarClient pulsarClient;
    //private final List<Producer> pulsarProducers;

    /** Creates the thread.
     *
     * @param frontier the frontier instantiating the thread.
     */
    public QuickToSendThread(final Frontier frontier) /*throws PulsarClientException*/ {
        setName(this.getClass().getSimpleName());
        setPriority(Thread.MAX_PRIORITY); // This must be done quickly
        /*ClientConfiguration conf = new ClientConfiguration();

        pulsarClient = PulsarClient.create("pulsar://localhost:6650",conf);
        pulsarProducers = new ArrayList<Producer>(512);
        List<CompletableFuture<Producer>> asyncProducers = new ArrayList<CompletableFuture<Producer>>(512);
        for (int i=0; i<512; i++) {
            ProducerConfiguration producerConfig = new ProducerConfiguration();
            producerConfig.setBatchingEnabled(true);
            producerConfig.setBatchingMaxMessages(128);
            producerConfig.setBatchingMaxPublishDelay(1000,TimeUnit.MILLISECONDS);
            producerConfig.setProducerName(frontier.rc.name);
            asyncProducers.add(pulsarClient.createProducerAsync("persistent://exensa/standalone/crawling/urlsToProcess-"+Integer.toString(i),  producerConfig));
        }
        for (int i=0; i<512; i++) {
            try {
                Producer p = asyncProducers.get(i).get();
                pulsarProducers.add(p);
            } catch (InterruptedException e) {
                LOGGER.error("Error while getting Pulsar consumer",e);
            } catch (ExecutionException e) {
                LOGGER.error("Error while getting Pulsar consumer",e);
            }
        }
        */
        this.frontier = frontier;
    }

    /** When set to true, this thread will complete its execution. */
    public volatile boolean stop;

    @Override
    public void run() {
        try {
            final ArrayBlockingQueue<ByteArrayList> quickToSendURLs = frontier.quickToSendURLs;
            while(! stop) {
                final ByteArrayList url = quickToSendURLs.poll(1, TimeUnit.SECONDS);

                if (url != null) {
                    final byte[] urlBuffer = url.elements();
                    final int startOfHost = BURL.startOfHost(urlBuffer);
                    final long hash = MurmurHash3.hash(urlBuffer, startOfHost, BURL.lengthOfHost(urlBuffer, startOfHost));

                    final BubingJob job = new BubingJob(url);
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Passing job " + job.toString());
                    try {
                        frontier.agent.submit(job);
                    } catch (Exception e) {
                        // This just shouldn't happen.
                        LOGGER.warn("Impossible to submit URL \"" + BURL.fromNormalizedByteArray(url.toByteArray())+"\"", e);
                    }
                    //pulsarProducers.get((int)((hash & 0x7fffffffffffffffl) % pulsarProducers.size())).sendAsync(urlBuffer);
                }
            }
        }
        catch (Throwable t) {
            LOGGER.error("Unexpected exception ", t);
        }

        /*
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
        }*/

        LOGGER.info("Completed");
    }
}
