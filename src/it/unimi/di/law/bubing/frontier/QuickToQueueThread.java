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
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.jai4j.NoSuchJobManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A thread that takes care of sending the content of {@link Frontier#quickToSendURLs} with submit().
 * The {@link #run()} method waits on the {@link Frontier#quickReceivedURLs} queue, checking that {@link #stop} becomes true every second. */

public final class QuickToQueueThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuickToQueueThread.class);
    /** A reference to the frontier. */
    private final Frontier frontier;

    /** Creates the thread.
     *
     * @param frontier the frontier instantiating the thread.
     */
    public QuickToQueueThread(final Frontier frontier) {
        setName(this.getClass().getSimpleName());
        setPriority(Thread.MAX_PRIORITY); // This must be done quickly
        this.frontier = frontier;
    }

    /** When set to true, this thread will complete its execution. */
    public volatile boolean stop;

    @Override
    public void run() {
        try {
            final ArrayBlockingQueue<ObjectArrayList<ByteArrayList>> quickToQueueURLLists[] = frontier.quickToQueueURLLists;
            while(! stop) {
                for (int i=0; i<quickToQueueURLLists.length;i++) {
                    final ObjectArrayList<ByteArrayList> urls = quickToQueueURLLists[i].poll(1000 / quickToQueueURLLists.length, TimeUnit.MILLISECONDS);
                    if (urls != null) {
                        for (ByteArrayList url : urls)
                            frontier.enqueue(url);
                    }
                }
            }
        }
        catch (Throwable t) {
            LOGGER.error("Unexpected exception ", t);
        }

        LOGGER.info("Completed");
    }
}
