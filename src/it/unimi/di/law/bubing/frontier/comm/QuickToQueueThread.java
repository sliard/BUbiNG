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

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

/** A thread that takes care of sending the content of {@link Frontier#quickToSendDiscoveredURLs} with submit().
 * The {@link #run()} method waits on the {@link Frontier#quickReceivedDiscoveredURLs} queue, checking that {@link #stop} becomes true every second. */

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
                boolean found = false;
                for (int i=0; i<quickToQueueURLLists.length;i++) {
                    final ObjectArrayList<ByteArrayList> urls = quickToQueueURLLists[i].poll();
                    if (urls != null) {
                        found = true;
                        for (ByteArrayList url : urls)
                            frontier.enqueue(url);
                    }
                }
                if (!found)
                    Thread.sleep(200);
            }
        }
        catch (Throwable t) {
            LOGGER.error("Unexpected exception ", t);
        }

        LOGGER.info("Completed");
    }
}
