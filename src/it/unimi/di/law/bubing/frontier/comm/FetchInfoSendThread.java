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

import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.protobuf.url.MsgURL;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.Frontier;

import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;

/**
 * A thread that takes care of sending the content of {@link Frontier#quickToSendDiscoveredURLs} with submit().
 */

public final class FetchInfoSendThread extends Thread
{
  private static final Logger LOGGER = LoggerFactory.getLogger( FetchInfoSendThread.class );
  /**
   * A reference to the frontier.
   */
  private final Frontier frontier;
  private final PulsarManager pulsarManager;

  /**
   * Creates the thread.
   *
   * @param frontier the frontier instantiating the thread.
   */
  public FetchInfoSendThread( final Frontier frontier, final PulsarManager pulsarManager ) {
    this.frontier = frontier;
    this.pulsarManager = pulsarManager;

    setName( this.getClass().getSimpleName() );
    setPriority( Thread.MAX_PRIORITY ); // This must be done quickly
  }

  /**
   * When set to true, this thread will complete its execution.
   */
  public volatile boolean stop = false;

  @Override
  public void run() {
    try {
      LOGGER.warn( "thread [started]" );

      final ArrayBlockingQueue<MsgCrawler.FetchInfo> quickToSendURLs = frontier.quickToSendDiscoveredURLs;
      boolean stopping = false;
      while ( true ) {
        if ( stop && !stopping ) {
          stopping = true;
          LOGGER.warn( "thread [stopping]" );
        }

        final MsgCrawler.FetchInfo fetchInfo = quickToSendURLs.poll( 1, TimeUnit.SECONDS );
        if ( fetchInfo == null ) {
          if ( stopping ) break;
          else continue;
        }

        if ( LOGGER.isTraceEnabled() ) {
          LOGGER.trace( "Sending fetchinfo for {}", Serializer.URL.Key.toString(fetchInfo.getUrlKey()) );
          for ( final MsgCrawler.FetchLinkInfo linkInfo : fetchInfo.getInternalLinksList() ) {
            LOGGER.trace( " - link to {}", Serializer.URL.Key.toString(linkInfo.getTarget()) );
          }
        }

        pulsarManager
          .getFetchInfoProducer( fetchInfo.getUrlKey() )
          .sendAsync( fetchInfo.toByteArray() );
      }
    }
    catch ( InterruptedException e ) {
      LOGGER.error( "Interrupted", e );
    }
    catch ( Throwable t ) {
      LOGGER.error( "Unexpected error", t );
    }
    finally {
      LOGGER.warn( "thread [stopped]" );
    }
  }
}
