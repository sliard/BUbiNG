package it.unimi.di.law.bubing.frontier;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import com.exensa.wdl.common.LanguageCodes;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.util.InspectableCachedHttpEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.parser.Parser;
import it.unimi.di.law.bubing.parser.SpamTextProcessor;
import it.unimi.di.law.bubing.spam.SpamDetector;
import it.unimi.di.law.bubing.store.Store;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.Link;
import it.unimi.di.law.bubing.util.URLRespectsRobots;
import it.unimi.di.law.warc.filters.Filter;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.shorts.Short2ShortMap;

import java.util.Random;
//RELEASE-STATUS: DIST

/** A thread parsing pages retrieved by a {@link FetchingThread}.
 *
 * <p>Instances of this class iteratively extract from {@link Frontier#results} (using polling and exponential backoff)
 * a {@link FetchData} that has been previously enqueued by a {@link FetchingThread}.
 * The content of the response is analyzed and the body of the response is possibly parsed, and its
 * digest is computed.
 * Newly discovered (during parsing) URLs are
 * {@linkplain Frontier#enqueue(MsgCrawler.FetchInfo) enqueued to the frontier}.
 * Then, a signal is issued on the {@link FetchData}, so that the owner (a {@link FetchingThread}}
 * can work on a different URL or possibly {@link Workbench#release(VisitState) release the visit state}.
 *
 * <p>At each step (fetching, parsing, following the URLs of a page, scheduling new URLs, storing) a
 * configurable {@link Filter} selects whether a URL is eligible for a specific activity.
 * This makes it possible a very fine-grained configuration of the crawl. BUbiNG will also check
 * that it is respecting the <code>robots.txt</code> protocol, and <em>this behavior cannot be
 * disabled programmatically</em>. If you want to crawl the web without respecting the protocol,
 * you'll have to write your own BUbiNG variant (for which we will be not responsible).
 */
public class ParsingThread extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParsingThread.class);

  /** A small gadget used to insert links in the frontier. It should be {@linkplain #init initialized}
   *  specifying URI and scheme/authority of the page being visited and the robot filter to be
   *  applied. Then, one or more URLs can be
   *  {@linkplain Frontier#enqueue(MsgCrawler.FetchInfo) enqueued}: the actual
   *  enqueuing takes place only if the URL passes both the schedule and the robots filter.
   */
  private static final class FrontierEnqueuer
  {
    private static final boolean ASSERTS = false;
    private final Frontier frontier;
    private final Filter<Link> scheduleFilter;
    private byte[] schemeAuthority;
    private URI uri;
    private FetchData fetchData;
    private char[][] robotsFilter;

    private int outlinks;
    private int scheduledLinks;
    private float totalWeight;

    private MsgCrawler.FetchInfo.Builder crawledPageInfoBuilder;

    /** Creates the enqueuer.
     *
     * @param frontier the frontier instantiating the enqueuer.
     * @param rc the configuration to be used.
     */
    FrontierEnqueuer(final Frontier frontier, final RuntimeConfiguration rc) {
      this.frontier = frontier;
      this.scheduleFilter = rc.scheduleFilter;

    }

    /** Initializes the enqueuer for parsing a page with a specific scheme+authority and robots filter.
     *
     * @param schemeAuthority the scheme+authority of the page to be parsed.
     * @param robotsFilter the robots filter of the (authority of the) page to be parsed.
     */
    void init( byte[] schemeAuthority,
               MsgFrontier.CrawlRequest crawlRequest,
               MsgCrawler.FetchInfo.Builder crawledPageInfoBuilder,
               FetchData fetchData,
               char[][] robotsFilter ) {
      this.schemeAuthority = schemeAuthority;
      this.crawledPageInfoBuilder = crawledPageInfoBuilder.setUrlKey( crawlRequest.getUrlKey() );
      this.fetchData = fetchData;
      this.robotsFilter = robotsFilter;
      this.uri = PulsarHelper.toURI( crawlRequest.getUrlKey() );
      this.scheduledLinks = this.outlinks = 0;
      this.totalWeight = 0.0f;

    }

    void flush() {
      crawledPageInfoBuilder
        .setContentLength( (int)fetchData.response().getEntity().getContentLength() )
        .setFetchDuration( (int)(fetchData.endTime - fetchData.startTime) )
        .setFetchDate( (int)(fetchData.startTime / (24*60*60*1000)) )
        .setHttpStatus( fetchData.response().getStatusLine().getStatusCode() )
        .setLanguage( fetchData.lang );
      frontier.enqueue( crawledPageInfoBuilder.build() );
    }

    /** Enqueues the given URL, provided that it passes the schedule filter, its host is {@link RuntimeConfiguration#blackListedHostHashes blacklisted}.
     *  Moreover, if the scheme+authority is the same as the one of the page being parsed, we check that the URL respects the robots filter.
     *
     * @param fetchedPageInfoBuilder the CrawledPageInfo to be enqueued.
     */
    void process( final MsgCrawler.FetchInfo.Builder fetchedPageInfoBuilder ) {
      for (int index = 0; index < fetchedPageInfoBuilder.getExternalLinksCount(); ) {
        MsgCrawler.FetchLinkInfo.Builder linkInfo = fetchedPageInfoBuilder.getExternalLinksBuilder(index);
        if (process(linkInfo, false))
          index ++;
        else
          fetchedPageInfoBuilder.removeExternalLinks(index);
      }
      for (int index = 0; index < fetchedPageInfoBuilder.getInternalLinksCount(); ) {
        MsgCrawler.FetchLinkInfo.Builder linkInfo = fetchedPageInfoBuilder.getInternalLinksBuilder(index);
        if (process(linkInfo, true))
          index ++;
        else
          fetchedPageInfoBuilder.removeInternalLinks(index);
      }
    }

    private boolean process( final MsgCrawler.FetchLinkInfo.Builder linkInfo, boolean isInternal ) {
      final URI url = PulsarHelper.toURI( linkInfo.getTarget() );
      outlinks++;

      final MsgCrawler.CrawlerInfo.Builder crawlerInfoBuilder = MsgCrawler.CrawlerInfo.newBuilder();

      if (!scheduleFilter.apply(new Link(uri, url)))
        return false;
			/*if (!scheduleFilter.apply(new Link(uri, url))) {
				crawlerInfoBuilder.setMatchesScheduleRule(false);
			} else
				crawlerInfoBuilder.setMatchesScheduleRule(true);
				*/
      crawlerInfoBuilder.setIsBlackListed(BlackListing.checkBlacklistedHost(frontier, url));

      final boolean sameSchemeAuthority = isInternal;
      if (ASSERTS)
        assert it.unimi.di.law.bubing.util.Util.toString(schemeAuthority).equals(BURL.schemeAndAuthority(url)) == sameSchemeAuthority : "(" + it.unimi.di.law.bubing.util.Util.toString(schemeAuthority) + ").equals(" + BURL.schemeAndAuthority(url) + ") != " + sameSchemeAuthority;

      if (RuntimeConfiguration.FETCH_ROBOTS) {
        if (robotsFilter == null)
          LOGGER.error("Null robots filter for " + it.unimi.di.law.bubing.util.Util.toString(schemeAuthority));
        else if (sameSchemeAuthority && !URLRespectsRobots.apply(robotsFilter, url)) {
          crawlerInfoBuilder.setDoesRespectRobots(false);
        }
      }
      linkInfo.setCrawlerInfo(crawlerInfoBuilder);
      return true;
    }
  }

  /** Whether we should stop (used also to reduce the number of threads). */
  public volatile boolean stop;

  /** Sensible format for a double. */
  private final java.text.NumberFormat formatDouble = new java.text.DecimalFormat("#,##0.00");
  /** A reference to the frontier. */
  private final Frontier frontier;
  /** A reference to the store. */
  private final Store store;
  /** The parsers used by this thread. */
  private final ArrayList<Parser<?>> parsers;
  /** A random number generator for the thread */
  private final Random rng;
  /**  */
  private final FrontierEnqueuer frontierLinkReceiver;
  /** A counter for java.nio.BufferOverflowException from Jericho */
  private static int overflowCounter = 0;
  /** Creates a thread.
   *
   * @param frontier the frontier instantiating the thread.
   * @param index the index of this thread (used to give it a name).
   */
  public ParsingThread(final Frontier frontier, final int index) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException, NoSuchAlgorithmException {
    setName(this.getClass().getSimpleName() + '-' + index);
    this.frontier = frontier;
    this.store = frontier.rc.storeClass.getConstructor(RuntimeConfiguration.class).newInstance(frontier.rc);
    this.rng = new Random(index);
    this.frontierLinkReceiver = new FrontierEnqueuer(frontier, frontier.rc);
    this.parsers = new ArrayList<>(frontier.rc.parsers.size());
    for(final Parser<?> parser : frontier.rc.parsers) this.parsers.add(parser.copy());
    frontier.availableFetchData.add(new FetchData(frontier.rc)); // Add extra available Fetch Data
    setPriority((Thread.NORM_PRIORITY + Thread.MIN_PRIORITY) / 2); // Below main threads
  }

  private void incrementCountAndPurge(boolean success, VisitState visitState, RuntimeConfiguration rc) {
    int count = 0;
    if (success) {
      count = frontier.schemeAuthority2Count.increment(visitState.schemeAuthority);
    } else {
      double w = (double)rc.maxUrlsPerSchemeAuthority / (double)rc.maxRequestsPerSchemeAuthority;
      int toAdd = 0;
      int floorW = (int)w;
      if (floorW > 0)
        toAdd = floorW;
      double p = w - floorW;
      if (rng.nextDouble() < p)
        toAdd ++;
      if (toAdd > 0)
        count = frontier.schemeAuthority2Count.increment(visitState.schemeAuthority);
    }
    if (count >= rc.maxUrlsPerSchemeAuthority - 1) {
      LOGGER.info("Reached maximum number of URLs for scheme+authority " + it.unimi.di.law.bubing.util.Util.toString(visitState.schemeAuthority));
      visitState.schedulePurge();
    }
  }

  @Override
  public void run() {
    try {
      LOGGER.warn( "thread [started]" );
      while ( !stop ) {
        final FetchData fetchData = getNextFetchData();
        if ( fetchData != null )
          processFetchData( fetchData );
      }
    }
    catch ( InterruptedException e ) {
      LOGGER.error( "Interrupted", e );
    }
    catch ( Throwable e ) {
      LOGGER.error( "Unexpected exception", e );
    }
    finally {
      close();
      LOGGER.warn( "thread [stopped]" );
    }
  }

  private FetchData getNextFetchData() throws InterruptedException {
    frontier.rc.ensureNotPaused();
    FetchData fetchData;
    for( int i=0; (fetchData=frontier.results.poll()) == null; ++i ) {
      if (stop) return null;
      long newSleep = 1 << Math.min(i, 10);
      Thread.sleep( newSleep );
      frontier.rc.ensureNotPaused();
    }
    return fetchData;
  }

  private void processFetchData( final FetchData fetchData ) throws IOException, InterruptedException {
    try { // This try/finally guarantees that we will release the visit state and signal back.
      parseAndStore( frontierLinkReceiver, fetchData );
    }
    finally {
      if (LOGGER.isTraceEnabled()) LOGGER.trace("Releasing visit state {}", fetchData.visitState);
      frontier.done.add( fetchData.visitState );
      frontier.availableFetchData.add( fetchData );
    }
  }

  private void close() {
    try {
      store.close();
      frontier.robotsWarcParallelOutputStream.get().close();
    }
    catch ( IOException e ) {
      LOGGER.error( "Error while closing store", e );
    }
  }

  private void parseAndStore( final FrontierEnqueuer frontierLinkReceiver, final FetchData fetchData ) throws IOException, InterruptedException {
    final RuntimeConfiguration rc = frontier.rc;
    final VisitState visitState = fetchData.visitState;
    if (LOGGER.isTraceEnabled()) LOGGER.trace("Got fetched response for visit state " + visitState);

    if ( fetchData.robots ) {
      frontier.robotsWarcParallelOutputStream.get().write(new HttpResponseWarcRecord(fetchData.uri(), fetchData.response()));
      if ((visitState.robotsFilter = URLRespectsRobots.parseRobotsResponse(fetchData, rc.userAgent)) == null) {
        // We go on getting/creating a workbench entry only if we have robots permissions.
        visitState.schedulePurge();
        LOGGER.warn("Visit state " + visitState + " killed by null robots.txt");
      }
      return;
    }

    final MsgCrawler.FetchInfo.Builder fetchedPageInfoBuilder = MsgCrawler.FetchInfo.newBuilder();

    frontierLinkReceiver.init(
      visitState.schemeAuthority,
      fetchData.getCrawlRequest(),
      fetchedPageInfoBuilder,
      fetchData,
      visitState.robotsFilter
    );

    final long streamLength = fetchData.response().getEntity().getContentLength();

    final ParseData parseData = streamLength != 0
      ? parse( fetchData, fetchedPageInfoBuilder )
      : new ParseData(); // We don't parse zero-length streams

    if ( parseData == null )
      return; // failure while parsing

    updateOutDegrees( fetchData, fetchedPageInfoBuilder );

    if (parseData.digest == null) {
      // We don't log for zero-length streams.
      if (streamLength != 0 && LOGGER.isDebugEnabled()) LOGGER.debug("Computing binary digest for " + fetchData.uri());
      // Fallback when all other parsers could not complete digest computation.
      parseData.digest = fetchData.binaryParser.parse(fetchData.uri(), fetchData.response(), null);
    }

    final boolean isNotDuplicate = streamLength == 0 || frontier.digests.addHash(parseData.digest); // Essentially thread-safe; we do not consider zero-content pages as duplicates
    if (LOGGER.isTraceEnabled()) LOGGER.trace("Decided that for {} isNotDuplicate={}", fetchData.uri(), isNotDuplicate);
    fetchData.isDuplicate(!isNotDuplicate);

    if (isNotDuplicate && rc.followFilter.apply(fetchData)) {
      frontierLinkReceiver.process(fetchedPageInfoBuilder);
      frontierLinkReceiver.flush();
    } else {
      LOGGER.debug("NOT Following {}", fetchData.uri());
    }

    final String result = store( rc, fetchData, parseData, isNotDuplicate, streamLength );

    if (LOGGER.isDebugEnabled())
      LOGGER.debug("Fetched " + fetchData.uri() + " (" + Util.formatSize((long)(1000.0 * fetchData.length() / (fetchData.endTime - fetchData.startTime + 1)), formatDouble) + "B/s; " + frontierLinkReceiver.scheduledLinks + "/" + frontierLinkReceiver.outlinks + "; " + result + ")");
  }

  private ParseData parse( final FetchData fetchData, final MsgCrawler.FetchInfo.Builder fetchedPageInfoBuilder ) {
    if ( !frontier.rc.parseFilter.apply(fetchData) ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "I'm not parsing page " + fetchData.uri() );
      return null;
    }

    final Parser<?> parser = getParser( fetchData );
    if ( parser == null ) {
      if ( LOGGER.isInfoEnabled() ) LOGGER.info( "I'm not parsing page " + fetchData.uri() + " because I could not find a suitable parser" );
      return null;
    }

    try {
      return doParse( parser, fetchData, fetchedPageInfoBuilder );
    }
    catch ( final BufferOverflowException e ) {
      LOGGER.warn( "Overflow while parsing {} ({})", fetchData.uri(), ++overflowCounter );
      return null;
    }
    catch ( final Exception e ) {
      // This mainly catches Jericho and network problems
      LOGGER.warn( "Exception while parsing " + fetchData.uri() + " with " + parser, e );
      return null;
    }
  }

  private void updateOutDegrees( final FetchData fetchData, final MsgCrawler.FetchInfo.Builder fetchedPageInfoBuilder ) {
    frontier.outdegree.add( fetchedPageInfoBuilder.getExternalLinksCount() );
    final ByteString currentZHost = fetchData.getCrawlRequest().getUrlKey().getZHost();
    int currentOutHostDegree = 0;
    for( final MsgCrawler.FetchLinkInfo u : fetchedPageInfoBuilder.getExternalLinksList() )
      if( !currentZHost.equals(u.getTarget().getZHost()) )
        currentOutHostDegree += 1;
    frontier.externalOutdegree.add( currentOutHostDegree );
  }

  private Parser<?> getParser( final FetchData fetchData ) {
    for ( final Parser<?> parser : parsers ) {
      if ( parser.apply(fetchData) )
        return parser;
    }
    return null;
  }

  private ParseData doParse( final Parser<?> parser, final FetchData fetchData, final MsgCrawler.FetchInfo.Builder fetchedPageInfoBuilder ) throws IOException {
    final RuntimeConfiguration rc = frontier.rc;
    final VisitState visitState = fetchData.visitState;
    final ParseData parseData = new ParseData();

    parseData.digest = parser.parse( fetchData.uri(), fetchData.response(), fetchedPageInfoBuilder );
    // Spam detection (NOTE: skipped if the parse() method throws an exception)
    if ( rc.spamDetector != null )
      updateSpammicity( parser, visitState );

    if (parser.getRewrittenContent() != null)
      rewriteContentToFetchData( parser.getRewrittenContent(), parser.guessedCharset(), fetchData );

    String title;
    if ( (title=parser.getTitle()) != null )
      fetchedPageInfoBuilder.setTitle(title);

    parseData.guessedCharset = parser.guessedCharset();
    if (parser.guessedLanguage() != null)
      parseData.guessedLanguage = parser.guessedLanguage().getLanguage();
    parseData.textContent = parser.getTextContent();

    fetchData.extraMap.putAll(ImmutableMap.of(
      "X-BUbiNG-Charset-Detection-Info", parser.getCharsetDetectionInfo().toString(),
      "X-BUbiNG-Language-Detection-Info", parser.getLanguageDetectionInfo().toString(),
      "BUbiNG-Guessed-Meta-Charset", parser.getCharsetDetectionInfo().htmlMetaCharset,
      "BUbiNG-Guessed-ICU-Charset", parser.getCharsetDetectionInfo().icuCharset,
      "BUbiNG-Guessed-HTTP-Charset", parser.getCharsetDetectionInfo().httpHeaderCharset
    ));
    fetchData.extraMap.put("BUbiNG-Guessed-Html5", String.valueOf(parser.html5()));
    fetchData.extraMap.put("BUbiNG-Guessed-responsive", String.valueOf(parser.responsiveDesign()));
    if (parseData.guessedCharset != null)
      fetchData.extraMap.put("BUbiNG-Guessed-Charset",parseData.guessedCharset.name());
    if (parseData.guessedLanguage != null) {
      fetchData.extraMap.put("BUbiNG-Guessed-Language", parseData.guessedLanguage);
      fetchData.lang = LanguageCodes.getByte(parseData.guessedLanguage);
    }

    return parseData;
  }

  private void updateSpammicity( final Parser<?> parser, final VisitState visitState ) {
    final RuntimeConfiguration rc = frontier.rc;
    if (visitState.termCountUpdates < rc.spamDetectionThreshold || rc.spamDetectionPeriodicity != Integer.MAX_VALUE) {
      final Object result = parser.result();
      if (result instanceof SpamTextProcessor.TermCount) visitState.updateTermCount((SpamTextProcessor.TermCount)result);
      if ((visitState.termCountUpdates - rc.spamDetectionThreshold) % rc.spamDetectionPeriodicity == 0) {
        visitState.spammicity = (float)((SpamDetector<Short2ShortMap>)rc.spamDetector).estimate(visitState.termCount);
        LOGGER.info("Spammicity for " + visitState + ": " + visitState.spammicity + " (" + visitState.termCountUpdates + " updates)");
      }
    }
  }

  private void rewriteContentToFetchData( final StringBuilder content, final Charset charsetOpt, final FetchData fetchData ) throws IOException {
    final HttpResponse response = fetchData.response();
    final InspectableCachedHttpEntity originalEntity = (InspectableCachedHttpEntity) response.getEntity();
    final BasicHttpEntity rewrittenEntity = new BasicHttpEntity();
    final InputStream is = IOUtils.toInputStream( content, charsetOpt != null ? charsetOpt : StandardCharsets.UTF_8 );

    rewrittenEntity.setContent( is );
    rewrittenEntity.setContentType( originalEntity.getContentType() );
    originalEntity.setEntity( rewrittenEntity );
    originalEntity.copyFullContent();
    final long contentLength = originalEntity.getContentLength();
    rewrittenEntity.setContentLength( contentLength );
    // We are cheating with the truth, so we must change the response's header
    response.setHeader("Content-Length", Long.toString(contentLength));
    response.setEntity( originalEntity );
  }

  private String store( final RuntimeConfiguration rc, final FetchData fetchData, final ParseData parseData, final boolean isNotDuplicate, final long streamLength ) throws IOException, InterruptedException {
    final VisitState visitState = fetchData.visitState;
    final boolean mustBeStored = rc.storeFilter.apply( fetchData );

    // ALERT: store exceptions should cause shutdown.
    final String result;
    if (mustBeStored) {
      if (isNotDuplicate) {
        // Soft, so we can change maxUrlsPerSchemeAuthority at runtime sensibly.
        incrementCountAndPurge(true, visitState, rc);
        final int code = fetchData.response().getStatusLine().getStatusCode() / 100;
        if (code > 0 && code < 6) frontier.archetypesStatus[code].incrementAndGet();
        else frontier.archetypesStatus[0].incrementAndGet();

        if (streamLength >= 0) frontier.contentLength.add(streamLength);

        final Header contentTypeHeader = fetchData.response().getEntity().getContentType();
        if (contentTypeHeader != null) {
          final String contentType = contentTypeHeader.getValue();
          if (StringUtils.startsWithIgnoreCase(contentType, "text")) frontier.contentTypeText.incrementAndGet();
          else if (StringUtils.startsWithIgnoreCase(contentType, "image")) frontier.contentTypeImage.incrementAndGet();
          else if (StringUtils.startsWithIgnoreCase(contentType, "application")) frontier.contentTypeApplication.incrementAndGet();
          else frontier.contentTypeOthers.incrementAndGet();
        }

        result = "stored";
      }
      else {
        frontier.duplicates.incrementAndGet();
        incrementCountAndPurge(false, visitState, rc);
        result = "duplicate";
      }
      fetchData.extraMap.put("BUbiNG-Fetching-Duration", Long.toString(fetchData.endTime - fetchData.startTime));
      store.store( fetchData.uri(), fetchData.response(), !isNotDuplicate,
        parseData.digest, parseData.getCharsetName(), parseData.guessedLanguage,
        fetchData.extraMap, parseData.textContent );

    }
    else {
      result = "not stored";
      incrementCountAndPurge(false, visitState, rc);
    }

    return result;
  }

  private static final class ParseData
  {
    Charset guessedCharset = null;
    String guessedLanguage = null;
    byte[] digest = null;
    StringBuilder textContent = null;

    String getCharsetName() {
      return guessedCharset == null ? null : guessedCharset.name();
    }
  }
}
