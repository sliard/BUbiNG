package it.unimi.di.law.bubing.frontier;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.LockFreeQueue;
import it.unimi.di.law.bubing.util.URLRespectsRobots;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

//RELEASE-STATUS: DIST

/**
 * A thread fetching pages that will be then analyzed by a {@link ParsingThread}.
 * <p>
 * <p>Instances of this class iteratively extract from {@link Frontier#todo} (using
 * polling and exponential backoff) a ready {@link VisitState} that has been
 * previously enqueued by the {@link TodoThread} and use their embedded
 * {@link FetchData} to fetch the first URL (or possibly the first few URLs, depending on {@link RuntimeConfiguration#keepAliveTime})
 * from the {@link VisitState} queue. Once the fetch is over, the embedded
 * {@link FetchData} is enqueued in {@link Frontier#results} and the thread
 * waits for a signal on the {@link FetchData}. The {@link ParsingThread} that
 * retrieves the {@link FetchData} must signal back that the {@link FetchData}
 * instances can be reused when it has finished to use its contents.
 * <p>
 * <p>The design of the interaction between instances of this class, the
 * {@link TodoThread} and instances of {@link ParsingThread} minimizes
 * contention by sandwiching all {@link FetchingThread} instances between two
 * wait-free queues (signalling back that a {@link FetchData} can be reused
 * causes of course no contention). Exponential backoff should happen rarely in
 * a full-speed crawl, as the {@linkplain Frontier#todo todo} queue is almost
 * always nonempty.
 * <p>
 * <p>Instances of this class do not access any shared data structure, except for
 * logging. It is expected that large instances of BUbiNG use thousands of
 * fetching threads to download from a large number of sites in parallel.
 * <p>
 * <p>This class implements {@link Closeable}: the {@link #close()} methods simply closes the underlying {@link FetchData} instance.
 */
public final class FetchingThread extends Thread implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FetchingThread.class);


  /**
   * Whether we should stop (used also to reduce the number of threads).
   */
  public volatile boolean stop;
  /**
   * A reference to the frontier.
   */
  private final Frontier frontier;
  /**
   * The synchronous HTTP client used by this thread.
   */
  private final HttpClient httpClient;
  /**
   * The fetched HTTP response used by this thread.
   */
  private FetchData fetchData;
  /**
   * The cookie store used by {@link #httpClient}.
   */
  private final BasicCookieStore cookieStore;

  /**
   * An SSL context that accepts all self-signed certificates.
   */
  private static final SSLContext TRUST_SELF_SIGNED_SSL_CONTEXT;

  static {
    try {
      TRUST_SELF_SIGNED_SSL_CONTEXT = SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build();
    } catch (Exception cantHappen) {
      throw new RuntimeException(cantHappen.getMessage(), cantHappen);
    }
  }

  /**
   * An SSL context that accepts all certificates
   */
  private static final SSLContext TRUST_ALL_CERTIFICATES_SSL_CONTEXT;

  static {
    try {
      TRUST_ALL_CERTIFICATES_SSL_CONTEXT = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
        public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
          return true;
        }
      }).build();
    } catch (Exception cantHappen) {
      throw new RuntimeException(cantHappen.getMessage(), cantHappen);
    }
  }

  /**
   * A support class that makes it possible to plug in a custom DNS resolver.
   */
  protected static final class BasicHttpClientConnectionManagerWithAlternateDNS
      extends BasicHttpClientConnectionManager {

    static Registry<ConnectionSocketFactory> getDefaultRegistry() {
      // setup a Trust Strategy that allows all certificates.
      //
      SSLContext sslContext = TRUST_ALL_CERTIFICATES_SSL_CONTEXT;
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register("http", PlainConnectionSocketFactory.getSocketFactory())
          .register("https",
              new SSLConnectionSocketFactory(sslContext,
                  new String[]{
                      "TLSv1",
                      "SSLv3",
                      "TLSv1.1",
                      "TLSv1.2",
                  }, null, new NoopHostnameVerifier()))
          .build();
    }

    public BasicHttpClientConnectionManagerWithAlternateDNS(final DnsResolver dnsResolver) {
      super(getDefaultRegistry(), null, null, dnsResolver);
    }
  }

  private static int length(final String s) {
    return s == null ? 0 : s.length();
  }

  /**
   * Returns the list of cookies in a given store in the form of an array, limiting their overall size
   * (only the maximal prefix of cookies satisfying the size limit is returned). Note that the size limit is expressed
   * in bytes, but the actual memory footprint is larger because of object overheads and because strings
   * are made of 16-bits characters. Moreover, cookie attributes are not measured when evaluating size.
   *
   * @param url               the URL which generated the cookies (for logging purposes).
   * @param cookieStore       a cookie store, usually generated by a response.
   * @param cookieMaxByteSize the maximum overall size of cookies in bytes.
   * @return the list of cookies in a given store in the form of an array, with limited overall size.
   */
  public static Cookie[] getCookies(final URI url, final CookieStore cookieStore, final int cookieMaxByteSize) {
    int overallLength = 0, i = 0;
    final List<Cookie> cookies = cookieStore.getCookies();
    for (Cookie cookie : cookies) {
      /* Just an approximation, and doesn't take attributes into account, but
       * there is no way to enumerate the attributes of a cookie. */
      overallLength += length(cookie.getName());
      overallLength += length(cookie.getValue());
      overallLength += length(cookie.getDomain());
      overallLength += length(cookie.getPath());
      if (overallLength > cookieMaxByteSize) {
        LOGGER.warn("URL " + url + " returned too large cookies (" + overallLength + " characters)");
        return cookies.subList(0, i).toArray(new Cookie[i]);
      }
      i++;
    }
    return cookies.toArray(new Cookie[cookies.size()]);
  }

  /**
   * Creates a new fetching thread.
   *
   * @param frontier a reference to the {@link Frontier}.
   * @param index    the index of this thread (only for logging purposes).
   */
  public FetchingThread(final Frontier frontier, final int index) throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
    setName(this.getClass().getSimpleName() + '-' + index);
    setPriority(Thread.MIN_PRIORITY); // Low priority; there will be thousands of this guys around.
    this.frontier = frontier;

    final BasicHttpClientConnectionManager connManager = new BasicHttpClientConnectionManagerWithAlternateDNS(frontier.rc.dnsResolver);

    connManager.closeIdleConnections(0, TimeUnit.MILLISECONDS);
    connManager.setConnectionConfig(ConnectionConfig.custom().setBufferSize(8 * 1024).build()); // TODO: make this configurable

    cookieStore = new BasicCookieStore();

    BasicHeader[] headers = {
        new BasicHeader("From", frontier.rc.userAgentFrom),
        new BasicHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.95,text/*;q=0.9,*/*;q=0.8"),
        new BasicHeader("Accept-Language", "*"),
        new BasicHeader("Accept-Charset", "*")
    };

    httpClient = HttpClients.custom()
        .setSSLHostnameVerifier(new NoopHostnameVerifier()) // why would we need to do it twice ?
        .setSSLContext(frontier.rc.acceptAllCertificates ? TRUST_ALL_CERTIFICATES_SSL_CONTEXT : TRUST_SELF_SIGNED_SSL_CONTEXT)
        .setConnectionManager(connManager)
        .setConnectionReuseStrategy(frontier.rc.keepAliveTime == 0 ? NoConnectionReuseStrategy.INSTANCE : DefaultConnectionReuseStrategy.INSTANCE)
        .setUserAgent(frontier.rc.userAgent)
        .setDefaultCookieStore(cookieStore)
        .setDefaultHeaders(ObjectArrayList.wrap(headers))
        .build();
    fetchData = new FetchData(frontier.rc);
  }

  /**
   * Causes the {@link FetchData} used by this thread to be {@linkplain FetchData#abort()} (whence, the corresponding connection to be closed).
   */
  public void abort() {
    FetchData fd = fetchData;
    if (fd != null)
      fd.abort();
  }

  @Override
  public void run() {
    try {
      final RuntimeConfiguration rc = frontier.rc;
      final int cookieMaxByteSize = rc.cookieMaxByteSize;
      final LockFreeQueue<FetchData> results = frontier.results; // Cached
      final LockFreeQueue<FetchData> availableFetchData = frontier.availableFetchData; // Cached

      while (!stop) {
        // Read
        VisitState visitState;
        long waitTime = 0;

        frontier.rc.ensureNotPaused();

        for (int i = 0; (visitState = frontier.todo.poll()) == null; i++) {
          frontier.rc.ensureNotPaused();
          if (stop) return;
          waitTime += 1 << Math.min(i, 10);
          Thread.sleep(1 << Math.min(i, 10));
        }

        if (waitTime > 0) {
          frontier.updateRequestedFrontSize();
          frontier.updateFetchingThreadsWaitingStats(waitTime);
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("Acquired visit state {}", visitState);

        // Try to find a fetchable URL (i.e., that does not violate the fetch filter or robots.txt).
        final long startTime = System.currentTimeMillis();

//				while(! visitState.isEmpty()) {

        final byte[] path = visitState.firstPath(); // Contains a crawlRequest with only path+query and crawl options

        for (int i = 0; (fetchData = availableFetchData.poll()) == null; i++) {
          rc.ensureNotPaused();
          if (stop) return;
          long newSleep = 1 << Math.min(i, 10);
          Thread.sleep(newSleep);
        }

        try {
          final MsgFrontier.CrawlRequest.Builder crawlRequest = MsgFrontier.CrawlRequest.newBuilder(MsgFrontier.CrawlRequest.parseFrom(path));
          final MsgURL.URL schemeAuthorityProto = PulsarHelper.schemeAuthority(visitState.schemeAuthority).build();
          final MsgURL.URL.Builder urlBuilder = crawlRequest.getUrlBuilder().setScheme(schemeAuthorityProto.getScheme()).setHost(schemeAuthorityProto.getHost());
          crawlRequest.setUrl(urlBuilder);

          if (LOGGER.isTraceEnabled())
            LOGGER.trace("Processing crawl request with path+query : {}", crawlRequest);
          final URI url = BURL.fromNormalizedSchemeAuthorityAndPathQuery(visitState.schemeAuthority,
              crawlRequest.getUrl().getPathQuery().getBytes(StandardCharsets.US_ASCII));

          if (LOGGER.isDebugEnabled()) LOGGER.debug("Next URL: {}", url);
          if (BlackListing.checkBlacklistedHost(frontier, url)) { // Check for blacklisted Host
            visitState.dequeue();
            visitState.schedulePurge();
            fetchData.inUse = false;
            frontier.availableFetchData.add(fetchData);
            fetchData = null;
          } else if (BlackListing.checkBlacklistedIP(frontier, url, visitState.workbenchEntry.ipAddress)) { // Check for blacklisted IP
            visitState.dequeue();
            visitState.schedulePurge();
            fetchData.inUse = false;
            frontier.availableFetchData.add(fetchData);
            fetchData = null;
          } else if (path == VisitState.ROBOTS_PATH) {
            fetchData.inUse = true;
            cookieStore.clear();
            try {
              fetchData.fetch(url,
                  crawlRequest.build(), httpClient, frontier.robotsRequestConfig, visitState, true);
            } catch (Exception shouldntHappen) {
              /* This shouldn't really happen--it's a bug that must be reported to the
               * ASF team. We cannot rely on the internal state of fetchData being OK,
               * so we just discard it and stop the keepalive download. We assume an
               * empty filter (as in the case of a 5xx status). We must perform some
               * bookkeeping usually performed by a ParsingThread. */
              LOGGER.error("Unexpected exception during fetch of " + url, shouldntHappen);
              fetchData.inUse = false;
              visitState.robotsFilter = URLRespectsRobots.EMPTY_ROBOTS_FILTER;
              final long endTime = System.currentTimeMillis();
              visitState.workbenchEntry.nextFetch = endTime + rc.ipDelay;
              visitState.nextFetch = endTime + rc.schemeAuthorityDelay;
              visitState.dequeue();
              continue;//break;
            }

            frontier.speedDist.incrementAndGet(Math.min(frontier.speedDist.length() - 1, Fast.mostSignificantBit(8 * fetchData.length() / (1 + fetchData.endTime - fetchData.startTime)) + 1));
            frontier.transferredBytes.addAndGet(fetchData.length());

            results.add(fetchData);
            try {
              if (fetchData.exception != null || frontier.rc.keepAliveTime == 0 || System.currentTimeMillis() - startTime >= frontier.rc.keepAliveTime)
                continue;//break;
            } finally {
              fetchData = null;
            }
          } else if (!frontier.rc.fetchFilter.apply(url)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("I'm not fetching URL {}", url);
            visitState.dequeue();
            frontier.done.add(visitState);
            fetchData.inUse = false;
            frontier.availableFetchData.add(fetchData);
            fetchData = null;
          } else if (visitState.robotsFilter != null && !URLRespectsRobots.apply(visitState.robotsFilter, url)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("URL {} disallowed by robots filter", url);
            visitState.dequeue();
            frontier.done.add(visitState);
            fetchData.inUse = false;
            frontier.availableFetchData.add(fetchData);
            fetchData = null;
          } else {
            if (RuntimeConfiguration.FETCH_ROBOTS && visitState.robotsFilter == null)
              LOGGER.error("Null robots filter for " + it.unimi.di.law.bubing.util.Util.toString(visitState.schemeAuthority));
            fetchData.inUse = true;
            cookieStore.clear();

            if (visitState.cookies != null) for (Cookie cookie : visitState.cookies) cookieStore.addCookie(cookie);
            try {
              fetchData.fetch(url, crawlRequest.build(), httpClient, frontier.defaultRequestConfig, visitState, false);
            } catch (Exception shouldntHappen) {
              /* This shouldn't really happen--it's a bug that must be reported to the ASF team.
               * We cannot rely on the internal state of fetchData being OK, so we just discard it
               * and stop the keepalive download. We must perform some bookkeeping usually
               * performed by a ParsingThread. */
              LOGGER.error("Unexpected exception during fetch of " + url, shouldntHappen);
              fetchData.inUse = false;
              final long endTime = System.currentTimeMillis();
              visitState.workbenchEntry.nextFetch = endTime + rc.ipDelay;
              visitState.nextFetch = endTime + rc.schemeAuthorityDelay;
              fetchData.inUse = false;
              frontier.availableFetchData.add(fetchData);
              fetchData = null;
              visitState.dequeue();
              continue;
              //							break;
            }

            frontier.speedDist.incrementAndGet(Math.min(frontier.speedDist.length() - 1, Fast.mostSignificantBit(8 * fetchData.length() / (1 + fetchData.endTime - fetchData.startTime)) + 1));
            frontier.transferredBytes.addAndGet(fetchData.length());

            results.add(fetchData);

            if (fetchData.exception == null) {
              visitState.cookies = getCookies(fetchData.uri(), cookieStore, cookieMaxByteSize);
            }
            try {
              if (fetchData.exception != null || frontier.rc.keepAliveTime == 0 || System.currentTimeMillis() - startTime >= frontier.rc.keepAliveTime)
                continue;
//								break;
            } finally {
              fetchData = null;
            }
          }
        } catch (InvalidProtocolBufferException e) {
          LOGGER.error("Error while parsing path {}", new String(path), e);
          fetchData.inUse = false;
          frontier.availableFetchData.add(fetchData);
          fetchData = null;
        }
      }

//			}
    } catch (Throwable e) {
      LOGGER.error("Unexpected exception", e);
    }
  }

  @Override
  public void close() throws IOException {
    FetchData fd = fetchData;
    if (fd != null)
      fd.close();
  }
}
