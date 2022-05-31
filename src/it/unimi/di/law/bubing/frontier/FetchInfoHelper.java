package it.unimi.di.law.bubing.frontier;

import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.common.TimeHelper;
import com.exensa.wdl.common.UnexpectedException;
import com.exensa.wdl.protobuf.crawler.EnumFetchStatus;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FetchInfoHelper {
  private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FetchInfoHelper.class);

  static int getCrawlRequestScheduleTime(final byte[] crawlRequest) throws InvalidProtocolBufferException {
    MsgFrontier.CrawlRequest.Builder crawlRequestBuilder = MsgFrontier.CrawlRequest.parseFrom(crawlRequest).toBuilder();
    return crawlRequestBuilder.getCrawlInfo().getScheduleTimeMinutes();
  }

  static MsgFrontier.CrawlRequest.Builder createCrawlRequest( final MsgURL.Key schemeAuthority, final byte[] minimalCrawlRequestSerialized ) throws InvalidProtocolBufferException {
    MsgFrontier.CrawlRequest.Builder crawlRequestBuilder = MsgFrontier.CrawlRequest.parseFrom(minimalCrawlRequestSerialized).toBuilder();
    MsgURL.Key.Builder urlKeyBuilder = crawlRequestBuilder.getUrlKeyBuilder();
    urlKeyBuilder
      .setScheme(schemeAuthority.getScheme())
      .setZDomain(schemeAuthority.getZDomain())
      .setZHostPart(schemeAuthority.getZHostPart());
    return crawlRequestBuilder;
  }
  private static MsgCrawler.FetchInfo fetchInfoFailedGeneric( final MsgFrontier.CrawlRequestOrBuilder crawlRequest,
                                                              final VisitState visitState,
                                                              final int fetchStatus ) {
    final MsgCrawler.FetchInfo.Builder fetchInfoBuilder = MsgCrawler.FetchInfo.newBuilder()
      .setUrlKey( crawlRequest.getUrlKey() )
      .setFetchDateDeprecated( TimeHelper.getDaysNow() )
      .setFetchTimeMinutes( TimeHelper.getMinutesNow() )
      .setFetchStatusValue( fetchStatus );
    if ( visitState.workbenchEntry != null && visitState.workbenchEntry.ipAddress != null )
      fetchInfoBuilder.setIpAddress( ByteString.copyFrom(visitState.workbenchEntry.ipAddress) );
    return fetchInfoBuilder.build();
  }
  private static MsgCrawler.FetchInfo fetchInfoFailedGeneric( final MsgFrontier.CrawlRequestOrBuilder crawlRequest,
                                                              final VisitState visitState,
                                                              final EnumFetchStatus.Enum fetchStatus ) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, fetchStatus.getNumber());
  }

  static MsgCrawler.FetchInfo fetchInfoFailedBlackList(MsgFrontier.CrawlRequestOrBuilder crawlRequest, VisitState visitState) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.BLACKLISTED);
  }

  static MsgCrawler.FetchInfo fetchInfoFailedFiltered(MsgFrontier.CrawlRequestOrBuilder crawlRequest, VisitState visitState) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.CRAWLER_FILTERED);
  }

  static MsgCrawler.FetchInfo fetchInfoFailedRobots(MsgFrontier.CrawlRequestOrBuilder crawlRequest, VisitState visitState) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.ROBOTS_DENIED);
  }


  static void failedCrawlRequest( final Frontier frontier, final VisitState visitState,
                                  final MsgURL.Key schemeAuthorityProto, final byte[] minimalCrawlRequestSerialized, Class<? extends Throwable> exceptionClass) {
    if (minimalCrawlRequestSerialized == VisitState.ROBOTS_PATH) // skip for robots.txt
      return;
    try {
      final MsgFrontier.CrawlRequest.Builder crawlRequest = createCrawlRequest(schemeAuthorityProto, minimalCrawlRequestSerialized);
      if (ExceptionHelper.EXCEPTION_TO_FETCH_STATUS.containsKey(exceptionClass))
        frontier.enqueue(fetchInfoFailedGeneric(crawlRequest, visitState, ExceptionHelper.EXCEPTION_TO_FETCH_STATUS.getInt(exceptionClass)));
      else
        frontier.enqueue(fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.UNKNOWN_FAILURE));
    } catch (InvalidProtocolBufferException ipbe) {
      throw new UnexpectedException(ipbe);
    }
    frontier.fetchingFailedCount.incrementAndGet();
  }

  static void drainVisitStateForError( final Frontier frontier, final VisitState visitState ) throws InterruptedException, IOException {
    try {
      final MsgURL.Key schemeAuthorityProto = PulsarHelper.schemeAuthority(visitState.schemeAuthority).build();
      LOGGER.debug("Draining " + visitState.size() + " + " + frontier.virtualizer.count(visitState) + " crawl request(s) for host " + Serializer.URL.Key.toString(schemeAuthorityProto));
      while (frontier.virtualizer.count(visitState) > 0 || !visitState.isEmpty()) {

        frontier.rc.ensureNotPaused();
        synchronized (visitState) {
          if (frontier.virtualizer.count(visitState) > 0)
            frontier.virtualizer.dequeueCrawlRequests(visitState, 100);

          while (!visitState.isEmpty()) {
            final byte[] minimalCrawlRequestSerialized = visitState.dequeue(); // contains a zPathQuery
            frontier.numberOfDrainedURLs.incrementAndGet();
            failedCrawlRequest(frontier, visitState, schemeAuthorityProto, minimalCrawlRequestSerialized, visitState.lastExceptionClass);
          }
        }
      }
    } catch (com.google.common.util.concurrent.UncheckedExecutionException e) {
      LOGGER.error("Unchecked Exception",e);
      visitState.schedulePurge(frontier.numberOfDrainedURLs);
    }
  }
}
