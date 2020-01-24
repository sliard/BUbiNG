package it.unimi.di.law.bubing.frontier;

import com.exensa.util.compression.HuffmanModel;
import com.exensa.wdl.protobuf.crawler.EnumFetchStatus;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.protobuf.ByteString;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.util.BURL;

import java.net.URI;

public class FetchInfoHelper {
  static MsgFrontier.CrawlRequest.Builder createCrawlRequest( final MsgURL.Key schemeAuthority, final byte[] zpath ) {
    return MsgFrontier.CrawlRequest.newBuilder().setUrlKey(
      MsgURL.Key.newBuilder()
        .setScheme(schemeAuthority.getScheme())
        .setZDomain(schemeAuthority.getZDomain())
        .setZHostPart(schemeAuthority.getZHostPart())
        .setZPathQuery(ByteString.copyFrom(zpath))
    );
  }


  static MsgCrawler.FetchInfo fetchInfoFailedGeneric(MsgFrontier.CrawlRequest crawlRequest, EnumFetchStatus.Enum status) {
    MsgCrawler.FetchInfo.Builder fetchInfoBuilder = MsgCrawler.FetchInfo.newBuilder();
    fetchInfoBuilder
      .setUrlKey(crawlRequest.getUrlKey())
      .setFetchStatus(status);
    return fetchInfoBuilder.build();
  }

  static MsgCrawler.FetchInfo fetchInfoFailedBlackList(MsgFrontier.CrawlRequest crawlRequest) {
    return fetchInfoFailedGeneric(crawlRequest, EnumFetchStatus.Enum.BLACKLISTED);
  }

  static MsgCrawler.FetchInfo fetchInfoFailedFiltered(MsgFrontier.CrawlRequest crawlRequest) {
    return fetchInfoFailedGeneric(crawlRequest, EnumFetchStatus.Enum.CRAWLER_FILTERED);
  }

  static MsgCrawler.FetchInfo fetchInfoFailedRobots(MsgFrontier.CrawlRequest crawlRequest) {
    return fetchInfoFailedGeneric(crawlRequest, EnumFetchStatus.Enum.ROBOTS_DENIED);
  }

  static void drainVisitStateForError( final Frontier frontier, final VisitState visitState ) throws InterruptedException {
    final MsgURL.Key schemeAuthorityProto = PulsarHelper.schemeAuthority(visitState.schemeAuthority).build();

    while ( !visitState.isEmpty() ) {
      frontier.rc.ensureNotPaused();
      final byte[] zpath = visitState.dequeue(); // contains a zPathQuery
      final MsgFrontier.CrawlRequest.Builder crawlRequest = createCrawlRequest( schemeAuthorityProto, zpath );
      final URI url = BURL.fromNormalizedSchemeAuthorityAndPathQuery( visitState.schemeAuthority, HuffmanModel.defaultModel.decompress(zpath) );
      frontier.enqueue(fetchInfoFailedGeneric( crawlRequest.build(), EnumFetchStatus.Enum.HOST_INVALID));
      frontier.fetchingFailedHostCount.incrementAndGet();
      frontier.fetchingFailedCount.incrementAndGet();
    }
  }

}
