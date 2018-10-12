package it.unimi.di.law.bubing.frontier.comm;

import com.exensa.util.URIex;
import com.exensa.util.compression.HuffmanModel;
import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.exensa.wdl.protobuf.url.EnumScheme;
import com.google.protobuf.ByteString;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.MurmurHash3_128;

import java.net.URI;
import java.nio.charset.StandardCharsets;


public class PulsarHelper
{
  private static final int seed = 0xa5f27b9f;
  private static final long LONG_SIGN_MASK = 0x7fffffffffffffffL;

  public static long hash( final MsgURL.URL url ) {
    final MurmurHash3_128.LongPair pair = new MurmurHash3_128.LongPair();
    MurmurHash3_128.murmurhash3_x64_128( url.getHost().toCharArray(), seed, pair );
    return pair.val1 ^ pair.val2; // FIXME: xor is probably useless
  }

  public static int getTopic( final MsgURL.Key urlkey, final int topicCount ) {
    final long hash = Serializer.Host.Key.hash( urlkey.getZHost().toByteArray() );
    return (int)((hash & LONG_SIGN_MASK) % topicCount);
  }

  public static MsgURL.Key fromURI( final URI uri ) {
    return Serializer.URL.Key.from(uri).build();
  }

  public static URI toURI( final MsgURL.URL url ) {
    return URI.create( toString(url) );
  }


  public static URI toURI( final MsgURL.Key urlKey ) {
    return URI.create( toString(urlKey) );
  }


  public static String toString( final MsgURL.Key url ) {
    return Serializer.URL.Key.toString(url);
  }
  public static String toString( final MsgURL.URL url ) {
    return getScheme(url.getScheme()) + "://" + url.getHost() + url.getPathQuery();
  }

  public static byte[] keepZPathQuery(MsgFrontier.CrawlRequest crawlRequest) {
    return  crawlRequest.getUrlKey().getZPathQuery().toByteArray();
  }

  public static byte[] schemeAuthority( final MsgURL.KeyOrBuilder urlKey ) {
    return (getScheme(urlKey.getScheme()) + "://" + Serializer.URL.from(urlKey).getHost()).getBytes(StandardCharsets.US_ASCII);
  }
  public static byte[] schemeAuthority( final MsgURL.URL url ) {
    return (getScheme(url.getScheme()) + "://" + url.getHost()).getBytes(StandardCharsets.US_ASCII);
  }
  public static MsgURL.Key.Builder schemeAuthority(final byte[] schemeAuthority) {
    MsgURL.Key.Builder urlBuilder = MsgURL.Key.newBuilder();
    urlBuilder.setZHost(ByteString.copyFrom(HuffmanModel.defaultModel.compress(BURL.hostFromSchemeAndAuthority(schemeAuthority).getBytes(StandardCharsets.US_ASCII))));
    String sa = new String(schemeAuthority, StandardCharsets.US_ASCII);
    if (sa.startsWith("https://"))
      urlBuilder.setScheme(EnumScheme.Enum.HTTPS);
    else
    if (sa.startsWith("http://"))
      urlBuilder.setScheme(EnumScheme.Enum.HTTP);
    else
      urlBuilder.setScheme(EnumScheme.Enum.UNKNOWN);
    return urlBuilder;
  }

  private static EnumScheme.Enum getScheme( final String scheme ) {
    if ( "http".equals(scheme) ) return EnumScheme.Enum.HTTP;
    else if ( "https".equals(scheme) ) return EnumScheme.Enum.HTTPS;
    else return EnumScheme.Enum.UNKNOWN;
  }

  private static String getScheme( final EnumScheme.Enum scheme ) {
    switch ( scheme ) {
      case HTTP : return "http";
      case HTTPS : return "https";
      default: return null;
    }
  }
}
