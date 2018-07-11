package it.unimi.di.law.bubing.frontier.comm;

import com.exensa.wdl.protobuf.url.MsgURL;
import com.exensa.wdl.protobuf.url.EnumScheme;
import it.unimi.di.law.bubing.util.MurmurHash3_128;

import java.net.URI;


public class PulsarHelper
{
  private static final int seed = 0xa5f27b9f;
  private static final long LONG_SIGN_MASK = 0x7fffffffffffffffL;

  public static long hash( final MsgURL.URL url ) {
    final MurmurHash3_128.LongPair pair = new MurmurHash3_128.LongPair();
    MurmurHash3_128.murmurhash3_x64_128( url.getHost().toCharArray(), seed, pair );
    return pair.val1 ^ pair.val2; // FIXME: xor is probably useless
  }

  public static int getTopic( final MsgURL.URL url, final int topicCount ) {
    final long hash = hash( url );
    return (int)((hash & LONG_SIGN_MASK) % topicCount);
  }

  public static MsgURL.URL fromURI( final URI uri ) {
    return MsgURL.URL.newBuilder()
      .setScheme( getScheme(uri.getScheme()) )
      .setHost( uri.getHost() )
      .setPathQuery( uri.getRawPath() + uri.getRawQuery() )
      .build();
  }

  public static URI toURI( final MsgURL.URL url ) {
    return URI.create( toString(url) );
  }

  public static String toString( final MsgURL.URL url ) {
    return getScheme(url.getScheme()) + url.getHost() + url.getPathQuery();
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
