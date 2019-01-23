package it.unimi.di.law.bubing.parser;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.link.EnumRel;
import com.exensa.wdl.protobuf.link.EnumType;
import com.exensa.wdl.protobuf.link.MsgLink;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.parser.html.LinksHandler;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.message.BasicHeaderValueParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class LinksHelper
{
  static HTMLLink fromHttpHeader( final String header ) {
    return HttpLinksHeaderParser.tryParse( header );
  }

  /*
  static List<HTMLLink> getLinks( final URI uri, final PageInfo pageInfo, final LinksHandler linksHandler ) {
    final URI headerBase = uri; // FIXME: should we use Content-Location
    final URI baseOpt = linksHandler.getBaseOpt() == null ? null : BURL.parse( linksHandler.getBaseOpt() );
    final URI contentBase = baseOpt == null ? uri : uri.resolve( baseOpt );
  }

  static void processLinks( final URI uri, final PageInfo pageInfo, final LinksHandler linksHandler, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder ) {
    final MsgCrawler.FetchLinkInfo.Builder fetchLinkInfoBuilder = MsgCrawler.FetchLinkInfo.newBuilder();
    final MsgLink.LinkInfo.Builder linkInfoBuilder = MsgLink.LinkInfo.newBuilder();

    final URI headerBase = uri; // FIXME: should we use Content-Location
    final URI baseOpt = linksHandler.getBaseOpt() == null ? null : BURL.parse( linksHandler.getBaseOpt() );
    final URI contentBase = baseOpt == null ? uri : uri.resolve( baseOpt );

    processLinks( uri, headerBase, pageInfo.getHeaderLinks(), fetchInfoBuilder, fetchLinkInfoBuilder, linkInfoBuilder );
    processLinks( uri, headerBase, pageInfo.getRedirectLinks(), fetchInfoBuilder, fetchLinkInfoBuilder, linkInfoBuilder );
    processLinks( uri, contentBase, linksHandler.getLinks(), fetchInfoBuilder, fetchLinkInfoBuilder, linkInfoBuilder );
  }
  */

  private static void processLinks( final URI uri, final URI base,
                                    final List<HTMLLink> links,
                                    final MsgCrawler.FetchInfo.Builder fetchInfoBuilder,
                                    final MsgCrawler.FetchLinkInfo.Builder fetchLinkInfoBuilder,
                                    final MsgLink.LinkInfo.Builder linkInfoBuilder ) {
    for ( final HTMLLink link : links ) {
      final URI targetURI = getTargetURI( link.uri, base );

      if ( targetURI == null )
        continue;
      if ( !trySetLinkInfos(link,linkInfoBuilder) )
        continue;

      fetchLinkInfoBuilder.setTarget( PulsarHelper.fromURI(targetURI) );
      fetchLinkInfoBuilder.setLinkInfo( linkInfoBuilder );

      if ( isSameSchemeAndHost(uri,targetURI) ) // FIXME: was isSameSchemeAndAuthority
        fetchInfoBuilder.addInternalLinks( fetchLinkInfoBuilder );
      else
        fetchInfoBuilder.addExternalLinks( fetchLinkInfoBuilder );
    }
  }

  private static boolean isSameSchemeAndHost( final URI left, final URI right ) {
    return left.getScheme().equals( right.getScheme() ) &&
      left.getHost().equals( right.getHost() );
  }

  private static URI getTargetURI( final String href, final URI base ) {
    final URI url = BURL.parse( href );
    return url == null ? null : base.resolve( url );
  }

  public static boolean trySetLinkInfos( final HTMLLink link, final MsgLink.LinkInfo.Builder linkInfoBuilder ) {
    final String type = link.type;
    if ( type == HTMLLink.Type.A || type == HTMLLink.Type.IMG ) {
      if ( !processRels(linkInfoBuilder,link.rel,allowedRelsMap_Anchors) )
        return false;
      linkInfoBuilder.setLinkType( EnumType.Enum.A );
    }
    else
    if ( type == HTMLLink.Type.LINK ) {
      if ( !processRels(linkInfoBuilder,link.rel,allowedRelsMap_Links) )
        return false;
      linkInfoBuilder.setLinkType( EnumType.Enum.LINK );
    }
    else
    if ( type == HTMLLink.Type.REDIRECT ) {
      linkInfoBuilder.setLinkType( EnumType.Enum.REDIRECT );
    }
    else
      return false;
    // Completely ignores NOFOLLOW links (TODO: ideally, should be done in Frontier Manager)
    if ((linkInfoBuilder.getLinkRel() & EnumRel.Enum.NOFOLLOW_VALUE) != 0)
      return false;
    if ( link.text != null )
      linkInfoBuilder.setText( link.text );
    else
      linkInfoBuilder.clearText();
    linkInfoBuilder.setLinkQuality( 1.0f );
    return true;
  }

  private static boolean processRels( final MsgLink.LinkInfo.Builder linkInfoBuilder, final String rel, final Object2IntOpenHashMap<String> map ) {
    int relValue = 0;
    if ( rel != null ) {
      final String[] rels = SPLIT_PATTERN.split( rel.toLowerCase().trim() );
      for ( final String r : rels ) {
        if ( excludeRels.contains(r) )
          return false;
        relValue |= map.getInt(r);
      }
    }
    linkInfoBuilder.setLinkRel( relValue );
    return true;
  }

  private static final Pattern SPLIT_PATTERN = Pattern.compile( "\\s+|,\\s*" );

  private static final Set<String> excludeRels = makeSet(
    "stylesheet",
    "prefetch",
    "dns-prefetch",
    "preconnect",
    "preload",
    "prerender",
    "shortcut",
    "icon",
    "mask-icon",
    "meta",
    "apple-touch-icon",
    "apple-touch-icon-precomposed",
    "apple-touch-startup-image",
    "image_src",
    "edituri",
    "https://api.w.org/",
    "manifest",
    "wlwmanifest",
    "profile",
    "publisher",
    "enclosure",
    "pingback"
  );

  private static final Object2IntOpenHashMap<String> allowedRelsMap_Links = makeMap(
    E( "canonical", EnumRel.Enum.CANONICAL_VALUE ),
    E( "shortlink", EnumRel.Enum.SHORTLINK_VALUE ),
    E( "index",     EnumRel.Enum.INDEX_VALUE ),
    E( "search",    EnumRel.Enum.SEARCH_VALUE ),
    E( "alternate", EnumRel.Enum.ALTERNATE_VALUE ),
    E( "start",     EnumRel.Enum.FIRST_VALUE ),
    E( "first",     EnumRel.Enum.FIRST_VALUE ),
    E( "begin",     EnumRel.Enum.FIRST_VALUE ),
    E( "prev",      EnumRel.Enum.PREV_VALUE ),
    E( "previous",  EnumRel.Enum.PREV_VALUE ),
    E( "next",      EnumRel.Enum.NEXT_VALUE ),
    E( "last",      EnumRel.Enum.LAST_VALUE ),
    E( "end",       EnumRel.Enum.LAST_VALUE ),
    E( "home",      EnumRel.Enum.HOME_VALUE ),
    E( "author",    EnumRel.Enum.AUTHOR_VALUE ),
    E( "license",   EnumRel.Enum.LICENSE_VALUE ),
    E( "archives",  EnumRel.Enum.ARCHIVES_VALUE ),
    E( "archive",   EnumRel.Enum.ARCHIVES_VALUE )
  );

  private static final Object2IntOpenHashMap<String> allowedRelsMap_Anchors = makeMap(
    E( "nofollow",   EnumRel.Enum.NOFOLLOW_VALUE ),
    E( "noopener",   EnumRel.Enum.NOOPENER_VALUE ),
    E( "noreferrer", EnumRel.Enum.NOREFERRER_VALUE ),
    E( "canonical",  EnumRel.Enum.CANONICAL_VALUE ),
    E( "bookmark",   EnumRel.Enum.BOOKMARK_VALUE ),
    E( "shortlink",  EnumRel.Enum.SHORTLINK_VALUE ),
    E( "tag",        EnumRel.Enum.TAG_VALUE ),
    E( "category",   EnumRel.Enum.TAG_VALUE ),
    E( "index",      EnumRel.Enum.INDEX_VALUE ),
    E( "search",     EnumRel.Enum.SEARCH_VALUE ),
    E( "alternate",  EnumRel.Enum.ALTERNATE_VALUE ),
    E( "start",      EnumRel.Enum.FIRST_VALUE ),
    E( "first",      EnumRel.Enum.FIRST_VALUE ),
    E( "begin",      EnumRel.Enum.FIRST_VALUE ),
    E( "prev",       EnumRel.Enum.PREV_VALUE ),
    E( "previous",   EnumRel.Enum.PREV_VALUE ),
    E( "next",       EnumRel.Enum.NEXT_VALUE ),
    E( "last",       EnumRel.Enum.LAST_VALUE ),
    E( "end",        EnumRel.Enum.LAST_VALUE ),
    E( "home",       EnumRel.Enum.HOME_VALUE ),
    E( "author",     EnumRel.Enum.AUTHOR_VALUE ),
    E( "license",    EnumRel.Enum.LICENSE_VALUE ),
    E( "archives",   EnumRel.Enum.ARCHIVES_VALUE ),
    E( "archive",    EnumRel.Enum.ARCHIVES_VALUE ),
    E( "external",   EnumRel.Enum.EXTERNAL_VALUE )
  );

  private static <T> ObjectOpenHashSet<T> makeSet( T... elements ) {
    return new ObjectOpenHashSet<>( elements );
  }

  private static <K> Map.Entry<K,Integer> E( final K key, final int value ) {
    return new java.util.AbstractMap.SimpleImmutableEntry<>( key, value );
  }

  private static <K> Object2IntOpenHashMap<K> makeMap( Map.Entry<K,Integer>... entries ) {
    final Object2IntOpenHashMap<K> map = new Object2IntOpenHashMap<>();
    for ( final Map.Entry<K,Integer> e : entries )
      map.put( e.getKey(), (int)e.getValue() );
    return map;
  }

  private static final class HttpLinksHeaderParser
  {
    private static final Logger LOGGER = LoggerFactory.getLogger( HttpLinksHeaderParser.class );
    private static final Pattern PATTERN = Pattern.compile( "\\s*<(.+)>\\s*(.*)" );

    static HTMLLink tryParse( final String header ) {
      final Matcher m = PATTERN.matcher( header );
      if ( !m.matches() ) {
        if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "failed to parse '{}'", header );
        return null;
      }

      final String href = m.group(1);
      final NameValuePair[] parameters = parseParameters( m.group(2) );
      final HashMap<String, String> map = new HashMap<>();
      for ( final NameValuePair nvp : parameters ) {
        final String name = nvp.getName().toLowerCase( Locale.ENGLISH );
        final String value = nvp.getValue();
        map.putIfAbsent( name, value );
      }

      return new HTMLLink( HTMLLink.Type.LINK, href, map.get("title"), null, map.get("rel") );
    }

    private static NameValuePair[] parseParameters( final String parameters ) {
      try {
        return BasicHeaderValueParser.parseParameters( parameters, null ); // thread-safe
      }
      catch ( ParseException e ) {
        if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "failed to parse parameters '{}'", parameters );
        return new NameValuePair[0];
      }
    }
  }
}
