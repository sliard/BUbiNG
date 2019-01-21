package it.unimi.di.law.bubing.parser;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.link.MsgLink;
import com.exensa.wdl.protobuf.link.EnumRel;
import com.exensa.wdl.protobuf.link.EnumType;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.parser.html.*;
import it.unimi.di.law.bubing.parser.PageInfo.Link;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.detection.*;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.StreamedSource;
import org.apache.http.*;
import org.apache.http.message.BasicHeaderValueParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class XHTMLParser implements Parser<Void>
{
  private static final Logger LOGGER = LoggerFactory.getLogger( XHTMLParser.class );
  private static final int CHAR_BUFFER_SIZE = 1024 * 1024; // The size of the internal Jericho buffer.
  private static final int MAX_CHARSET_PAGE_CONTENT = 5000; // The max required amount of page content (without HTML entities) for charset detection
  private static final int REWRITTEN_INITIAL_CAPACITY = 128 * 1024;
  private static final int MAX_ANCHOR_TEXT_LENGTH = 1024;

  private final char[] buffer;
  private final DigestAppendable digestAppendable;
  private final PureTextAppendable pureTextAppendable;
  private final HtmlCharsetDetector charsetDetector;
  private final StringBuilder rewritten;
  private final XhtmlContentHandler.Metadata metadata;
  private PageInfo pageInfo;

  public XHTMLParser( final String dummy ) {
    this.buffer = new char[ CHAR_BUFFER_SIZE ];
    this.digestAppendable = new DigestAppendable();
    this.pureTextAppendable = new PureTextAppendable();
    this.charsetDetector = new HtmlCharsetDetector( MAX_CHARSET_PAGE_CONTENT );
    this.rewritten = new StringBuilder( REWRITTEN_INITIAL_CAPACITY );
    this.metadata = new XhtmlContentHandler.Metadata();
    this.pageInfo = null;
  }

  @Override
  public byte[] parse( final URI uri, final HttpResponse httpResponse, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder ) throws IOException {
    init( uri );

    pageInfo.extractFromHttpHeader( httpResponse );
    pageInfo.extractFromMetas( httpResponse );
    pageInfo.extractFromHtml( httpResponse, buffer );

    final HttpEntity entity = httpResponse.getEntity();
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) entity.getContent();
    fetchInfoBuilder.setUrlKey( PulsarHelper.fromURI(uri) );

    final HtmlContentHandler htmlContentHandler = new HtmlContentHandler( digestAppendable, pureTextAppendable );
    final LinksHandler linksHandler = new LinksHandler( pageInfo.getLinks(), MAX_ANCHOR_TEXT_LENGTH );
    final XhtmlContentHandler xhtmlContentHandler = new XhtmlContentHandler( metadata, htmlContentHandler, linksHandler );
    final JerichoToXhtml jerichoToXhtml = new JerichoToXhtml( xhtmlContentHandler );
    final JerichoHtmlBackup jerichoHtmlBackup = new JerichoHtmlBackup( rewritten );

    final StreamedSource streamedSource = new StreamedSource(new InputStreamReader( contentStream, pageInfo.getGuessedCharset() ));
    streamedSource.setBuffer( buffer );
    final JerichoParser jerichoParser = new JerichoParser( streamedSource, new JerichoParser.TeeHandler(jerichoToXhtml,jerichoHtmlBackup) );

    try {
      jerichoParser.parse();
    }
    catch ( JerichoParser.ParseException e ) {
      LOGGER.warn( "Failed to parse HTML from " + uri.toString(), e );
      return null;
    }

    pageInfo.extractFromContent( htmlContentHandler.pureTextAppendable.getContent() );
    fillFetchInfo( uri, linksHandler, fetchInfoBuilder );
    updateDigestForRedirection( httpResponse );

    LOGGER.info( "Finished parsing {}, outlinks (e:{}, i:{})", uri, fetchInfoBuilder.getExternalLinksCount(), fetchInfoBuilder.getInternalLinksCount() );
    return digestAppendable.digest();
  }

  private void init( final URI uri ) {
    metadata.clear();
    pageInfo = new PageInfo( uri, charsetDetector );
    digestAppendable.init( uri );
    pureTextAppendable.init();
    rewritten.setLength( 0 );
  }

  private void fillFetchInfo( final URI uri, final LinksHandler linksHandler, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder ) {
    fetchInfoBuilder.getRobotsTagBuilder()
      .setNOINDEX( pageInfo.getRobotsTagState().contains(RobotsTagState.NOINDEX) )
      .setNOFOLLOW( pageInfo.getRobotsTagState().contains(RobotsTagState.NOFOLLOW) )
      .setNOARCHIVE( pageInfo.getRobotsTagState().contains(RobotsTagState.NOARCHIVE) )
      .setNOSNIPPET( pageInfo.getRobotsTagState().contains(RobotsTagState.NOSNIPPET) );
    LinksHelper.processLinks( uri, pageInfo, linksHandler, fetchInfoBuilder );
  }

  private void updateDigestForRedirection( final HttpResponse httpResponse ) {
    // This is to avoid collapsing 3xx pages with boilerplate content (as opposed to 0-length content).
    if ( httpResponse.getStatusLine().getStatusCode()/100 == 3 ) {
      digestAppendable.append( (char)0 );
      if ( pageInfo.getLocationDetectionInfo().httpHeaderLocation != null )
        digestAppendable.append( pageInfo.getLocationDetectionInfo().httpHeaderLocation.toString() );
      digestAppendable.append( (char)0 );
      if ( pageInfo.getLocationDetectionInfo().htmlRefreshLocation != null )
        digestAppendable.append( pageInfo.getLocationDetectionInfo().htmlRefreshLocation.toString() );
      digestAppendable.append( (char)0 );
    }
  }

  @Override
  public Charset guessedCharset() {
    return pageInfo.getGuessedCharset();
  }

  @Override
  public Locale guessedLanguage() {
    return pageInfo.getGuessedLanguage();
  }

  @Override
  public CharsetDetectionInfo getCharsetDetectionInfo() {
    return pageInfo.getCharsetDetectionInfo();
  }

  @Override
  public LanguageDetectionInfo getLanguageDetectionInfo() {
    return pageInfo.getLanguageDetectionInfo();
  }

  @Override
  public StringBuilder getRewrittenContent() {
    return rewritten;
  }

  @Override
  public StringBuilder getTextContent() {
    return pureTextAppendable.getContent();
  }

  @Override
  public String getTitle() {
    return metadata.get( "title" );
  }

  @Override
  public Boolean responsiveDesign() {
    return pageInfo.hasViewportMeta();
  }

  @Override
  public Boolean html5() {
    return pageInfo.isHtmlVersionAtLeast5();
  }

  @Override
  public Void result() {
    return null;
  }

  @Override
  public Parser<Void> copy() {
    return new XHTMLParser( null );
  }

  @Override
  public boolean apply( @Nullable URIResponse uriResponse ) {
    final Header contentType = uriResponse.response().getEntity().getContentType();
    return contentType != null && contentType.getValue().startsWith("text/");
  }

  public static final class HtmlContentHandler implements ContentHandler
  {
    private final DigestAppendable digestAppendable;
    private final PureTextAppendable pureTextAppendable;

    HtmlContentHandler( final DigestAppendable digestAppendable, final PureTextAppendable pureTextAppendable ) {
      this.digestAppendable = digestAppendable;
      this.pureTextAppendable = pureTextAppendable;
    }

    @Override
    public void startElement( final String uri, final String localName, final String qName, final Attributes atts ) {
      if ( digestAppendable != null ) {
        digestAppendable.startTag( localName );
        if ( localName == HTMLElementName.IFRAME || localName == HTMLElementName.FRAME ) {
          final String src = atts.getValue( "src" );
          if ( src != null ) {
            digestAppendable.append( '\"' );
            digestAppendable.append( src );
            digestAppendable.append( '\"' );
          }
        }
      }
    }

    @Override
    public void endElement( final String uri, final String localName, final String qName ) {
      if ( digestAppendable != null ) {
        digestAppendable.endTag( localName );
      }
    }

    @Override
    public void characters( final char[] ch, final int start, final int length ) {
      if ( digestAppendable != null )
        digestAppendable.append( ch, start, length );
      if ( pureTextAppendable != null )
        pureTextAppendable.append( ch, start, length );
    }

    @Override
    public void ignorableWhitespace( final char[] ch, final int start, final int length ) {
      characters( ch, start, length );
    }

    @Override public void setDocumentLocator( final Locator locator ) { }
    @Override public void startDocument() { }
    @Override public void endDocument() { }
    @Override public void startPrefixMapping( final String prefix, final String uri ) { }
    @Override public void endPrefixMapping( final String prefix ) { }
    @Override public void processingInstruction( final String target, final String data ) { }
    @Override public void skippedEntity( final String name ) { }
  }

  public static final class LinksHelper
  {
    static PageInfo.Link fromHttpHeader( final String header ) {
      return HttpLinksHeaderParser.tryParse( header );
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

    private static void processLinks( final URI uri, final URI base,
                                      final List<Link> links,
                                      final MsgCrawler.FetchInfo.Builder fetchInfoBuilder,
                                      final MsgCrawler.FetchLinkInfo.Builder fetchLinkInfoBuilder,
                                      final MsgLink.LinkInfo.Builder linkInfoBuilder ) {
      for ( final Link link : links ) {
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

    private static boolean trySetLinkInfos( final Link link, final MsgLink.LinkInfo.Builder linkInfoBuilder ) {
      final String type = link.type;
      if ( type == Link.Type.A || type == Link.Type.IMG ) {
        if ( !processRels(linkInfoBuilder,link.rel,allowedRelsMap_Anchors) )
          return false;
        linkInfoBuilder.setLinkType( EnumType.Enum.A );
      }
      else
      if ( type == Link.Type.LINK ) {
        if ( !processRels(linkInfoBuilder,link.rel,allowedRelsMap_Links) )
          return false;
        linkInfoBuilder.setLinkType( EnumType.Enum.LINK );
      }
      else
      if ( type == Link.Type.REDIRECT ) {
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

      static PageInfo.Link tryParse( final String header ) {
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

        return new PageInfo.Link( Link.Type.LINK, href, map.get("title"), null, map.get("rel") );
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
}
