package it.unimi.di.law.bubing.parser;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.link.MsgLink;
import com.exensa.wdl.protobuf.link.EnumRel;
import com.exensa.wdl.protobuf.link.EnumType;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.parser.html.*;
import it.unimi.di.law.bubing.parser.html.LinksHandler.Link;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.ByteArrayCharSequence;
import it.unimi.di.law.bubing.util.cld2.Cld2Result;
import it.unimi.di.law.bubing.util.cld2.Cld2Tool;
import it.unimi.di.law.bubing.util.detection.*;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import net.htmlparser.jericho.StreamedSource;
import org.apache.http.*;
import org.apache.http.message.BasicHeaderValueParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.htmlparser.jericho.HTMLElementName.*;


public final class XHTMLParser implements Parser<Void>
{
  private static final Logger LOGGER = LoggerFactory.getLogger( XHTMLParser.class );
  private static final int CHAR_BUFFER_SIZE = 1024 * 1024; // The size of the internal Jericho buffer.
  private static final int MAX_CHARSET_PAGE_CONTENT = 5000; // The max required amount of page content (without HTML entities) for charset detection
  private static final int MIN_CLD2_PAGE_CONTENT = 6;
  private static final int MAX_CLD2_PAGE_CONTENT = 5000;
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
    final LinksHandler linksHandler = new LinksHandler( MAX_ANCHOR_TEXT_LENGTH );
    final XhtmlContentHandler xhtmlContentHandler = new XhtmlContentHandler( metadata, htmlContentHandler, linksHandler );
    final JerichoToXhtml jerichoToXhtml = new JerichoToXhtml( xhtmlContentHandler );
    final JerichoHtmlBackup jerichoHtmlBackup = new JerichoHtmlBackup( rewritten );

    final StreamedSource streamedSource = new StreamedSource(new InputStreamReader( contentStream, pageInfo.guessedCharset ));
    streamedSource.setBuffer( buffer );
    final JerichoParser jerichoParser = new JerichoParser( streamedSource, new JerichoParser.TeeHandler(jerichoToXhtml,jerichoHtmlBackup) );

    try {
      jerichoParser.parse();
    }
    catch ( JerichoParser.ParseException e ) {
      LOGGER.warn( "Failed to parse HTML from " + uri.toString(), e );
      return null;
    }

    pageInfo.extractFromContent( htmlContentHandler );
    LinksHelper.processLinks( uri, pageInfo, linksHandler, fetchInfoBuilder );
    updateDigestForRedirection( httpResponse );

    LOGGER.info( "Finished parsing {}, outlinks : {}/{} ", uri, fetchInfoBuilder.getExternalLinksCount(), fetchInfoBuilder.getInternalLinksCount() );
    return digestAppendable.digest();
  }

  private void init( final URI uri ) {
    metadata.clear();
    pageInfo = new PageInfo( uri, charsetDetector );
    digestAppendable.init( uri );
    pureTextAppendable.init();
    rewritten.setLength( 0 );
  }

  private void updateDigestForRedirection( final HttpResponse httpResponse ) {
    // This is to avoid collapsing 3xx pages with boilerplate content (as opposed to 0-length content).
    if ( httpResponse.getStatusLine().getStatusCode()/100 == 3 ) {
      digestAppendable.append( (char)0 );
      if ( pageInfo.locationDetectionInfo.httpHeaderLocation != null )
        digestAppendable.append( pageInfo.locationDetectionInfo.httpHeaderLocation.toString() );
      digestAppendable.append( (char)0 );
      if ( pageInfo.locationDetectionInfo.htmlRefreshLocation != null )
        digestAppendable.append( pageInfo.locationDetectionInfo.htmlRefreshLocation.toString() );
      digestAppendable.append( (char)0 );
    }
  }

  @Override
  public Charset guessedCharset() {
    return pageInfo.guessedCharset;
  }

  @Override
  public Locale guessedLanguage() {
    return pageInfo.guessedLanguage;
  }

  @Override
  public CharsetDetectionInfo getCharsetDetectionInfo() {
    return pageInfo.charsetDetectionInfo;
  }

  @Override
  public LanguageDetectionInfo getLanguageDetectionInfo() {
    return pageInfo.languageDetectionInfo;
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
    return pageInfo.hasViewportMeta;
  }

  @Override
  public Boolean html5() {
    return pageInfo.htmlVersionAtLeast5;
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

  private static final class PageInfo
  {
    private static final Pattern HTTP_EQUIV_PATTERN = Pattern.compile( ".*http-equiv\\s*=\\s*(.+)", Pattern.CASE_INSENSITIVE );
    private static final Pattern CONTENT_TYPE_PATTERN = Pattern.compile( "['\"]?content-type['\"]?\\s*(.+)", Pattern.CASE_INSENSITIVE );
    private static final Pattern CONTENT_LANGUAGE_PATTERN = Pattern.compile( "['\"]?content-language['\"]?\\s*(.+)", Pattern.CASE_INSENSITIVE );
    private static final Pattern CONTENT_PATTERN = Pattern.compile( "\\s*content\\s*=\\s*['\"]([^'\"]+)['\"]", Pattern.CASE_INSENSITIVE );
    private static final Pattern REFRESH_PATTERN = Pattern.compile( "['\"]?refresh['\"]?\\s*(.+)", Pattern.CASE_INSENSITIVE );
    private static final Pattern ROBOTS_PATTERN = Pattern.compile( "['\"]?robots['\"]?\\s*(.+)", Pattern.CASE_INSENSITIVE );
    private static final Pattern REFRESH_CONTENT_PATTERN = Pattern.compile( "\\s*content\\s*=\\s*['\"]?(?:\\d+;\\s*)URL\\s*=\\s*['\"]?([^'\"]+)", Pattern.CASE_INSENSITIVE );

    private final URI baseUri;
    private final HtmlCharsetDetector charsetDetector;
    private final CharsetDetectionInfo charsetDetectionInfo;
    private final LanguageDetectionInfo languageDetectionInfo;
    private final LocationDetectionInfo locationDetectionInfo;
    private final RobotsTagState robotsTagState;
    private final ArrayList<Link> headerLinks;
    private Charset guessedCharset;
    private Locale guessedLanguage;
    private URI guessedLocation;
    private boolean hasViewportMeta;
    private boolean htmlVersionAtLeast5;

    PageInfo( final URI baseUri ) {
      this( baseUri, new HtmlCharsetDetector(MAX_CHARSET_PAGE_CONTENT) );
    }

    PageInfo( final URI baseUri, final HtmlCharsetDetector charsetDetector ) {
      this.baseUri = baseUri;
      this.charsetDetector = charsetDetector;
      this.charsetDetectionInfo = new CharsetDetectionInfo();
      this.languageDetectionInfo = new LanguageDetectionInfo();
      this.locationDetectionInfo = new LocationDetectionInfo();
      this.robotsTagState = new RobotsTagState();
      this.headerLinks = new ArrayList<>();
      this.guessedCharset = StandardCharsets.UTF_8;
      this.guessedLanguage = null;
      this.guessedLocation = baseUri;
      this.hasViewportMeta = false;
      this.htmlVersionAtLeast5 = false;
    }

    void extractFromHttpHeader( final HttpResponse httpResponse ) {
      final boolean dummy =
        tryExtractCharsetFromHeader( httpResponse ) |
        tryExtractLanguageFromHeader( httpResponse ) |
        tryExtractLocationFromHeader( httpResponse ) |
        tryExtractContentLocationFromHeader( httpResponse ) |
        tryExtractRobotstagFromHeader( httpResponse ) |
        tryExtractLinksFromHeader( httpResponse );
    }

    void extractFromMetas( final HttpResponse httpResponse ) throws IOException {
      final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
      final List<ByteArrayCharSequence> allMetaEntries = HTMLParser.getAllMetaEntries( contentStream.buffer, contentStream.inspectable );
      for ( final ByteArrayCharSequence meta : allMetaEntries ) {
        final boolean dummy =
          tryExtractHttpEquivFromMeta( meta ) ||
          tryExtractCharsetFromMeta( meta ) ||
          tryExtractViewportFromMeta( meta ) ||
          tryExtractRobotsFromMeta( meta );
      }
    }

    void extractFromHtml( final HttpResponse httpResponse, final char[] buffer ) throws IOException {
      final boolean dummy =
        tryExtractHtmlVersion( httpResponse ) |
        tryExtractLanguageFromHtml( httpResponse ) |
        tryGuessCharsetFromHtml( httpResponse, buffer );
    }

    void extractFromContent( final HtmlContentHandler htmlContentHandler ) {
      final boolean dummy =
        tryGuessLanguageFromContent( htmlContentHandler );
    }

    List<Link> getLocationLinks() {
      return java.util.stream.Stream.of(
          locationDetectionInfo.httpHeaderLocation,
          locationDetectionInfo.htmlRefreshLocation,
          locationDetectionInfo.httpHeaderContentLocation )
        .filter( Objects::nonNull )
        .map( (uri) -> new Link( LINK, uri.toString(), null, null, null ) )
        .collect( java.util.stream.Collectors.toList() );
    }

    // internal --------------------------------------------------------------------------------------------------------------------

    private boolean tryExtractCharsetFromHeader( final HttpResponse httpResponse ) {
      // TODO: check if it will make sense to use httpResponse.getLocale()
      final Header contentTypeHeader = httpResponse.getEntity().getContentType();
      if ( contentTypeHeader == null ) return false;
      final String charsetName = HTMLParser.getCharsetNameFromHeader( contentTypeHeader.getValue() );
      if ( charsetName == null ) return false;
      charsetDetectionInfo.httpHeaderCharset = charsetName;
      return true; // FIXME: as original code, do not set guessedCharset from HTTP HEADER
      //return trySetGuessedCharsetFrom( charsetName, "in HTTP HEADER" );
    }

    private boolean tryExtractLanguageFromHeader( final HttpResponse httpResponse ) {
      final Header contentLanguageHeader = httpResponse.getFirstHeader( "Content-Language" );
      if ( contentLanguageHeader == null ) return false;
      final String languageTag = contentLanguageHeader.getValue();
      if ( languageTag == null ) return false;
      languageDetectionInfo.httpHeaderLanguage = languageTag;
      return trySetGuessedLanguageFrom( languageTag, "in HTTP HEADER" );
    }

    private boolean tryExtractLocationFromHeader( final HttpResponse httpResponse ) {
      final Header locationHeader = httpResponse.getFirstHeader( "Location" );
      if ( locationHeader == null ) return false;
      final String location = locationHeader.getValue();
      if ( location == null ) return false;
      locationDetectionInfo.httpHeaderLocation = trySetGuessedLocationFrom( "Location", location, "in HTTP HEADER" );
      return locationDetectionInfo.httpHeaderLocation != null;
    }

    private boolean tryExtractContentLocationFromHeader( final HttpResponse httpResponse ) {
      final Header contentLocationHeader = httpResponse.getFirstHeader( "Content-Location" );
      if ( contentLocationHeader == null ) return false;
      final String location = contentLocationHeader.getValue();
      if ( location == null ) return false;
      locationDetectionInfo.httpHeaderContentLocation = trySetGuessedLocationFrom( "Content-Location", location, "in HTTP HEADER" );
      return locationDetectionInfo.httpHeaderContentLocation != null;
    }

    private boolean tryExtractRobotstagFromHeader( final HttpResponse httpResponse ) {
      final Header[] headers = httpResponse.getHeaders( "X-Robots-Tag" );
      if ( headers.length == 0 ) return false;
      for ( final Header h : headers )
        robotsTagState.add( h.getValue() );
      return true;
    }

    private boolean tryExtractLinksFromHeader( final HttpResponse httpResponse ) {
      final Header[] headers = httpResponse.getHeaders( "Link" );
      if ( headers.length == 0 ) return false;
      for ( final Header h : headers ) {
        final Link link = LinksHelper.fromHttpHeader( h.getValue() );
        if ( link != null )
          headerLinks.add( link );
      }
      return true;
    }

    private boolean tryExtractHttpEquivFromMeta( final ByteArrayCharSequence meta ) {
      final Matcher mHttpEquiv = HTTP_EQUIV_PATTERN.matcher( meta );
      if ( !mHttpEquiv.matches() ) return false;
      final String httpEquiv = mHttpEquiv.group(1);
      if ( httpEquiv == null ) return false;
      return tryExtractCharsetFromHttpEquiv( httpEquiv ) ||
        tryExtractLanguageFromHttpEquiv( httpEquiv ) ||
        tryExtractRefreshFromHttpEquiv( httpEquiv );
    }

    private boolean tryExtractCharsetFromMeta( final ByteArrayCharSequence meta ) {
      final String charsetName = HTMLParser.getCharsetNameFromHeader( meta );
      if ( charsetName == null ) return false;
      charsetDetectionInfo.htmlMetaCharset = charsetName;
      return trySetGuessedCharsetFrom( charsetName, "in META CHARSET" );
    }

    private boolean tryExtractViewportFromMeta( final ByteArrayCharSequence meta ) {
      if ( !HTMLParser.VIEWPORT_PATTERN.matcher(meta).matches() )
        return false;
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found viewport in META of {}", baseUri.toString() );
      hasViewportMeta = true;
      return true;
    }

    private boolean tryExtractRobotsFromMeta( final ByteArrayCharSequence meta ) {
      final Matcher mRobots = ROBOTS_PATTERN.matcher( meta );
      if ( !mRobots.matches() ) return false;
      final Matcher mContent = CONTENT_PATTERN.matcher( mRobots.group(1) );
      if ( !mContent.matches() ) return false;
      final String robotsTags = mContent.group(1);
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found robots {} in META of {}", robotsTags, baseUri.toString() );
      robotsTagState.add( robotsTags );
      return true;
    }

    private boolean tryExtractCharsetFromHttpEquiv( final String httpEquiv ) {
      final Matcher mContentType = CONTENT_TYPE_PATTERN.matcher( httpEquiv );
      if ( !mContentType.matches() ) return false;
      final Matcher mContent = CONTENT_PATTERN.matcher( mContentType.group(1) );
      if ( !mContent.matches() ) return false;
      final String charsetName = HTMLParser.getCharsetNameFromHeader( mContent.group(1) );
      if ( charsetName == null ) return false;
      charsetDetectionInfo.htmlMetaCharset = charsetName;
      return trySetGuessedCharsetFrom( charsetName, "in META HTTP-EQUIV" );
    }

    private boolean tryExtractLanguageFromHttpEquiv( final String httpEquiv ) {
      final Matcher mContentLanguage = CONTENT_LANGUAGE_PATTERN.matcher( httpEquiv );
      if ( !mContentLanguage.matches() ) return false;
      final Matcher mContent = CONTENT_PATTERN.matcher( mContentLanguage.group(1) );
      if ( !mContent.matches() ) return false;
      final String languageTag = mContent.group(1);
      languageDetectionInfo.htmlLanguage = languageTag;
      return trySetGuessedLanguageFrom( languageTag, "in META HTTP-EQUIV" );
    }

    private boolean tryExtractRefreshFromHttpEquiv( final String httpEquiv ) {
      final Matcher mRefresh = REFRESH_PATTERN.matcher( httpEquiv );
      if ( !mRefresh.matches() ) return false;
      final Matcher mContent = REFRESH_CONTENT_PATTERN.matcher( mRefresh.group(1) );
      if ( !mContent.matches() ) return false;
      final String location = mContent.group(1);
      locationDetectionInfo.htmlRefreshLocation = trySetGuessedLocationFrom( "Refresh", location, "in META HTTP-EQUIV" );
      return locationDetectionInfo.htmlRefreshLocation != null;
    }

    private boolean tryExtractHtmlVersion( final HttpResponse httpResponse ) throws IOException {
      final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
      final String docType = HTMLParser.getDocType( contentStream.buffer, contentStream.inspectable );
      if ( docType == null ) return false;
      htmlVersionAtLeast5 = docType.equalsIgnoreCase( "html" );
      return htmlVersionAtLeast5;
    }

    private boolean tryExtractLanguageFromHtml( final HttpResponse httpResponse ) throws IOException {
      final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
      final String languageName = HTMLParser.getLanguageName( contentStream.buffer, contentStream.inspectable );
      if ( languageName == null ) return false;
      return trySetGuessedLanguageFrom( languageName, "in HTML TAG" );
    }

    private boolean tryGuessCharsetFromHtml( final HttpResponse httpResponse, final char[] buffer ) throws IOException {
      if ( guessedCharset != null ) return false; // FIXME: see tryExtractCharsetFromHeader
      final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
      final String charsetName = charsetDetector.detect( contentStream.buffer, contentStream.inspectable, buffer );
      if ( charsetName == null ) return false;
      charsetDetectionInfo.icuCharset = charsetName;
      return trySetGuessedCharsetFrom( charsetName, "with ICU" );
    }

    private boolean tryGuessLanguageFromContent( final HtmlContentHandler htmlContentHandler ) {
      final CharSequence content = htmlContentHandler.pureTextAppendable.getContent();
      if ( content.length() < MIN_CLD2_PAGE_CONTENT )
        return false;
      final String tld = baseUri.getHost().substring( baseUri.getHost().lastIndexOf('.') + 1 );
      final String hint = guessedLanguage == null ? null : guessedLanguage.getLanguage();
      final Cld2Result result = Cld2Tool.detect( content, MAX_CLD2_PAGE_CONTENT, tld, hint ); // TODO : use encoding hints see https://github.com/CLD2Owners/cld2/blob/master/public/encodings.h
      if ( LOGGER.isTraceEnabled() ) LOGGER.trace( "Raw text submitted to language detection is {}", content.toString() );
      languageDetectionInfo.cld2Language = result.code;
      if ( !result.language.equals("Unknown") )
        return trySetGuessedLanguageFrom( result.code, "from CONTENT" );
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Unable to guess language for {}", baseUri );
      return false;
    }

    private boolean trySetGuessedCharsetFrom( final String charsetName, final String from ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug("Found charset {} {} of {}", charsetName, from, baseUri.toString() );
      try {
        guessedCharset = Charset.forName( charsetName );
        return true;
      }
      catch ( UnsupportedCharsetException e ) {
        if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Charset {} found {} is not supported", charsetName, from );
        return false;
      }
    }

    private boolean trySetGuessedLanguageFrom( final String languageTag, final String from ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found language {} {} of {}", languageTag, from, baseUri.toString() );
      try {
        guessedLanguage = new Locale.Builder().setLanguageTag( languageTag ).build();
        return true;
      }
      catch ( IllformedLocaleException e ) {
        if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Language {} found {} is not supported", languageTag, from );
        return false;
      }
    }

    private URI trySetGuessedLocationFrom( final String that, final String value, final String from ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found {} {} {} of {}", that, value, from, baseUri.toString() );
      final URI validUri = BURL.parse( value );
      if ( validUri == null ) {
        if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "{} {} found {} is not valid", that, value, from );
        return null;
      }
      guessedLocation = baseUri.resolve( validUri );
      return guessedLocation;
    }
  }

  private static final class HtmlContentHandler implements ContentHandler
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
        if ( localName == IFRAME || localName == FRAME ) {
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

  private static final class LinksHelper
  {
    static Link fromHttpHeader( final String header ) {
      return HttpLinksHeaderParser.tryParse( header );
    }

    static void processLinks( final URI uri, final PageInfo pageInfo, final LinksHandler linksHandler, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder ) {
      final MsgCrawler.FetchLinkInfo.Builder fetchLinkInfoBuilder = MsgCrawler.FetchLinkInfo.newBuilder();
      final MsgLink.LinkInfo.Builder linkInfoBuilder = MsgLink.LinkInfo.newBuilder();

      final URI headerBase = uri; // FIXME: should we use Content-Location
      final URI contentBase = linksHandler.getBaseOpt() == null ? uri : uri.resolve( linksHandler.getBaseOpt() );

      processLinks( uri, headerBase, pageInfo.headerLinks, fetchInfoBuilder, fetchLinkInfoBuilder, linkInfoBuilder );
      processLinks( uri, headerBase, pageInfo.getLocationLinks(), fetchInfoBuilder, fetchLinkInfoBuilder, linkInfoBuilder );
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
      if ( type == A || type == IMG ) {
        if ( !processRels(linkInfoBuilder,link.rel,allowedRelsMap_Anchors) )
          return false;
        linkInfoBuilder.setLinkType( EnumType.Enum.A );
      }
      else
      if ( type == LINK ) {
        if ( !processRels(linkInfoBuilder,link.rel,allowedRelsMap_Links) )
          return false;
        linkInfoBuilder.setLinkType( EnumType.Enum.LINK );
      }
      else
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

      static Link tryParse( final String header ) {
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

        return new Link( LINK, href, map.get("title"), null, map.get("rel") );
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
