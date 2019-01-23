package it.unimi.di.law.bubing.parser;

import it.unimi.di.law.bubing.parser.html.*;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.StartTagType;
import net.htmlparser.jericho.StreamedSource;
import org.apache.http.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


public final class XHTMLParser implements Parser<Void>
{
  static {
    /* As suggested by Martin Jericho. This should speed up things and avoid problems with
     * server tags embedded in weird places (e.g., JavaScript string literals). Server tags
     * should not appear in generated HTML anyway. */
    StartTagType.SERVER_COMMON.deregister();
    StartTagType.SERVER_COMMON_COMMENT.deregister();
    StartTagType.SERVER_COMMON_ESCAPED.deregister();
  }

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
  public ParseData parse( final URI uri, final HttpResponse httpResponse ) throws IOException {
    init( uri );

    pageInfo.extractFromHttpHeader( httpResponse );
    pageInfo.extractFromMetas( httpResponse );
    pageInfo.extractFromHtml( httpResponse, buffer );

    final HttpEntity entity = httpResponse.getEntity();
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) entity.getContent();

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
    updateDigestForRedirection( httpResponse );

    final List<HTMLLink> allLinks = new ArrayList<>();
    allLinks.addAll( pageInfo.getHeaderLinks() );
    allLinks.addAll( pageInfo.getRedirectLinks() );
    allLinks.addAll( pageInfo.getLinks() );

    final URI baseUriOpt = linksHandler.getBaseOpt() == null ? null : BURL.parse( linksHandler.getBaseOpt() );
    final URI baseUri = baseUriOpt == null ? uri : uri.resolve( baseUriOpt );

    return new ParseData(
      baseUri,
      metadata.get( "title" ),
      pageInfo,
      digestAppendable.digest(),
      pureTextAppendable.getContent(),
      rewritten,
      allLinks
    );
  }

  /*
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
  public URI getBase() {
  }

  @Override
  public List<HTMLLink> getLinks() {

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
  */

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

  // implementation ----------------------------------------------------------------------------------------------------------------

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
      if ( pageInfo.getLocationDetectionInfo().httpHeaderLocation != null )
        digestAppendable.append( pageInfo.getLocationDetectionInfo().httpHeaderLocation.toString() );
      digestAppendable.append( (char)0 );
      if ( pageInfo.getLocationDetectionInfo().htmlRefreshLocation != null )
        digestAppendable.append( pageInfo.getLocationDetectionInfo().htmlRefreshLocation.toString() );
      digestAppendable.append( (char)0 );
    }
  }

  // inner classes -----------------------------------------------------------------------------------------------------------------

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
}
