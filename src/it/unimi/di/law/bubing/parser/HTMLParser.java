package it.unimi.di.law.bubing.parser;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.martiansoftware.jsap.*;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.util.*;
import it.unimi.di.law.bubing.util.Util;
import it.unimi.di.law.bubing.util.cld2.Cld2Result;
import it.unimi.di.law.bubing.util.cld2.Cld2Tool;
import it.unimi.di.law.bubing.util.detection.CharsetDetectionInfo;
import it.unimi.di.law.bubing.util.detection.LanguageDetectionInfo;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.StringHttpMessages;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.util.TextPattern;
import net.htmlparser.jericho.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;

import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.link.MsgLink;
import com.exensa.wdl.protobuf.link.EnumRel;
import com.exensa.wdl.protobuf.link.EnumType;
import com.exensa.wdl.protobuf.crawler.MsgRobotsTag;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.UnsupportedCharsetException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.htmlparser.jericho.HTMLElementName.*;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

// RELEASE-STATUS: DIST

/**
 * An HTML parser with additional responsibilities.
 * An instance of this class does some buffering that makes it possible to
 * parse quickly a {@link HttpResponse}. Instances are heavyweight&mdash;they
 * should be pooled and shared, since their usage is transitory and CPU-intensive.
 */
public final class HTMLParser<T> implements Parser<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HTMLParser.class);

  static {
    /* As suggested by Martin Jericho. This should speed up things and avoid problems with
     * server tags embedded in weird places (e.g., JavaScript string literals). Server tags
     * should not appear in generated HTML anyway. */

    StartTagType.SERVER_COMMON.deregister();
    StartTagType.SERVER_COMMON_COMMENT.deregister();
    StartTagType.SERVER_COMMON_ESCAPED.deregister();
  }

  private final static HashSet<String> relExcludeList;
  static {
    String[] excludeArray = new String[]{"stylesheet",
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
      "pingback"};
    relExcludeList = Sets.newHashSet(excludeArray);
  }

  /**
   * The pattern prefixing the URL in a <code>META </code> <code>HTTP-EQUIV </code> element of refresh type.
   */
  private static final TextPattern URLEQUAL_PATTERN = new TextPattern("URL=", TextPattern.CASE_INSENSITIVE);
  /**
   * The size of the internal Jericho buffer.
   */
  private static final int CHAR_BUFFER_SIZE = 1024*1024;
  /**
   * The max required amount of page content (without HTML entities) for charset detection
   */
  private static final int MAX_CHARSET_PAGE_CONTENT = 5000;
  /**
   * The max required amount of text page content (with HTML entities) for language detection
   */
  private static final int MAX_LANGUAGE_PAGE_CONTENT = 5000;
  /**
   * The character buffer. It is set up at construction time, but it can be changed later.
   */
  private final char[] buffer;
  /**
   * An object emboding the digest logic, or {@code null} for no digest computation.
   */
  private final DigestAppendable digestAppendable;
  /**
   * A text processor, or {@code null}.
   */
  private final TextProcessor<T> textProcessor;
  /**
   * If <code>true</code>, pages with the same content but with different authorities are considered duplicates.
   */
  private final boolean crossAuthorityDuplicates;
  /**
   * A charset detector
   */
  private final HtmlCharsetDetector charsetDetector;
  /**
   * The location URL from headers of the last response, if any, or {@code null}.
   */
  private URI location;
  /**
   * The location URL from <code>META</code> elements of the last response, if any, or {@code null}.
   */
  private URI metaLocation;
  /**
   * <code>true</code> if parsing detects HTML version >= 5
   */
  private Boolean htmlVersionAtLeast5;
  /**
   * <code>true</code> if parsing found a <code><meta name="viewport" ...></code> element
   */
  private Boolean foundViewPortMeta;
  /**
   * The charset we guessed for the last response.
   */
  private Charset guessedCharset;
  /**
   * The charset we guessed for the last response.
   */
  private Locale guessedLanguage;
  /**
   * The title of the page, <code>null</code> if none
   */
  private String title;

  private CharsetDetectionInfo charsetDetectionInfo;
  private LanguageDetectionInfo languageDetectionInfo;

  /**
   * The rewritten version of the page
   */
  private static final boolean REWRITE = true; // FIXME: should be configurable
  private final StringBuilder rewritten;
  private final StringBuilder currentTextOfInterest;
  private boolean captureTextOfInterest;
  private final PureTextAppendable textContent;

  private static final HashSet<String> ENDLINE_SET = Sets.newHashSet(ARTICLE, ASIDE, FOOTER, DETAILS, SECTION, HEADER, HGROUP, NAV, P, H1, H2, H3, H4, H5, H6, UL, OL, DIR, MENU, PRE, DL, DIV, CENTER, NOSCRIPT, NOFRAMES, BLOCKQUOTE, FORM, ISINDEX, HR, TABLE, FIELDSET, ADDRESS, LI, DT, DD, TR, CAPTION, LEGEND, BR);
  private static final HashSet<String> INDENT_SET = Sets.newHashSet(LI, DD);

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page. By default, only pages from within the same
   * scheme+authority may be considered to be duplicates.
   *
   * @param hashFunction the hash function used to digest, {@code null} if no digesting will be performed.
   */
  public HTMLParser(final HashFunction hashFunction) {
    this(hashFunction, true);
  }

  /**
   * Builds a parser for link extraction and, possibly, digesting a page.
   *
   * @param hashFunction             the hash function used to digest, {@code null} if no digesting will be performed.
   * @param textProcessor            a text processor, or {@code null} if no text processing is required.
   * @param crossAuthorityDuplicates if <code>true</code>, pages with different scheme+authority but with the same content will be considered to be duplicates, as long
   *                                 as they are assigned to the same {@link Agent}.
   * @param bufferSize               the fixed size of the internal buffer; if zero, the buffer will be dynamic.
   */
  public HTMLParser(final HashFunction hashFunction, final TextProcessor<T> textProcessor, final boolean crossAuthorityDuplicates, final int bufferSize) {
    this.buffer = bufferSize != 0 ? new char[bufferSize] : null;
    this.digestAppendable = hashFunction == null ? null : new DigestAppendable( hashFunction );
    this.textProcessor = textProcessor;
    this.crossAuthorityDuplicates = crossAuthorityDuplicates;
    this.charsetDetector = new HtmlCharsetDetector( MAX_CHARSET_PAGE_CONTENT );
    this.charsetDetectionInfo = new CharsetDetectionInfo();
    this.languageDetectionInfo = new LanguageDetectionInfo();
    this.rewritten = REWRITE ? new StringBuilder() : null;
    this.currentTextOfInterest = new StringBuilder();
    this.captureTextOfInterest = false;
    this.textContent = new PureTextAppendable();
    this.htmlVersionAtLeast5 = false;
    this.foundViewPortMeta = false;
  }

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
   *
   * @param hashFunction             the hash function used to digest, {@code null} if no digesting will be performed.
   * @param crossAuthorityDuplicates if <code>true</code>, pages with different scheme+authority but with the same content will be considered to be duplicates, as long
   *                                 as they are assigned to the same {@link Agent}.
   */
  public HTMLParser(final HashFunction hashFunction, final boolean crossAuthorityDuplicates) {
    this(hashFunction, null, crossAuthorityDuplicates, CHAR_BUFFER_SIZE);
  }

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
   *
   * @param hashFunction             the hash function used to digest, {@code null} if no digesting will be performed.
   * @param textProcessor            a text processor, or {@code null} if no text processing is required.
   * @param crossAuthorityDuplicates if <code>true</code>, pages with different scheme+authority but with the same content will be considered to be duplicates, as long
   *                                 as they are assigned to the same {@link Agent}.
   */
  public HTMLParser(final HashFunction hashFunction, final TextProcessor<T> textProcessor, final boolean crossAuthorityDuplicates) {
    this(hashFunction, textProcessor, crossAuthorityDuplicates, CHAR_BUFFER_SIZE);
  }

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page. (No cross-authority duplicates are considered)
   *
   * @param messageDigest the name of a message-digest algorithm, or the empty string if no digest will be computed.
   * @throws NoSuchAlgorithmException
   */
  public HTMLParser(final String messageDigest) throws NoSuchAlgorithmException {
    this(BinaryParser.forName(messageDigest));
  }

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
   *
   * @param messageDigest            the name of a message-digest algorithm, or the empty string if no digest will be computed.
   * @param crossAuthorityDuplicates a string whose value can only be "true" or "false" that is used to determine if you want to check for cross-authority duplicates.
   * @throws NoSuchAlgorithmException
   */
  public HTMLParser(final String messageDigest, final String crossAuthorityDuplicates) throws NoSuchAlgorithmException {
    this(BinaryParser.forName(messageDigest), Util.parseBoolean(crossAuthorityDuplicates));
  }

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
   *
   * @param messageDigest            the name of a message-digest algorithm, or the empty string if no digest will be computed.
   * @param textProcessorSpec        the specification of a text processor that will be passed to an {@link ObjectParser}.
   * @param crossAuthorityDuplicates a string whose value can only be "true" or "false" that is used to determine if you want to check for cross-authority duplicates.
   * @throws NoSuchAlgorithmException
   */
  @SuppressWarnings("unchecked")
  public HTMLParser(final String messageDigest, final String textProcessorSpec, final String crossAuthorityDuplicates) throws NoSuchAlgorithmException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, IOException {
    this(BinaryParser.forName(messageDigest), (TextProcessor<T>) ObjectParser.fromSpec(textProcessorSpec), Util.parseBoolean(crossAuthorityDuplicates));
  }

  /**
   * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction only (no digesting).
   */
  public HTMLParser() {
    this(null, null, false, CHAR_BUFFER_SIZE);
  }


  private MsgFrontier.CrawlRequest.Builder makePageInfoFromURI(final URI origin) {
    MsgFrontier.CrawlRequest.Builder newPageInfo = MsgFrontier.CrawlRequest.newBuilder();
    newPageInfo.setUrlKey(PulsarHelper.fromURI(origin))
        .setCrawlOptions(MsgFrontier.CrawlRequestOptions.getDefaultInstance())
        .build();
    return newPageInfo;
  }

  private MsgCrawler.FetchLinkInfo.Builder makeLinkInfoFromBasicURI(final URI targetURI) {
    MsgCrawler.FetchLinkInfo.Builder newLinkInfo = MsgCrawler.FetchLinkInfo.newBuilder();
    newLinkInfo.setTarget( PulsarHelper.fromURI(targetURI) );
    return newLinkInfo;
  }

  /**
   * Pre-process a string that represents a raw link found in the page, trying to derelativize it. If it succeeds, the
   * resulting URL is passed to the link receiver.
   *
   * @param crawledPageInfoBuilder the link receiver that will receive the resulting URL.
   * @param base         the base URL to be used to derelativize the link.
   * @param link         the raw link to be derelativized.
   */
  private void process( final MsgCrawler.FetchInfo.Builder crawledPageInfoBuilder,
                        final byte[] schemeAuthority,
                        final URI base,
                        final String link,
                        final String anchorText,
                        final String linkName,
                        final boolean isNoFollow, final boolean isNoIndex, final boolean isCanonical) {
    if (link == null) return;
    final URI url = BURL.parse(link);
    if (url == null) return;
    final URI targetURI = base.resolve(url);

    final MsgLink.LinkInfo.Builder linkInfo = MsgLink.LinkInfo.newBuilder();
    if (linkName == HTMLElementName.A)
      linkInfo.setLinkType(EnumType.Enum.A);
    else if (linkName == HTMLElementName.AREA)
      linkInfo.setLinkType(EnumType.Enum.AREA);
    else if (linkName == HTMLElementName.EMBED)
      linkInfo.setLinkType(EnumType.Enum.EMBED);
    else if (linkName == HTMLElementName.FRAME)
      linkInfo.setLinkType(EnumType.Enum.FRAME);
    else if (linkName == HTMLElementName.IFRAME)
      linkInfo.setLinkType(EnumType.Enum.IFRAME);
    else if (linkName == HTMLElementName.IMG)
      linkInfo.setLinkType(EnumType.Enum.IMG);
    else if (linkName == HTMLElementName.LINK)
      linkInfo.setLinkType(EnumType.Enum.LINK);
    else if (linkName == HTMLElementName.OBJECT)
      linkInfo.setLinkType(EnumType.Enum.OBJECT);

    linkInfo.setLinkRel(
      (isNoFollow ? EnumRel.Enum.NOFOLLOW_VALUE : 0) |
      (isCanonical ? EnumRel.Enum.CANONICAL_VALUE : 0)
    );

    if (anchorText != null && anchorText.length() > 0)
      linkInfo.setText(anchorText);
    linkInfo.setLinkQuality(1.0f);

    final MsgCrawler.FetchLinkInfo.Builder fetchLinkInfo = makeLinkInfoFromBasicURI(targetURI);
    fetchLinkInfo.setLinkInfo( linkInfo );

    if (BURL.sameSchemeAuthority(schemeAuthority,targetURI))
      crawledPageInfoBuilder.addInternalLinks(fetchLinkInfo.buildPartial());
    else
      crawledPageInfoBuilder.addExternalLinks(fetchLinkInfo.buildPartial());
  }

  private void tryGuessCharsetFromHeader( final URI uri, final HttpResponse httpResponse ) {
    final HttpEntity entity = httpResponse.getEntity();
    // TODO: check if it will make sense to use getValue() of entity
    // Try to guess using headers
    final Header contentTypeHeader = entity.getContentType();
    if (contentTypeHeader != null) {
      charsetDetectionInfo.httpHeaderCharset = getCharsetNameFromHeader(contentTypeHeader.getValue());
      if (charsetDetectionInfo.httpHeaderCharset != null) {
        if (LOGGER.isDebugEnabled())
          LOGGER.debug("Found charset {} in HTTP HEADER of {}", charsetDetectionInfo.httpHeaderCharset, uri.toString());
        try {
          guessedCharset = Charset.forName(charsetDetectionInfo.httpHeaderCharset);
        } catch (Exception e) {
          LOGGER.debug("Charset {} found in header is not supported", charsetDetectionInfo.httpHeaderCharset);
        }
      }
    }
  }

  private void tryGuessLanguageFromHeader( final URI uri, final HttpResponse httpResponse ) {
    // Try to guess language using headers
    final Header contentLanguageHeader = httpResponse.getFirstHeader("Content-Language");
    if (contentLanguageHeader != null) {
      languageDetectionInfo.httpHeaderLanguage = contentLanguageHeader.getValue();
      if (languageDetectionInfo.httpHeaderLanguage != null) {
        if (LOGGER.isDebugEnabled())
          LOGGER.debug("Found language {} in HTTP HEADER of {}", languageDetectionInfo.httpHeaderLanguage, uri.toString());
        guessedLanguage = Locale.forLanguageTag(languageDetectionInfo.httpHeaderLanguage);
      }
    }
  }

  private void tryGetViewportFromMetas( final URI uri, List<ByteArrayCharSequence> allMetaEntries ) {
    foundViewPortMeta = getViewport(allMetaEntries);
    if ( LOGGER.isDebugEnabled() && foundViewPortMeta )
      LOGGER.debug("Found viewport in META HTML of {}", uri.toString());
  }

  // returns true if a *valid* charset is found from metas
  private boolean tryGuessCharsetFromMetas( final URI uri, List<ByteArrayCharSequence> allMetaEntries ) {
    charsetDetectionInfo.htmlMetaCharset = getCharsetName(allMetaEntries);
    if (charsetDetectionInfo.htmlMetaCharset != null) {
      if (LOGGER.isDebugEnabled())
        LOGGER.debug("Found charset {} in META HTML of {}", charsetDetectionInfo.htmlMetaCharset, uri.toString());
      try {
        guessedCharset = Charset.forName(charsetDetectionInfo.htmlMetaCharset);
        return true;
      }
      catch (Exception e) {
        LOGGER.debug("Charset {} found in HTML <meta> is not supported", charsetDetectionInfo.htmlMetaCharset);
      }
    }
    return false;
  }

  private void tryGuessHtmlVersion( final URI uri, final byte[] buffer, final int length ) {
    String docTypeContent = getDocType( buffer, Math.min(length,50) );
    if (docTypeContent != null) {
      htmlVersionAtLeast5 = docTypeContent.toLowerCase().equals("html");
      if (htmlVersionAtLeast5)
        LOGGER.debug("HTML5 document for {}", uri.toString());
    }
  }

  private void tryGuessLanguageFromHtml( final URI uri, final byte[] buffer, final int length ) {
    languageDetectionInfo.htmlLanguage = getLanguageName( buffer, length );
    if (languageDetectionInfo.htmlLanguage != null) {
      Locale fromHtmlLocal = Locale.forLanguageTag(languageDetectionInfo.htmlLanguage);
      if (!fromHtmlLocal.getLanguage().equals(""))
        guessedLanguage = fromHtmlLocal;
      if (LOGGER.isDebugEnabled())
        LOGGER.debug("Found language {} with code {} ({})in <html lang=?..> of {}", languageDetectionInfo.htmlLanguage, guessedLanguage.getLanguage(), guessedLanguage.getDisplayLanguage(), uri.toString());
    }
  }

  private void tryGuessCharsetFromICU( final URI uri, final byte[] buffer, final int length ) throws IOException {
    final String charsetName = charsetDetector.detect( buffer, length, this.buffer );
    if ( charsetName != null ) {
      charsetDetectionInfo.icuCharset = charsetName;
      if (LOGGER.isDebugEnabled())
        LOGGER.debug("Found charset {} with ICU {}", charsetDetectionInfo.icuCharset, uri.toString());
      try {
        guessedCharset = Charset.forName(charsetDetectionInfo.icuCharset);
      }
      catch (UnsupportedCharsetException e) {
        LOGGER.error("Charset {} found in header is not supported", charsetDetectionInfo.icuCharset);
      }
    }
  }

  private final class ParserImpl
  {
    private final MsgCrawler.FetchInfo.Builder fetchInfoBuilder;
    private final StreamedSource streamedSource;
    private final byte[] schemeAuthority;
    private URI base;
    private int lastSegmentEnd = 0;
    private int inSpecialText = 0;
    private int skipping = 0;
    private String linkValue = null;
    private boolean rewrite = false;

    ParserImpl( final URI uri, final InputStream contentStream, final Charset charset, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder ) throws IOException {
      this.fetchInfoBuilder = fetchInfoBuilder;
      this.streamedSource = new StreamedSource(new InputStreamReader(contentStream, charset));
      if (buffer != null) streamedSource.setBuffer(buffer);
      this.schemeAuthority = BURL.schemeAndAuthorityAsByteArray(BURL.toByteArray(uri));
      this.base = uri;
    }

    URI run() throws IOException {
      textContent.init(); // FIXME: added by Manu
      for (final Segment segment : streamedSource) {
        rewrite = true;
        if (segment.getEnd() <= lastSegmentEnd)
          continue;
        lastSegmentEnd = segment.getEnd();

        if (segment instanceof StartTag)
          onStartTag( (StartTag)segment );
        else
        if ( segment instanceof EndTag )
          onEndTag( (EndTag)segment );
        else
        if ( segment instanceof CharacterReference )
          onCharacterReference( (CharacterReference)segment );
        else
          onOtherSegment( segment );

        if (rewritten != null && rewrite && skipping == 0) {
          java.nio.CharBuffer cb = streamedSource.getCurrentSegmentCharBuffer();
          rewritten.append( cb.array(), cb.position(), cb.remaining() );
        }
      }
      textContent.flush(); // FIXME: added by Manu
      return base;
    }

    private void onStartTag( final StartTag startTag ) {
      final StartTagType startTagType = startTag.getStartTagType();

      if (startTagType == StartTagType.COMMENT)
        return;

      final String name = startTag.getName();

      if (name == HTMLElementName.TITLE) {
        captureTextOfInterest = true;
        return;
      }

      if (name == HTMLElementName.SPAN || name == HTMLElementName.FONT || name == HTMLElementName.STRONG ||
        name == HTMLElementName.I || name == HTMLElementName.B || name == HTMLElementName.EM)
        rewrite = false; // INLINE_NO_WHITESPACE
      else if (name == HTMLElementName.SCRIPT || name == HTMLElementName.OPTION || name == HTMLElementName.STYLE) {
        textContent.append(' ');
        if (!startTag.isSyntacticalEmptyElementTag()) // FIXME: may be isEmptyElementTag(), copy/paste from BUbiNG source code
          skipping += 1;
        rewrite = false; // IGNORABLE_ELEMENT
      } else if (ENDLINE_SET.contains(name))
        textContent.append('\n');
      else
        textContent.append(' ');

      if ((name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) && !startTag.isSyntacticalEmptyElementTag())
        inSpecialText++;

      if (digestAppendable != null)
        digestAppendable.append(startTag);
      // TODO: detect flow breakers

      if (name == HTMLElementName.A || name == HTMLElementName.AREA) // most frequent first
        linkValue = startTag.getAttributeValue("href");
      else if (name == HTMLElementName.LINK) {
        String rel = startTag.getAttributeValue("rel");
        if (rel != null) {
          String[] rels = rel.split("[ ,]");
          if (rels.length > 0) {
            HashSet<String> relSet = Sets.newHashSet(rels);
            if (Sets.intersection(relSet,relExcludeList).size() == 0)
              linkValue = startTag.getAttributeValue("href");
          }
        }
      }
      else if (name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME || name == HTMLElementName.EMBED)
        linkValue = startTag.getAttributeValue("src");
      else if (name == HTMLElementName.IMG || name == HTMLElementName.SCRIPT)
        //linkValue = startTag.getAttributeValue("src");
        linkValue = null;
      else if (name == HTMLElementName.OBJECT)
        //linkValue = startTag.getAttributeValue("data");
        linkValue = null;
      else if (name == HTMLElementName.BASE) {
        final String s = startTag.getAttributeValue("href");
        if (s != null) {
          final URI link = BURL.parse(s);
          if (link != null) {
            if (link.isAbsolute()) base = link;
            else if (LOGGER.isDebugEnabled()) LOGGER.debug("Found relative BASE URL: \"{}\"", link);
          }
        }
      }
      else if (name == HTMLElementName.META) {
        String metaEquiv = startTag.getAttributeValue("http-equiv");
        if (metaEquiv != null) {
          final String metaContent = startTag.getAttributeValue("content");
          if ( metaContent != null) {
            metaEquiv = metaEquiv.toLowerCase();
            if (metaEquiv.equals("refresh"))
              onMetaEquivRefresh( metaContent );
            else
            if (metaEquiv.equals("location"))
              onMetaEquivLocation( metaContent );
          }
        }
        else {
          final String metaName = startTag.getAttributeValue("name");
          final String metaContent = startTag.getAttributeValue("content");
          if ( metaName != null && metaContent != null && metaName.equalsIgnoreCase("robots") )
            onMetaRobots( metaContent.toLowerCase() );
        }
      }

      if (linkValue != null) {
        // isNoFollow = startTag.getAttributeValue("rel").equalsIgnoreCase("nofollow");
        captureTextOfInterest = true;
      }

    }

    private void onMetaEquivRefresh( final String content ) {
      // http-equiv="refresh" content="0;URL=http://foo.bar/..."
      final int pos = URLEQUAL_PATTERN.search(content);
      if ( pos == -1 ) return;
      final String urlPattern = content.substring(pos + URLEQUAL_PATTERN.length());
      final URI refresh = BURL.parse(urlPattern);
      if ( refresh == null ) return;
      // This shouldn't happen by standard, but people unfortunately does it.
      if (!refresh.isAbsolute() && LOGGER.isDebugEnabled())
        LOGGER.debug("Found relative META refresh URL: \"{}\"", urlPattern);
      // FIXME: no 'metaRefresh' ?
      fetchInfoBuilder.addExternalLinks(
        makeLinkInfoFromBasicURI( base.resolve(refresh) )
          .setLinkInfo( MsgLink.LinkInfo.newBuilder()
            .setLinkType( EnumType.Enum.REDIRECT )
          )
      );
    }

    private void onMetaEquivLocation( final String content ) {
      // http-equiv="location" content="http://foo.bar/..."
      final URI metaLocation = BURL.parse(content);
      if ( metaLocation == null ) return;
      // This shouldn't happen by standard, but people unfortunately does it.
      if (!metaLocation.isAbsolute() && LOGGER.isDebugEnabled())
        LOGGER.debug("Found relative META location URL: \"{}\"", content);
      HTMLParser.this.metaLocation = base.resolve(metaLocation);
      fetchInfoBuilder.addExternalLinks(
        makeLinkInfoFromBasicURI( HTMLParser.this.metaLocation )
          .setLinkInfo( MsgLink.LinkInfo.newBuilder()
            .setLinkType( EnumType.Enum.REDIRECT )
          )
      );
    }

    private void onMetaRobots( final String content ) {
      final MsgRobotsTag.RobotsTag.Builder robotsTagBuilder = fetchInfoBuilder.getRobotsTagBuilder();
      if (content.contains("noindex"))
        robotsTagBuilder.setNOINDEX(true);
      if (content.contains("nofollow"))
        robotsTagBuilder.setNOFOLLOW(true);
      if (content.contains("noarchive"))
        robotsTagBuilder.setNOARCHIVE(true);
      if (content.contains("nosnippet"))
        robotsTagBuilder.setNOSNIPPET(true);
    }

    private void onEndTag( final EndTag endTag ) {
      final String name = endTag.getName();
      if (name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) {
        inSpecialText = Math.max(0, inSpecialText - 1); // Ignore extra closing tags
      }

      if (digestAppendable != null) {
        if (endTag.getTagType() != EndTagType.NORMAL) // FIXME: what ?
          return;
        digestAppendable.append(endTag);
      }

      if (name == HTMLElementName.TITLE) {
        captureTextOfInterest = false;
        title = currentTextOfInterest.toString();
        currentTextOfInterest.setLength(0);
      }

      if ((linkValue != null) &&
        (name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME || name == HTMLElementName.EMBED) ||
        (name == HTMLElementName.IMG || name == HTMLElementName.SCRIPT) ||
        (name == HTMLElementName.OBJECT) ||
        (name == HTMLElementName.A || name == HTMLElementName.AREA || name == HTMLElementName.LINK)){
        process(fetchInfoBuilder, schemeAuthority, base, linkValue, currentTextOfInterest.toString(), name,false, false, false);
        linkValue = null;
        currentTextOfInterest.setLength(0);
        captureTextOfInterest = false;
      }

      if (name == HTMLElementName.SPAN || name == HTMLElementName.FONT || name == HTMLElementName.STRONG ||
        name == HTMLElementName.I || name == HTMLElementName.B || name == HTMLElementName.EM)
        rewrite = false;
      else
      if (name == HTMLElementName.SCRIPT || name == HTMLElementName.OPTION || name == HTMLElementName.STYLE) {
        textContent.append('\n');
        skipping = Math.max(0, skipping - 1); // Ignore extra closing tags
        rewrite = false;
      }
      else
      if (ENDLINE_SET.contains(name))
        textContent.append('\n');
      else
        textContent.append(' ');
    }

    private void onOtherSegment( final Segment segment ) throws IOException {
      if ( inSpecialText != 0 )
        return;

      final java.nio.CharBuffer cb = streamedSource.getCurrentSegmentCharBuffer();

      textContent.append( cb.array(), cb.position(), cb.remaining() );
      if ( captureTextOfInterest )
        currentTextOfInterest.append( cb.array(), cb.position(), cb.remaining() );
      if ( digestAppendable != null )
        digestAppendable.append( cb.array(), cb.position(), cb.remaining() );
      if ( textProcessor != null )
        textProcessor.append( cb );
    }

    private void onCharacterReference( final CharacterReference charRef ) throws IOException {
      if ( inSpecialText != 0 )
        return;

      charRef.appendCharTo( textContent );
      if ( captureTextOfInterest )
        charRef.appendCharTo( currentTextOfInterest );
      if ( digestAppendable != null )
        charRef.appendCharTo( digestAppendable );
      if ( textProcessor != null )
        charRef.appendCharTo( textProcessor );
    }
  }

  // returns base URI
  private URI doParse( final URI uri, final InputStream contentStream, final Charset charset, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder ) throws IOException {
    final ParserImpl impl = new ParserImpl( uri, contentStream, charset, fetchInfoBuilder );
    return impl.run();
  }

  private void tryGuessLanguageFromTextContent( final URI uri ) {
    String tld = uri.getHost().substring(uri.getHost().lastIndexOf('.') + 1);
    String guessedLang = guessedLanguage == null ? null : guessedLanguage.getLanguage();
    final CharSequence content = textContent.getContent();
    if (content.length() > 5) {
      //String textForLangDetect = textContent.subSequence(0, Math.min(textContent.length(), MAX_LANGUAGE_PAGE_CONTENT) - 1).toString() + " "; // +" " is Workaround CLD2 bug (SIGSEGV)
      //Cld2Result result = Cld2Tool.detect(textForLangDetect, tld, guessedLang);
      Cld2Result result = Cld2Tool.detect( content, MAX_LANGUAGE_PAGE_CONTENT, tld, guessedLang );
      if (LOGGER.isDebugEnabled())
        LOGGER.debug("Raw text submitted to language detection is {}", content.toString());
      //cld2Result.setEncoding_hint(22); // TODO : use encoding hints see https://github.com/CLD2Owners/cld2/blob/master/public/encodings.h
      languageDetectionInfo.cld2Language = result.code;
      if (result.language.equals("Unknown")) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Unable to guess language for {}", uri);
        }
      } else {
        Locale localeFromCLD = Locale.forLanguageTag(result.code);
        if (localeFromCLD != null && !localeFromCLD.getLanguage().equals(""))
          guessedLanguage = localeFromCLD;
        if (LOGGER.isDebugEnabled())
          LOGGER.debug("Guessed language {} for {}", result.language, uri);
      }
    }
  }

  private void updateDigestForRedirection( final HttpResponse httpResponse ) {
    // This is to avoid collapsing 3xx pages with boilerplate content (as opposed to 0-length content).
    if (digestAppendable != null && httpResponse.getStatusLine().getStatusCode() / 100 == 3) {
      digestAppendable.append((char) 0);
      if (location != null) digestAppendable.append(location.toString());
      digestAppendable.append((char) 0);
      if (metaLocation != null) digestAppendable.append(metaLocation.toString());
      digestAppendable.append((char) 0);
    }
  }

  @Override
  public byte[] parse(final URI uri, final HttpResponse httpResponse, final MsgCrawler.FetchInfo.Builder fetchInfoBuilder) throws IOException {
    guessedCharset = null;
    boolean charsetValid = false;
    guessedLanguage = null;
    charsetDetectionInfo.icuCharset =
        charsetDetectionInfo.httpHeaderCharset =
            charsetDetectionInfo.htmlMetaCharset = "-";
    languageDetectionInfo.cld2Language =
        languageDetectionInfo.htmlLanguage =
            languageDetectionInfo.httpHeaderLanguage = "-";
    if ( rewritten != null )
      rewritten.setLength(0);
    captureTextOfInterest = false;
    currentTextOfInterest.setLength(0);

    final HttpEntity entity = httpResponse.getEntity();
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) entity.getContent();

    tryGuessCharsetFromHeader( uri, httpResponse );
    tryGuessLanguageFromHeader( uri, httpResponse );

		/* Note that the bubing-guessed-charset header and the header guessed by inspecting
			the entity content are complementary. The first is supposed to appear when parsing
			a store, the second while crawling. They should be aligned. This is a bit tricky,
			but we want to avoid the dependency on "rewindable" streams while parsing. */

    final Header bubingGuessedCharsetHeader = httpResponse instanceof WarcRecord ? ((WarcRecord) httpResponse).getWarcHeader(WarcHeader.Name.BUBING_GUESSED_CHARSET) : null;

    List<ByteArrayCharSequence> allMetaEntries = getAllMetaEntries( contentStream.buffer, contentStream.inspectable );

    tryGetViewportFromMetas( uri, allMetaEntries );

    if (bubingGuessedCharsetHeader != null)
      guessedCharset = Charset.forName(bubingGuessedCharsetHeader.getValue());
    else
      charsetValid = tryGuessCharsetFromMetas( uri, allMetaEntries );

    tryGuessHtmlVersion( uri, contentStream.buffer, contentStream.inspectable );
    tryGuessLanguageFromHtml( uri, contentStream.buffer, contentStream.inspectable );
    if ( guessedCharset == null || !charsetValid )
      tryGuessCharsetFromICU( uri, contentStream.buffer, contentStream.inspectable );

    if (LOGGER.isDebugEnabled())
      LOGGER.debug("Guessing charset \"{}\" for URL {}", guessedCharset, uri);
    if ( guessedCharset == null )
      guessedCharset = Charsets.UTF_8; // Fallback in case of exception

    //MsgFrontier.CrawlRequest origin = makePageInfoFromURI(uri).buildPartial();
    fetchInfoBuilder.setUrlKey( PulsarHelper.fromURI(uri) );
    if (textProcessor != null) textProcessor.init(uri);

    // Get location if present
    location = null;
    metaLocation = null;

    final Header locationHeader = httpResponse.getFirstHeader(HttpHeaders.LOCATION);
    if (locationHeader != null) {
      final URI location = BURL.parse(locationHeader.getValue());
      if (location != null) {
        // This shouldn't happen by standard, but people unfortunately does it.
        if (!location.isAbsolute() && LOGGER.isDebugEnabled())
          LOGGER.debug("Found relative header location URL: \"{}\"", location);

        this.location = uri.resolve(location);
        final MsgCrawler.FetchLinkInfo.Builder linkInfo = makeLinkInfoFromBasicURI(this.location);
        linkInfo.getLinkInfoBuilder()
          .setLinkType( EnumType.Enum.REDIRECT );
        fetchInfoBuilder.addExternalLinks( linkInfo );
      }
    }

    if (digestAppendable != null)
      digestAppendable.init(crossAuthorityDuplicates ? null : uri);

    final URI base = doParse( uri, contentStream, guessedCharset, fetchInfoBuilder );

    // Find language in rewritten
    tryGuessLanguageFromTextContent( uri );

    updateDigestForRedirection( httpResponse );

    LOGGER.info("Finished parsing {}, outlinks : {}/{} ", base, fetchInfoBuilder.getExternalLinksCount(), fetchInfoBuilder.getInternalLinksCount());
    return digestAppendable != null ? digestAppendable.digest() : null;
  }

  @Override
  public Charset guessedCharset() {
    return guessedCharset;
  }

  @Override
  public Locale guessedLanguage() {
    return guessedLanguage;
  }

  @Override
  public CharsetDetectionInfo getCharsetDetectionInfo() {
    if (charsetDetectionInfo.icuCharset == null)
      charsetDetectionInfo.icuCharset = "-";
    if (charsetDetectionInfo.htmlMetaCharset == null)
      charsetDetectionInfo.htmlMetaCharset = "-";
    if (charsetDetectionInfo.httpHeaderCharset == null)
      charsetDetectionInfo.httpHeaderCharset = "-";
    return charsetDetectionInfo;
  }

  @Override
  public LanguageDetectionInfo getLanguageDetectionInfo() {
    if (languageDetectionInfo.cld2Language == null)
      languageDetectionInfo.cld2Language = "-";
    if (languageDetectionInfo.htmlLanguage == null)
      languageDetectionInfo.htmlLanguage = "-";
    if (languageDetectionInfo.httpHeaderLanguage == null)
      languageDetectionInfo.httpHeaderLanguage = "-";
    return languageDetectionInfo;
  }

  @Override
  public StringBuilder getRewrittenContent() {
    return rewritten;
  }

  @Override
  public StringBuilder getTextContent() {
    return textContent.getContent();
  }

  @Override
  public String getTitle() {
    return title;
  }


  @Override
  public Boolean responsiveDesign() {
    return foundViewPortMeta;
  }

  @Override
  public Boolean html5() {
    return htmlVersionAtLeast5;
  }

  /**
   * Returns the BURL location header, if present; if it is not present, but the page contains a valid metalocation, the latter
   * is returned. Otherwise, {@code null} is returned.
   *
   * @return the location (or metalocation), if present; {@code null} otherwise.
   */
  public URI location() {
    //TODO: see if we must derelativize
    if (location != null) return location;
    else if (metaLocation != null) return metaLocation;
    else return null;
  }

  public static boolean checkCharset(byte[] b, int blen, Charset charset) {
    CharsetDecoder cd =
        charset.newDecoder();
    try {
      cd.decode(ByteBuffer.wrap(b, 0, blen));
    } catch (CharacterCodingException e) {
      return false;
    }
    return true;
  }

  protected static final TextPattern DOCTYPE_PATTERN = new TextPattern("<!doctype", TextPattern.CASE_INSENSITIVE);
  /**
   * Used by {@link #getCharsetName(byte[], int)}.
   */
  protected static final TextPattern META_PATTERN = new TextPattern("<meta", TextPattern.CASE_INSENSITIVE);
  /**
   * Used by {@link #getLanguageName(byte[], int)}.
   */
  protected static final TextPattern HTML_PATTERN = new TextPattern("<html", TextPattern.CASE_INSENSITIVE);

  /**
   * Used by {@link #getCharsetName(byte[], int)}.
   */
  public static final Pattern HTTP_EQUIV_PATTERN = Pattern.compile(".*http-equiv\\s*=\\s*('|\")?content-type('|\")?.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  /**
   * Used by {@link #getCharsetName(byte[], int)}.
   */
  public static final Pattern CONTENT_PATTERN = Pattern.compile(".*content\\s*=\\s*('|\")([^'\"]*)('|\").*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  /**
   * Used by {@link #getCharsetName(byte[], int)}.
   */
  public static final Pattern CHARSET_PATTERN = Pattern.compile(".*charset\\s*=\\s*\"?([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=\"]]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  protected static final Pattern LANG_PATTERN = Pattern.compile(".*lang\\s*=\\s*\"?([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=\"]]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static final Pattern VIEWPORT_PATTERN = Pattern.compile(".*name\\s*=\\s*('|\")?viewport('|\")?.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * Returns the charset name as indicated by a <code>META</code>
   * <code>HTTP-EQUIV</code> element, if
   * present, interpreting the provided byte array as a sequence of
   * ISO-8859-1-encoded characters. Only the first such occurrence is considered (even if
   * it might not correspond to a valid or available charset).
   * <p>
   * <p><strong>Beware</strong>: it might not work if the
   * <em>value</em> of some attribute in a <code>meta</code> tag
   * contains a string matching (case insensitively) the r.e.
   * <code>http-equiv\s*=\s*('|")content-type('|")</code>, or
   * <code>content\s*=\s*('|")[^"']*('|")</code>.
   *
   * @param buffer a buffer containing raw bytes that will be interpreted as ISO-8859-1 characters.
   * @param length the number of significant bytes in the buffer.
   * @return the charset name, or {@code null} if no
   * charset is specified; note that the charset might be not valid or not available.
   */
  public static String getCharsetName(final byte buffer[], final int length) {
    int start = 0;
    while ((start = META_PATTERN.search(buffer, start, length)) != -1) {

      /* Look for attribute http-equiv with value content-type,
       * if present, look for attribute content and, if present,
       * return its value. */

      int end = start;
      while (end < length && buffer[end] != '>') end++; // Look for closing '>'
      if (end == length) return null; // No closing '>'

      final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(buffer, start + META_PATTERN.length(), end - start - META_PATTERN.length());
      if (HTTP_EQUIV_PATTERN.matcher(tagContent).matches()) {
        final Matcher m = CONTENT_PATTERN.matcher(tagContent);
        if (m.matches())
          return getCharsetNameFromHeader(m.group(2)); // got it!
      }

      final Matcher mCharset = CHARSET_PATTERN.matcher(tagContent);
      if (mCharset.matches())
        return getCharsetNameFromHeader(mCharset.group(0)); // got it!
      start = end + 1;
    }

    return null; // no '<meta' found
  }

  public static List<ByteArrayCharSequence> getAllMetaEntries(final byte buffer[], final int length) {
    ArrayList<ByteArrayCharSequence> metas = new ArrayList<>();
    int start = 0;
    while ((start = META_PATTERN.search(buffer, start, length)) != -1) {
      int end = start;
      while (end < length && buffer[end] != '>') end++; // Look for closing '>'
      if (end == length) return metas; // No closing '>'
      metas.add(new ByteArrayCharSequence(buffer, start + META_PATTERN.length(), end - start - META_PATTERN.length()));
      start = end + 1;
    }
    return metas;
  }

  /**
   * Returns the charset name as indicated by a <code>META</code>
   * <code>HTTP-EQUIV</code> element, if
   * present, interpreting the provided byte array as a sequence of
   * ISO-8859-1-encoded characters. Only the first such occurrence is considered (even if
   * it might not correspond to a valid or available charset).
   * <p>
   * <p><strong>Beware</strong>: it might not work if the
   * <em>value</em> of some attribute in a <code>meta</code> tag
   * contains a string matching (case insensitively) the r.e.
   * <code>http-equiv\s*=\s*('|")content-type('|")</code>, or
   * <code>content\s*=\s*('|")[^"']*('|")</code>.
   *
   * @param allMetaEntries a list of all meta entries found in the buffer.
   * @return the charset name, or {@code null} if no
   * charset is specified; note that the charset might be not valid or not available.
   */
  public static String getCharsetName(final List<ByteArrayCharSequence> allMetaEntries) {
    for (ByteArrayCharSequence meta : allMetaEntries) {
      if (HTTP_EQUIV_PATTERN.matcher(meta).matches()) {
        final Matcher m = CONTENT_PATTERN.matcher(meta);
        if (m.matches())
          return getCharsetNameFromHeader(m.group(2)); // got it!
      }

      final Matcher mCharset = CHARSET_PATTERN.matcher(meta);
      if (mCharset.matches())
        return getCharsetNameFromHeader(mCharset.group(0)); // got it!
    }
    return null; // no '<meta' found
  }

  /**
   * Returns a boolean which indicate if a viewport meta was found among meta entries of
   * the document.
   *
   * @param allMetaEntries a list of all meta entries found in the buffer.
   * @return true if the viewport meta was found and false otherwise
   */
  public static boolean getViewport(final List<ByteArrayCharSequence> allMetaEntries) {
    for (ByteArrayCharSequence meta : allMetaEntries)
      if (VIEWPORT_PATTERN.matcher(meta).matches())
        return true;
    return false;
  }

  public static String getLanguageName( final byte buffer[], final int length ) {
    int start = HTML_PATTERN.search( buffer, 0, length );
    if ( start == -1 ) return null;
    int end = start;
    while (end < length && buffer[end] != '>') end++; // Look for closing '>'
    final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(buffer, start + HTML_PATTERN.length(), end - start - HTML_PATTERN.length());
    return getLanguageNameFromHTML( tagContent );
  }

  /**
   * Returns document doctype declaration if found.
   *
   * @param buffer a buffer containing raw bytes that will be interpreted as ISO-8859-1 characters.
   * @param length the number of significant bytes in the buffer.
   * @return the doctype declaration if found, null otherwise
   */
  public static String getDocType(final byte buffer[], final int length) {
    int start = DOCTYPE_PATTERN.search( buffer, 0, length );
    if ( start == -1 ) return null;
    int end = start;
    while (end < length && buffer[end] != '>') end++; // Look for closing '>'
    final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(buffer, start + DOCTYPE_PATTERN.length(), end - start - DOCTYPE_PATTERN.length());
    return tagContent.toString().trim();
  }

  /**
   * Extracts the charset name from the header value of a <code>content-type</code>
   * header using a regular expression.
   * <p>
   * <strong>Warning</strong>: it might not work if someone puts the string <code>charset=</code>
   * in a string inside some attribute/value pair.
   *
   * @param headerValue The value of a <code>content-type</code> header.
   * @return the charset name, or {@code null} if no
   * charset is specified; note that the charset might be not valid or not available.
   */
  public static String getCharsetNameFromHeader(final CharSequence headerValue) {
    final Matcher m = CHARSET_PATTERN.matcher(headerValue);
    if (m.matches()) {
      final String s = m.group(1);
      int start = 0, end = s.length();
      // TODO: we discard delimiting single/double quotes; is it necessary?
      if (end > 0 && (s.charAt(0) == '\"' || s.charAt(0) == '\'')) start = 1;
      if (end > 0 && (s.charAt(end - 1) == '\"' || s.charAt(end - 1) == '\'')) end--;
      if (start < end) return s.substring(start, end);
    }
    return null;
  }

  public static String getLanguageNameFromHTML(final CharSequence headerValue ) {
    final Matcher m = LANG_PATTERN.matcher(headerValue);
    if (m.matches()) {
      final String s = m.group(1);
      int start = 0, end = s.length();
      // TODO: we discard delimiting single/double quotes; is it necessary?
      if (end > 0 && (s.charAt(0) == '\"' || s.charAt(0) == '\'')) start = 1;
      if (end > 0 && (s.charAt(end - 1) == '\"' || s.charAt(end - 1) == '\'')) end--;
      if (start < end) return s.substring(start, end);
    }
    return null;
  }

  @Override
  public boolean apply(final URIResponse uriResponse) {
    final Header contentType = uriResponse.response().getEntity().getContentType();
    return contentType != null && contentType.getValue().startsWith("text/");
  }

  @Override
  public HTMLParser<T> clone() {
    return new HTMLParser<>(digestAppendable == null ? null : digestAppendable.hashFunction, textProcessor == null ? null : textProcessor.copy(), crossAuthorityDuplicates, buffer.length);
  }

  @Override
  public HTMLParser<T> copy() {
    return clone();
  }

  @Override
  public T result() {
    return textProcessor == null ? null : textProcessor.result();
  }

  public static void main(String arg[]) throws IllegalArgumentException, IOException, URISyntaxException, JSAPException, NoSuchAlgorithmException {

    final SimpleJSAP jsap = new SimpleJSAP(HTMLParser.class.getName(), "Produce the digest of a page: the page is downloaded or passed as argument by specifying a file",
        new Parameter[]{
            new UnflaggedOption("url", JSAP.STRING_PARSER, JSAP.REQUIRED, "The url of the page."),
            new Switch("crossAuthorityDuplicates", 'c', "cross-authority-duplicates"),
            new FlaggedOption("charBufferSize", JSAP.INTSIZE_PARSER, Integer.toString(CHAR_BUFFER_SIZE), JSAP.NOT_REQUIRED, 'b', "buffer", "The size of the parser character buffer (0 for dynamic sizing)."),
            new FlaggedOption("file", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "file", "The page to be processed."),
            new FlaggedOption("digester", JSAP.STRING_PARSER, "MD5", JSAP.NOT_REQUIRED, 'd', "digester", "The digester to be used.")
        });

    final JSAPResult jsapResult = jsap.parse(arg);
    if (jsap.messagePrinted()) System.exit(1);

    final String url = jsapResult.getString("url");
    final String digester = jsapResult.getString("digester");
    final boolean crossAuthorityDuplicates = jsapResult.userSpecified("crossAuthorityDuplicates");
    final int charBufferSize = jsapResult.getInt("charBufferSize");

    final HTMLParser<Void> htmlParser = new HTMLParser<>(BinaryParser.forName(digester), (TextProcessor<Void>) null, crossAuthorityDuplicates, charBufferSize);
    final MsgCrawler.FetchInfo.Builder crawledPageInfoBuilder = MsgCrawler.FetchInfo.newBuilder();
    final byte[] digest;

    if (!jsapResult.userSpecified("file")) {
      final URI uri = new URI(url);
      final HttpGet request = new HttpGet(uri);
      request.setConfig(RequestConfig.custom().setRedirectsEnabled(false).build());
      digest = htmlParser.parse(uri, HttpClients.createDefault().execute(request), crawledPageInfoBuilder);
    } else {
      final String file = jsapResult.getString("file");
      final String content = IOUtils.toString(new InputStreamReader(new FileInputStream(file)));
      digest = htmlParser.parse(BURL.parse(url), new StringHttpMessages.HttpResponse(content), crawledPageInfoBuilder);
    }

    System.out.println("DigestHexString: " + Hex.encodeHexString(digest));
    System.out.println("Links: " + crawledPageInfoBuilder.getExternalLinksList());

    final Set<String> urlStrings = new ObjectOpenHashSet<>();
    for (final MsgCrawler.FetchLinkInfo link : crawledPageInfoBuilder.getExternalLinksList())
      urlStrings.add( PulsarHelper.toString(link.getTarget()) );
    if (urlStrings.size() != crawledPageInfoBuilder.getExternalLinksList().size())
      System.out.println("There are " + crawledPageInfoBuilder.getExternalLinksList().size() + " URIs but " + urlStrings.size() + " strings");

  }

}
