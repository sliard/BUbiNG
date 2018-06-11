package it.unimi.di.law.bubing.parser;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import com.martiansoftware.jsap.*;
import it.unimi.di.law.bubing.Agent;
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
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
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

/** An HTML parser with additional responsibilities.
 * An instance of this class does some buffering that makes it possible to
 * parse quickly a {@link HttpResponse}. Instances are heavyweight&mdash;they
 * should be pooled and shared, since their usage is transitory and CPU-intensive.
 */
public class HTMLParser<T> implements Parser<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(HTMLParser.class);

	static {
		/* As suggested by Martin Jericho. This should speed up things and avoid problems with
		 * server tags embedded in weird places (e.g., JavaScript string literals). Server tags
		 * should not appear in generated HTML anyway. */

		StartTagType.SERVER_COMMON.deregister();
		StartTagType.SERVER_COMMON_COMMENT.deregister();
		StartTagType.SERVER_COMMON_ESCAPED.deregister();
	}

    /** An implementation of a {@link Parser.LinkReceiver} that accumulates the URLs in a public set. */
	public final static class SetLinkReceiver implements LinkReceiver {
		/** The set of URLs gathered so far. */
		public final Set<URI> urls = new ObjectLinkedOpenHashSet<>();

		@Override
		public void location(URI location) {
			urls.add(location);
		}

		@Override
		public void metaLocation(URI location) {
			urls.add(location);
		}

		@Override
		public void metaRefresh(URI refresh) {
			urls.add(refresh);
		}

		@Override
		public void link(URI link) {
			urls.add(link);
		}

		@Override
		public void init(URI responseUrl) {
			urls.clear();
		}

		@Override
		public Iterator<URI> iterator() {
			return urls.iterator();
		}

		@Override
		public int size() {
			return urls.size();
		}
	}

	/** A class computing the digest of a page.
	 *
	 * <p>The page is somewhat simplified before being passed (as a sequence of bytes obtained
	 * by breaking each character into the upper and lower byte) to a {@link Hasher}.
	 * All start/end tags are case-normalized, and their whole content (except for the
	 * element-type name) is removed.
	 * An exception is made for <code>SRC</code> attribute of
	 * <code>FRAME</code> and <code>IFRAME</code> elements, as they are necessary to
	 * distinguish correctly framed pages without alternative text. The attributes will be resolved
	 * w.r.t. the {@linkplain #init(URI) URL associated to the page}.
	 * Moreover, non-HTML tags are substituted with a special tag <code>unknown</code>.
	 *
	 * <p>For what concerns the text, all digits are substituted by a whitespace, and nonempty whitespace maximal sequences are coalesced
	 * to a single space. Tags are considered as a non-whitespace character.
	 *
	 * <p>To avoid clashes between digests coming from different sites, you can optionally set a URL
	 * (passed to the {@link #init(URI)} method) whose scheme+authority will be used to update the digest before adding the actual text page.
	 *
	 * <p>Additionally, since BUbiNG 0.9.10 location redirect URLs (both from headers and from META elements),
	 * if present, are mixed in to avoid collapsing 3xx pages with boilerplate text.
	 */
	public final static class DirectDigestAppendable implements Appendable {

		/** Cached byte representations of all opening tags. The map must be queried using {@linkplain HTMLElementName Jericho names}. */
		protected static final Reference2ObjectOpenHashMap<String, byte[]> startTags;

		/** Cached byte representations of all closing tags. The map must be queried using {@linkplain HTMLElementName Jericho names}. */
		protected static final Reference2ObjectOpenHashMap<String, byte[]> endTags;

		static {
			final List<String> elementNames = HTMLElements.getElementNames();
			startTags = new Reference2ObjectOpenHashMap<>(elementNames.size());
			endTags = new Reference2ObjectOpenHashMap<>(elementNames.size());

			// Set up defaults for bizarre element types
			startTags.defaultReturnValue(Util.toByteArray("<unknown>"));
			endTags.defaultReturnValue(Util.toByteArray("</unknown>"));

			// Scan all known element types and fill startTag/endTag
			for (final String name : elementNames) {
				startTags.put(name, Util.toByteArray("<" + name + ">"));
				endTags.put(name, Util.toByteArray("</" + name + ">"));
			}
		}

		protected final HashFunction hashFunction;

		/** True iff the last character appended was a space. */
		protected boolean lastAppendedWasSpace;
		/** The last returne digest, or {@code null} if {@link #init(URI)} has been called but {@link #digest()} hasn't. */
		protected byte[] digest;
		protected MurmurHash3_128.LongPair tempDigest = null;
		protected static final int DIGEST_BUFFER_SIZE = 64*1024;
		protected char[] buffer = null;
		protected int bufferLen = 0;
		protected int totalLen = 0;
		/** Create a digest appendable using a given hash function.
		 *
		 * @param hashFunction the hash function used to digest. */
		public DirectDigestAppendable(final HashFunction hashFunction) {
			this.hashFunction = hashFunction;
			buffer = new char[DIGEST_BUFFER_SIZE];
			tempDigest = new MurmurHash3_128.LongPair();
		}

		/** Initializes the digest computation.
		 *
		 * @param url a URL, or {@code null} for no URL. In the former case, the host name will be used to initialize the digest.
		 */
		public void init(final URI url) {
			bufferLen = 0;
			totalLen = 0;
			tempDigest = new MurmurHash3_128.LongPair();
			MurmurHash3_128.murmurhash3_x64_128_init(128945, tempDigest);
			digest = null;

			if (url != null) {
				// Note that we need to go directly to the hasher to encode explicit IP addresses
				String host = url.getHost();
				for (int i = 0; i< host.length(); i++)
					appendChar(host.charAt(i));
				appendChar(' ');
			}
			lastAppendedWasSpace = false;
		}

		private void appendChar(char c) {
			buffer[bufferLen] = c;
			bufferLen++;
			totalLen++;
			if (bufferLen == DIGEST_BUFFER_SIZE) {
				MurmurHash3_128.murmurhash3_x64_128_update(buffer, 0, DIGEST_BUFFER_SIZE, tempDigest);
				bufferLen = 0;
			}
		}

		@Override
		public Appendable append(CharSequence csq, int start, int end) {
			// Hopefully this will soon be inlined by the jvm: no need to duplicate the code! :-)
			for (int i = start; i < end; i++) append(csq.charAt(i));
			return this;
		}

		@Override
		public Appendable append(char c) {
			if (Character.isWhitespace(c) || Character.isDigit(c)) {
				if (!lastAppendedWasSpace) {
					appendChar(' ');
					lastAppendedWasSpace = true;
				}
			} else {
				appendChar(c);
				lastAppendedWasSpace = false;
			}
			return this;
		}

		@Override
		public Appendable append(CharSequence csq) {
			char [] chars = csq.toString().toCharArray();
			for (char c : chars)
				append(c);
			return this;
		}

		private void append(byte[] a) {
			for (byte b:a) appendChar((char)b);
		}

		private void append(char[] a, int offset, int length) {
			for (int i = offset; i < offset + length; i++)
				appendChar(a[i]);
		}

		public byte[] digest() {
			if (digest == null) {
				if (bufferLen > 0) {
					MurmurHash3_128.murmurhash3_x64_128_update(buffer, 0, bufferLen, tempDigest);
					bufferLen = 0;
				}
				MurmurHash3_128.murmurhash3_x64_128_finalize(totalLen, tempDigest);
				digest = tempDigest.asBytes();

			}
			return digest;
		}

		public void startTag(final StartTag startTag) {
			final String name = startTag.getName();
			append(startTags.get(name));

			// IFRAME or FRAME + SRC
			if (name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME) {
				final String s = startTag.getAttributeValue("src");
				if (s != null) {
					appendChar('\"');
					append(s);
					appendChar('\"');
				}
			}
			lastAppendedWasSpace = false;
		}

		public void endTag(final EndTag endTag) {
			append(endTags.get(endTag.getName()));
			lastAppendedWasSpace = false;
		}
	}
	public final static class PureTextAppendable implements Appendable, CharSequence {
		/** True iff the last character appended was a space. */
		protected boolean lastAppendedWasSpace;
		protected boolean lastAppendedWasNewLine;

		protected final StringBuilder textContent;

		public PureTextAppendable() {
			textContent = new StringBuilder();
			lastAppendedWasSpace = true;
			lastAppendedWasNewLine = true;
		}

		/** Initializes the digest computation.
		 */
		public void init() {
			textContent.setLength(0);
			lastAppendedWasSpace = true;
			lastAppendedWasNewLine = true;
		}

		@Override
		public Appendable append(CharSequence csq, int start, int end) {
			// Hopefully this will soon be inlined by the jvm: no need to duplicate the code! :-)
			for (int i = start; i < end; i++) append(csq.charAt(i));
			return this;
		}

		@Override
		public Appendable append(char c) {
			if (Character.isWhitespace(c)) {
				int charType = Character.getType(c);
				if (charType == Character.SPACE_SEPARATOR) {
					if (!lastAppendedWasSpace) {
						textContent.append(' ');
						lastAppendedWasSpace = true;
					}
				}
				if (charType == Character.LINE_SEPARATOR || charType == Character.PARAGRAPH_SEPARATOR || charType == Character.CONTROL) {
					if (!lastAppendedWasNewLine) {
						textContent.append('\n');
						lastAppendedWasSpace = true;
						lastAppendedWasNewLine = true;
					}
				}
			} else {
				textContent.append(c);
				lastAppendedWasSpace = false;
				lastAppendedWasNewLine = false;
			}
			return this;
		}

		@Override
		public Appendable append(CharSequence csq) {
			char [] chars = csq.toString().toCharArray();
			for (char c : chars)
				append(c);
			return this;
		}


		@Override
		public int length() {
			return textContent.length();
		}

		@Override
		public char charAt(int index) {
			return textContent.charAt(index);
		}

		@Override
		public CharSequence subSequence(int start, int end) {
			return textContent.subSequence(start, end);
		}

		@Override
		public String toString() {
			return textContent.toString();
		}
	}

	/** The pattern prefixing the URL in a <code>META </code> <code>HTTP-EQUIV </code> element of refresh type. */
	protected static final TextPattern URLEQUAL_PATTERN = new TextPattern("URL=", TextPattern.CASE_INSENSITIVE);
	/** The size of the internal Jericho buffer. */
	public static final int CHAR_BUFFER_SIZE = 128 * 1024;
	/** The max required amount of page content (without HTML entities) for charset detection */
	protected static final int MAX_CHARSET_PAGE_CONTENT = 5000;
	/** The max required amount of text page content (with HTML entities) for language detection */
	protected static final int MAX_LANGUAGE_PAGE_CONTENT = 5000;
	/** The character buffer. It is set up at construction time, but it can be changed later. */
	protected final char[] buffer;
	/** The charset we guessed for the last response. */
	protected Charset guessedCharset;
	/** The charset we guessed for the last response. */
	protected Locale guessedLanguage;

	protected CharsetDetectionInfo charsetDetectionInfo;
	protected LanguageDetectionInfo languageDetectionInfo;

	/** The charset we guessed for the last response. */
	protected Charset finalGuessedCharset;
	/** An object emboding the digest logic, or {@code null} for no digest computation. */
	protected final DirectDigestAppendable digestAppendable;
	/** A text processor, or {@code null}. */
	protected final TextProcessor<T> textProcessor;
	/** The location URL from headers of the last response, if any, or {@code null}. */
	protected URI location;
	/** The location URL from <code>META</code> elements of the last response, if any, or {@code null}. */
	protected URI metaLocation;
	/** If <code>true</code>, pages with the same content but with different authorities are considered duplicates. */
	protected boolean crossAuthorityDuplicates;

	/** A charset detector */
	private final CharsetDetector charsetDetector;

	/** A buffer used for storing raw non-html data and detectCharset */
	private final byte[] charsetDetectionBuffer;

	/** The rewritten version of the page */
	protected StringBuilder rewritten;
	protected PureTextAppendable textContent;

	private static final HashSet<String> ENDLINE_SET = Sets.newHashSet(ARTICLE, ASIDE, FOOTER, DETAILS, SECTION, HEADER, HGROUP, NAV, P, H1, H2, H3, H4, H5, H6, UL, OL, DIR, MENU, PRE, DL, DIV, CENTER, NOSCRIPT, NOFRAMES, BLOCKQUOTE, FORM, ISINDEX, HR, TABLE, FIELDSET, ADDRESS, LI, DT, DD, TR, CAPTION, LEGEND, BR);
	private static final HashSet<String> INDENT_SET = Sets.newHashSet( LI, DD );
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
	 * @param hashFunction the hash function used to digest, {@code null} if no digesting will be performed.
	 * @param textProcessor a text processor, or {@code null} if no text processing is required.
	 * @param crossAuthorityDuplicates if <code>true</code>, pages with different scheme+authority but with the same content will be considered to be duplicates, as long
	 * as they are assigned to the same {@link Agent}.
	 * @param bufferSize the fixed size of the internal buffer; if zero, the buffer will be dynamic.
	 */
	public HTMLParser(final HashFunction hashFunction, final TextProcessor<T> textProcessor, final boolean crossAuthorityDuplicates, final int bufferSize) {
		buffer = bufferSize != 0 ? new char[bufferSize] : null;
		digestAppendable = hashFunction == null ? null : new DirectDigestAppendable(hashFunction);
		this.textProcessor = textProcessor;
		this.crossAuthorityDuplicates = crossAuthorityDuplicates;
		charsetDetectionBuffer = new byte[MAX_CHARSET_PAGE_CONTENT];
		this.charsetDetector = new CharsetDetector();
		charsetDetectionInfo = new CharsetDetectionInfo();
		languageDetectionInfo = new LanguageDetectionInfo();
		rewritten = new StringBuilder();
		textContent = new PureTextAppendable();
	}

	/**
	 * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
	 *
	 * @param hashFunction the hash function used to digest, {@code null} if no digesting will be performed.
	 * @param crossAuthorityDuplicates if <code>true</code>, pages with different scheme+authority but with the same content will be considered to be duplicates, as long
	 * as they are assigned to the same {@link Agent}.
	 */
	public HTMLParser(final HashFunction hashFunction, final boolean crossAuthorityDuplicates) {
		this(hashFunction, null, crossAuthorityDuplicates, CHAR_BUFFER_SIZE);
	}

	/**
	 * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
	 *
	 * @param hashFunction the hash function used to digest, {@code null} if no digesting will be performed.
	 * @param textProcessor a text processor, or {@code null} if no text processing is required.
	 * @param crossAuthorityDuplicates if <code>true</code>, pages with different scheme+authority but with the same content will be considered to be duplicates, as long
	 * as they are assigned to the same {@link Agent}.
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
	 * @param messageDigest the name of a message-digest algorithm, or the empty string if no digest will be computed.
	 * @param crossAuthorityDuplicates a string whose value can only be "true" or "false" that is used to determine if you want to check for cross-authority duplicates.
	 * @throws NoSuchAlgorithmException
	 */
	public HTMLParser(final String messageDigest, final String crossAuthorityDuplicates) throws NoSuchAlgorithmException {
		this(BinaryParser.forName(messageDigest), Util.parseBoolean(crossAuthorityDuplicates));
	}

	/**
	 * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction and, possibly, digesting a page.
	 *
	 * @param messageDigest the name of a message-digest algorithm, or the empty string if no digest will be computed.
	 * @param textProcessorSpec the specification of a text processor that will be passed to an {@link ObjectParser}.
	 * @param crossAuthorityDuplicates a string whose value can only be "true" or "false" that is used to determine if you want to check for cross-authority duplicates.
	 * @throws NoSuchAlgorithmException
	 */
	@SuppressWarnings("unchecked")
	public HTMLParser(final String messageDigest, final String textProcessorSpec, final String crossAuthorityDuplicates) throws NoSuchAlgorithmException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, IOException {
		this(BinaryParser.forName(messageDigest), (TextProcessor<T>)ObjectParser.fromSpec(textProcessorSpec), Util.parseBoolean(crossAuthorityDuplicates));
	}

	/**
	 * Builds a parser with a fixed buffer of {@link #CHAR_BUFFER_SIZE} characters for link extraction only (no digesting).
	 */
	public HTMLParser() {
		this(null, null, false, CHAR_BUFFER_SIZE);
	}

	/** Pre-process a string that represents a raw link found in the page, trying to derelativize it. If it succeeds, the
	 *  resulting URL is passed to the link receiver.
	 *
	 * @param linkReceiver the link receiver that will receive the resulting URL.
	 * @param base the base URL to be used to derelativize the link.
	 * @param s the raw link to be derelativized.
	 */
	protected void process(final LinkReceiver linkReceiver, final URI base, final String s) {
		if (s == null) return;
		final URI url = BURL.parse(s);
		if (url == null) return;
		linkReceiver.link(base.resolve(url));
	}

	@Override
	public byte[] parse(final URI uri, final HttpResponse httpResponse, final LinkReceiver linkReceiver) throws IOException {
		guessedCharset = null;
		boolean charsetValid = false;
		guessedLanguage = null;
		charsetDetectionInfo.icuCharset = charsetDetectionInfo.httpHeaderCharset = charsetDetectionInfo.htmlMetaCharset = "-";
		languageDetectionInfo.cld2Language = languageDetectionInfo.htmlLanguage = languageDetectionInfo.httpHeaderLanguage = "-";
		rewritten.setLength(0);
		textContent.init();

		final HttpEntity entity = httpResponse.getEntity();
		final InputStream contentStream = entity.getContent();
		byte[] inspectableEntityContent = ((InspectableFileCachedInputStream)contentStream).buffer;
		int inspectableEntityContentLength = ((InspectableFileCachedInputStream)contentStream).inspectable;

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


		/* Note that the bubing-guessed-charset header and the header guessed by inspecting
			the entity content are complementary. The first is supposed to appear when parsing
			a store, the second while crawling. They should be aligned. This is a bit tricky,
			but we want to avoid the dependency on "rewindable" streams while parsing. */

		final Header bubingGuessedCharsetHeader = httpResponse instanceof WarcRecord ? ((WarcRecord)httpResponse).getWarcHeader(WarcHeader.Name.BUBING_GUESSED_CHARSET) : null;

		if (bubingGuessedCharsetHeader != null) guessedCharset = Charset.forName(bubingGuessedCharsetHeader.getValue());
		else {

			charsetDetectionInfo.htmlMetaCharset = getCharsetName(inspectableEntityContent, inspectableEntityContentLength);
			if (charsetDetectionInfo.htmlMetaCharset != null) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Found charset {} in META HTML of {}", charsetDetectionInfo.htmlMetaCharset, uri.toString());
				try {
					guessedCharset = Charset.forName(charsetDetectionInfo.htmlMetaCharset);
					charsetValid = true;
				} catch (Exception e) {
					LOGGER.debug("Charset {} found in HTML <meta> is not supported", charsetDetectionInfo.htmlMetaCharset);
				}
			}
		}

		if (contentStream instanceof InspectableFileCachedInputStream) {
			final InspectableFileCachedInputStream inspectableStream = (InspectableFileCachedInputStream)contentStream;
			languageDetectionInfo.htmlLanguage = getLanguageName(inspectableStream.buffer, inspectableStream.inspectable);
			if (languageDetectionInfo.htmlLanguage != null) {
				Locale fromHtmlLocal = Locale.forLanguageTag(languageDetectionInfo.htmlLanguage);
				if (!fromHtmlLocal.getLanguage().equals(""))
					guessedLanguage = fromHtmlLocal;
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Found language {} with code {} ({})in <html lang=?..> of {}", languageDetectionInfo.htmlLanguage, guessedLanguage.getLanguage(), guessedLanguage.getDisplayLanguage(), uri.toString());
			}
		}

		if (guessedCharset == null || !charsetValid) {
			InputStream beginningOfStream = new ByteArrayInputStream(inspectableEntityContent, 0, inspectableEntityContentLength);
			int lastSegmentEnd = 0;
			int inSpecialText = 0;
			int byteCounter = 0;

			@SuppressWarnings("resource")
			final StreamedSource streamedSource = new StreamedSource(new InputStreamReader(beginningOfStream,new NoOpDecoder()));
			if (buffer != null) streamedSource.setBuffer(buffer);
			for (final Segment segment : streamedSource) {
				if (byteCounter >= MAX_CHARSET_PAGE_CONTENT)
					break;
				if (segment.getEnd() > lastSegmentEnd) {
					lastSegmentEnd = segment.getEnd();
					if (segment instanceof StartTag) {
						final StartTag startTag = (StartTag)segment;
						final StartTagType startTagType = startTag.getStartTagType();
						if ( startTagType == StartTagType.COMMENT )
							continue;
						/*if ( startTagType == StartTagType.DOCTYPE_DECLARATION )
							docTypeDeclaration( startTag );*/
						final String name = startTag.getName();
						if ((name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT ) && !startTag.isSyntacticalEmptyElementTag()) inSpecialText++;
					}
					else if (segment instanceof EndTag) {
						final EndTag endTag = (EndTag)segment;
						final String name = endTag.getName();
						if (name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) {
							inSpecialText = Math.max(0, inSpecialText - 1); // Ignore extra closing tags
						}
					}
					else if ((inSpecialText == 0) && !(segment instanceof CharacterReference)) {
						java.nio.CharBuffer cb = streamedSource.getCurrentSegmentCharBuffer();

						if (inSpecialText == 0) {
							for( int i = cb.position(); i < cb.position()+cb.remaining() && (byteCounter < MAX_CHARSET_PAGE_CONTENT); i++,byteCounter++)
								charsetDetectionBuffer[byteCounter] = (byte) cb.get(i);
						}

					}
				}
			}

			if (byteCounter > 0) {
				charsetDetector.setText(new ByteArrayInputStream(charsetDetectionBuffer, 0, byteCounter));
				CharsetMatch match = charsetDetector.detect();
				if (match != null) {
					charsetDetectionInfo.icuCharset = match.getName();
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("Found charset {} with ICU {}", charsetDetectionInfo.icuCharset, uri.toString());
					try {
						guessedCharset = Charset.forName(charsetDetectionInfo.icuCharset);
					} catch (UnsupportedCharsetException e) {
						LOGGER.error("Charset {} found in header is not supported", charsetDetectionInfo.icuCharset);
					}
				}
			}
		}

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Guessing charset \"{}\" for URL {}", guessedCharset, uri);
		Charset charset = Charsets.UTF_8; // Fallback in case of exception
		if (guessedCharset != null)
		{
			charset=guessedCharset;
		}
		finalGuessedCharset = charset;

		linkReceiver.init(uri);
		if (textProcessor != null) textProcessor.init(uri);

		// Get location if present
		location = null;
		metaLocation = null;

		final Header locationHeader = httpResponse.getFirstHeader(HttpHeaders.LOCATION);
		if (locationHeader != null) {
			final URI location = BURL.parse(locationHeader.getValue());
			if (location != null) {
				// This shouldn't happen by standard, but people unfortunately does it.
				if (! location.isAbsolute() && LOGGER.isDebugEnabled()) LOGGER.debug("Found relative header location URL: \"{}\"", location);
				linkReceiver.location(this.location = uri.resolve(location));
			}
		}


		@SuppressWarnings("resource")
		final StreamedSource streamedSource = new StreamedSource(new InputStreamReader(contentStream, charset));
		if (buffer != null) streamedSource.setBuffer(buffer);
		if (digestAppendable != null) digestAppendable.init(crossAuthorityDuplicates? null : uri);
		URI base = uri;

		int lastSegmentEnd = 0;
		int inSpecialText = 0;
		int skipping = 0;

		for (final Segment segment : streamedSource) {
			boolean rewrite = true;
			if (segment.getEnd() > lastSegmentEnd) {
				lastSegmentEnd = segment.getEnd();
				if (segment instanceof StartTag) {
					final StartTag startTag = (StartTag)segment;
					final StartTagType startTagType = startTag.getStartTagType();

					if ( startTagType == StartTagType.COMMENT )
						continue;

					final String name = startTag.getName();


					if ( name == HTMLElementName.SPAN || name == HTMLElementName.FONT || name == HTMLElementName.STRONG ||
							name == HTMLElementName.I || name == HTMLElementName.B || name == HTMLElementName.EM )
						rewrite = false; // INLINE_NO_WHITESPACE
					else
					if ( name == HTMLElementName.SCRIPT || name == HTMLElementName.OPTION || name == HTMLElementName.STYLE ) {
						textContent.append(" ");
						if ( !startTag.isSyntacticalEmptyElementTag() ) // FIXME: may be isEmptyElementTag(), copy/paste from BUbiNG source code
							skipping += 1;
						rewrite = false; // IGNORABLE_ELEMENT
					} else
						if (ENDLINE_SET.contains(name))
							textContent.append("\n");
						else
							textContent.append(" ");

					if ((name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) && !startTag.isSyntacticalEmptyElementTag()) inSpecialText++;

					if (digestAppendable != null) digestAppendable.startTag(startTag);
					// TODO: detect flow breakers

					// IFRAME or FRAME + SRC

					if (name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME || name == HTMLElementName.EMBED) process(linkReceiver, base, startTag.getAttributeValue("src"));
					else if (name == HTMLElementName.IMG || name == HTMLElementName.SCRIPT) process(linkReceiver, base, startTag.getAttributeValue("src"));
					else if (name == HTMLElementName.OBJECT) process(linkReceiver, base, startTag.getAttributeValue("data"));
					else if (name == HTMLElementName.A || name == HTMLElementName.AREA || name == HTMLElementName.LINK) process(linkReceiver, base, startTag.getAttributeValue("href"));
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

					// META REFRESH/LOCATION
					else if (name == HTMLElementName.META) {
						final String equiv = startTag.getAttributeValue("http-equiv");
						final String content = startTag.getAttributeValue("content");
						if (equiv != null && content != null) {
							equiv.toLowerCase();

							// http-equiv="refresh" content="0;URL=http://foo.bar/..."
							if (equiv.equals("refresh")) {

								final int pos = URLEQUAL_PATTERN.search(content);
								if (pos != -1) {
									final String urlPattern = content.substring(pos + URLEQUAL_PATTERN.length());
									final URI refresh = BURL.parse(urlPattern);
									if (refresh != null) {
										// This shouldn't happen by standard, but people unfortunately does it.
										if (! refresh.isAbsolute() && LOGGER.isDebugEnabled()) LOGGER.debug("Found relative META refresh URL: \"{}\"", urlPattern);
										linkReceiver.metaRefresh(base.resolve(refresh));
									}
								}
							}

							// http-equiv="location" content="http://foo.bar/..."
							if (equiv.equals("location")) {
								final URI metaLocation = BURL.parse(content);
								if (metaLocation != null) {
									// This shouldn't happen by standard, but people unfortunately does it.
									if (! metaLocation.isAbsolute() && LOGGER.isDebugEnabled()) LOGGER.debug("Found relative META location URL: \"{}\"", content);
									linkReceiver.metaLocation(this.metaLocation = base.resolve(metaLocation));
								}
							}
						}
					}
				}
				else if (segment instanceof EndTag) {
					final EndTag endTag = (EndTag)segment;
					final String name = endTag.getName();
					if (name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) {
						inSpecialText = Math.max(0, inSpecialText - 1); // Ignore extra closing tags
					}

					if (digestAppendable != null) {
						if (endTag.getTagType() != EndTagType.NORMAL) continue;
						digestAppendable.endTag(endTag);
					}

					if ( name == HTMLElementName.SPAN || name == HTMLElementName.FONT || name == HTMLElementName.STRONG ||
							name == HTMLElementName.I || name == HTMLElementName.B || name == HTMLElementName.EM )
						rewrite = false;
					else
					if ( name == HTMLElementName.SCRIPT || name == HTMLElementName.OPTION || name == HTMLElementName.STYLE ) {
						textContent.append("\n");
						skipping = Math.max( 0, skipping-1 ); // Ignore extra closing tags
						rewrite = false;
					} else
						if (ENDLINE_SET.contains(name))
							textContent.append("\n");
						else
							textContent.append(" ");
				}
				else {
					java.nio.CharBuffer cb = streamedSource.getCurrentSegmentCharBuffer();

					if (inSpecialText == 0) {

						if (digestAppendable != null) {
							if (segment instanceof CharacterReference) ((CharacterReference)segment).appendCharTo(digestAppendable);
							else {
								digestAppendable.append(cb.array(), cb.position(), cb.remaining());
							}
						}

						if (textProcessor != null)  {
							if (segment instanceof CharacterReference)
								((CharacterReference) segment).appendCharTo(textProcessor);
							else {
								textProcessor.append(cb);
							}
						}
						if (segment instanceof CharacterReference)
							((CharacterReference) segment).appendCharTo(textContent);
						else {
							textContent.append(cb);
						}
					}

				}
				if (rewrite && (skipping == 0)) {
					java.nio.CharBuffer cb = streamedSource.getCurrentSegmentCharBuffer();
					rewritten.append(cb.array(), cb.position(), cb.remaining());
				}

			}
		}

		// Find language in rewritten

		String tld = uri.getHost().substring(uri.getHost().lastIndexOf('.') + 1);
		String guessedLang = guessedLanguage == null ? null : guessedLanguage.getLanguage();
		if (textContent.length() > 5) {
			String textForLangDetect = textContent.subSequence(0, Math.min(textContent.length(), MAX_LANGUAGE_PAGE_CONTENT) - 1).toString() + " "; // +" " is Workaround CLD2 bug (SIGSEGV)
			Cld2Result result = Cld2Tool.detect(textForLangDetect, tld, guessedLang);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Raw text submitted to language detection is {}", textContent.toString());
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

		// This is to avoid collapsing 3xx pages with boilerplate content (as opposed to 0-length content).
		if (digestAppendable != null && httpResponse.getStatusLine().getStatusCode() / 100 == 3) {
			digestAppendable.append((char)0);
			if (location != null) digestAppendable.append(BURL.toByteArray(location));
			digestAppendable.append((char)0);
			if (metaLocation != null) digestAppendable.append(BURL.toByteArray(metaLocation));
			digestAppendable.append((char)0);
		}
		//LOGGER.info("Finished parsing {} , digest : {}", base, digestAppendable != null ? digestAppendable.digest() : "null");
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
		return textContent.textContent;
	}

	/** Returns the BURL location header, if present; if it is not present, but the page contains a valid metalocation, the latter
	 *  is returned. Otherwise, {@code null} is returned.
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
			cd.decode(ByteBuffer.wrap(b,0,blen));
		} catch (CharacterCodingException e) {
			return false;
		}
		return true;
	}

    protected static final TextPattern DOCTYPE_PATTERN = new TextPattern("<doctype", TextPattern.CASE_INSENSITIVE);
	/** Used by {@link #getCharsetName(byte[], int)}. */
	protected static final TextPattern META_PATTERN = new TextPattern("<meta", TextPattern.CASE_INSENSITIVE);
	/** Used by {@link #getLanguageName(byte[], int)}. */
	protected static final TextPattern HTML_PATTERN = new TextPattern("<html", TextPattern.CASE_INSENSITIVE);

	/** Used by {@link #getCharsetName(byte[], int)}. */
	protected static final Pattern HTTP_EQUIV_PATTERN = Pattern.compile(".*http-equiv\\s*=\\s*('|\")?content-type('|\")?.*", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
	/** Used by {@link #getCharsetName(byte[], int)}. */
	protected static final Pattern CONTENT_PATTERN = Pattern.compile(".*content\\s*=\\s*('|\")([^'\"]*)('|\").*", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
	/** Used by {@link #getCharsetName(byte[], int)}. */
	protected static final Pattern CHARSET_PATTERN = Pattern.compile (".*charset\\s*=\\s*\"?([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=\"]]+).*", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
	protected static final Pattern LANG_PATTERN = Pattern.compile (".*lang\\s*=\\s*\"?([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=\"]]+).*", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);

	/** Returns the charset name as indicated by a <code>META</code>
	 * <code>HTTP-EQUIV</code> element, if
	 * present, interpreting the provided byte array as a sequence of
	 * ISO-8859-1-encoded characters. Only the first such occurrence is considered (even if
	 * it might not correspond to a valid or available charset).
	 *
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
		while((start = META_PATTERN.search(buffer, start, length)) != -1) {

			/* Look for attribute http-equiv with value content-type,
			 * if present, look for attribute content and, if present,
			 * return its value. */

			int end = start;
			while(end < length && buffer[end] != '>') end++; // Look for closing '>'
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

	public static String getLanguageName(final byte buffer[], final int length) {
		int start = 0;
		while((start = HTML_PATTERN.search(buffer, start, length)) != -1) {

			/* Look for tag <html with value lang,
			 * return its value. */

			int end = start;
			while(end < length && buffer[end] != '>') end++; // Look for closing '>'
			if (end == length) return null; // No closing '>'

			final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(buffer, start + META_PATTERN.length(), end - start - META_PATTERN.length());

			final Matcher mCharset = LANG_PATTERN.matcher(tagContent);
			if (mCharset.matches())
				return getLanguageNameFromHTML(mCharset.group(0)); // got it!
			start = end + 1;
		}

		return null; // no '<meta' found
	}

	/**

	 *//*
    public static String getDocType(final String document) {
        final Matcher m = DOCTYPE_PATTERN.matcher(document);
        if (m.matches()) {
            final String s = m.group(1);
            int start = 0, end = s.length();
            // TODO: we discard delimiting single/double quotes; is it necessary?
            if (end > 0 && (s.charAt(0) == '\"' || s.charAt(0) == '\'')) start = 1;
            if (end > 0 && (s.charAt(end - 1) == '\"' || s.charAt(end - 1) == '\'')) end--;
            if (start < end) return s.substring(start, end);
        }
        return null;
    }*/

	/** Extracts the charset name from the header value of a <code>content-type</code>
	 *  header using a regular expression.
	 *
	 *  <strong>Warning</strong>: it might not work if someone puts the string <code>charset=</code>
	 *  in a string inside some attribute/value pair.
	 *
	 *  @param headerValue The value of a <code>content-type</code> header.
	 *  @return the charset name, or {@code null} if no
	 *  charset is specified; note that the charset might be not valid or not available.
	 */
	public static String getCharsetNameFromHeader(final String headerValue) {
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

	public static String getLanguageNameFromHTML(final String headerValue) {
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
				new Parameter[] {
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

		final HTMLParser<Void> htmlParser =  new HTMLParser<>(BinaryParser.forName(digester), (TextProcessor<Void>)null, crossAuthorityDuplicates, charBufferSize);
		final SetLinkReceiver linkReceiver = new SetLinkReceiver();
		final byte[] digest;

		if (!jsapResult.userSpecified("file")) {
			final URI uri = new URI(url);
			final HttpGet request = new HttpGet(uri);
			request.setConfig(RequestConfig.custom().setRedirectsEnabled(false).build());
			digest = htmlParser.parse(uri, HttpClients.createDefault().execute(request), linkReceiver);
		}
		else {
			final String file = jsapResult.getString("file");
			final String content = IOUtils.toString(new InputStreamReader(new FileInputStream(file)));
			digest = htmlParser.parse(BURL.parse(url) , new StringHttpMessages.HttpResponse(content), linkReceiver);
		}

		System.out.println("DigestHexString: " + Hex.encodeHexString(digest));
		System.out.println("Links: " + linkReceiver.urls);

		final Set<String> urlStrings = new ObjectOpenHashSet<>();
		for (final URI link: linkReceiver.urls) urlStrings.add(link.toString());
		if (urlStrings.size() != linkReceiver.urls.size()) System.out.println("There are " + linkReceiver.urls.size() + " URIs but " + urlStrings.size() + " strings");

	}

}
