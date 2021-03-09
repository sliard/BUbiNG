package it.unimi.di.law.bubing.util;

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

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.lang.MutableString;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.*;
import java.util.regex.Pattern;

// RELEASE-STATUS: DIST

/** Static methods to manipulate normalized, canonical URLs in BUbiNG.
 *
 * <p>URLs in BUbiNG (and in the crawl data) are represented by instances of {@link URI}
 * passing through a number of normalizations and fixes that
 * try to avoid useless page duplication (for the record, {@link URL} is known to be quite broken).
 * In particular, URLs that contains a scheme but not an authority (e.g., <code>mailto:foo@example.com</code>)
 * will not be considered valid.
 *
 * You should always use the provided
 * {@linkplain #parse(MutableString) factory method} to create a {@link URI} to represent a BUbiNG URL. The method is
 * oriented towards errors&mdash;it returns {@code null} upon malformed URLs rather than
 * throwing an exception.
 *
 * <p>{@link URI} instances generated by the factory methods of this class are guaranteed to be entirely formed by ASCII strings,
 * provided that you always access the raw version of the getters (e.g., {@link URI#getRawPath()}).
 * In particular, the {@link #toString()} method always returns an ASCII string.
 *
 * <p>A number of methods such as {@link #toByteArray(URI)}, {@link #schemeAndAuthority(byte[])},
 * {@link #fromNormalizedSchemeAuthorityAndPathQuery(String, byte[])}, and so on make it possible to
 * manipulate URLs represented as byte arrays of ASCII characters, greatly reducing the memory
 * usage in applications (such as crawlers) storing millions of URLs.
 *
 * <p><strong>WARNING</strong>: methods returning byte arrays from {@link URI} instances
 * <em>assume that the argument getters in raw form all return ASCII strings</em>, as it
 * happens from {@link URI} instances parsed by this class. The methods blindly computes
 * the representation by taking the lower 7 bits of each character. Enable assertions for
 * this class if you suspect that the methods are being applied to non-ASCII {@link URI} instances.
 */
public final class BURL {
	private static final Logger LOGGER = LoggerFactory.getLogger(BURL.class);

	private static final boolean DEBUG = false;

	/** Characters that will cause a URI spec to be rejected. */
	public static final char[] FORBIDDEN_CHARS = { '\n', '\r' };

	/** A list of bad characters. It includes the backslash, replaced by the slash, and illegal characters
	 * such as spaces and braces, which are replaced by the equivalent percent escape. Square brackets
	 * are percent-escaped, too, albeit legal in some circumstances, as they appear frequently in paths. */
	public static final char[] BAD_CHAR = new char[] { '\\', ' ', '\t', '[', ']', '"', '|', '{', '}', '^', '<', '>', '`' };
	/** Substitutes for {@linkplain #BAD_CHAR bad characters}. */
	public static final String[] BAD_CHAR_SUBSTITUTE = new String[BAD_CHAR.length];

	private static final char[] HEX_DIGIT = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	static {
		BAD_CHAR_SUBSTITUTE[0] = "/";
		for(int i = BAD_CHAR.length; i-- != 1;) BAD_CHAR_SUBSTITUTE[i] = (BAD_CHAR[i] < 16 ? "%0" : "%") + Integer.toHexString(BAD_CHAR[i]).toUpperCase();
	}

	private BURL() {}

	/**  Creates a new BUbiNG URL from a string specification if possible, or returns {@code null} otherwise.
	 *
	 * @param spec the string specification for a URL.
	 * @return a BUbiNG URL corresponding to <code>spec</code> without possibly the fragment, or {@code null} if <code>spec</code> is malformed.
	 * @see #parse(MutableString)
	 */

	public static URI parse(String spec) {
		return parse(new MutableString(spec));
	}

	public static URI parseAndCanonicalize(String spec) { return parseAndCanonicalize(new MutableString(spec), true);}

	/** Creates a new BUbiNG URL from a {@linkplain MutableString mutable string}
	 * specification if possible, or returns {@code null} otherwise.
	 *
	 * <p>The conditions for this method not returning {@code null} are as follows:
	 * <ul>
	 * <li><code>spec</code>, once trimmed, must not contain characters in {@link #FORBIDDEN_CHARS};
	 * <li>once characters in {@link #BAD_CHAR} have been substituted with the corresponding
	 * strings in {@link #BAD_CHAR_SUBSTITUTE}, and percent signs not followed by two hexadecimal
	 * digits have been substituted by <code>%25</code>, <code>spec</code> must not throw
	 * an exception when {@linkplain URI#URI(java.lang.String) made into a URI}.
	 * <li>the {@link URI} instance so obtained must not be {@linkplain URI#isOpaque() opaque}.
	 * <li>the {@link URI} instance so obtained, if {@linkplain URI#isAbsolute() absolute},
	 * must have a non-{@code null} {@linkplain URI#getAuthority() authority}.
	 * </ul>
	 *
	 * <p>For efficiency, this method modifies the provided specification,
	 * and in particular it makes it {@linkplain MutableString#loose() loose}. <i>Caveat emptor</i>.
	 *
	 * <p>Fragments are removed (for a web crawler fragments are just noise). {@linkplain URI#normalize() Normalization}
	 * is applied for you. Scheme and host name are downcased. If the URL has no host name, it is guaranteed
	 * that the path is non-{@code null} and non-empty (by adding a slash, if necessary). If
	 * the host name ends with a dot, it is removed. Percent-specified special characters have always their
	 * hexadecimal letters in upper case.
	 *
	 * @param spec the string specification for a URL; <strong>it can be modified by this method</strong>, and
	 * in particularly it will always be made {@linkplain MutableString#loose() loose}.
	 * @return a BUbiNG URL corresponding to <code>spec</code> without possibly the fragment, or {@code null} if
	 * <code>spec</code> is malformed.
	 * @see #parse(String)
	 */

	public static URI parse(MutableString spec) {
		return parseAndCanonicalize(spec, false);
	}

	public static URI parseAndCanonicalize(MutableString spec, boolean canonicalize) {

		if (DEBUG) LOGGER.debug("parse(" + spec + ")");
		spec.loose().trim();
		if (spec.indexOfAnyOf(FORBIDDEN_CHARS) != -1) return null;

		// By the book, but flexible.
		spec.replace(BAD_CHAR, BAD_CHAR_SUBSTITUTE);

		// Find percents not followed by two hexadecimal digits and fix them.
		final char[] a = spec.array();
		final int l = spec.length();

		for(int i = l; i-- != 0;) {
			if (a[i] == '%') {
				if (i >= l - 2 || ! isHexDigit(a[i + 1]) || ! isHexDigit(a[i + 2])) spec.insert(i + 1, "25");
				else {
					// Normalize hex codes to upper case.
					spec.setCharAt(i + 1, Character.toUpperCase(spec.charAt(i + 1)));
					spec.setCharAt(i + 2, Character.toUpperCase(spec.charAt(i + 2)));
				}
			}
		}

		try {
			final URI uri = new URI(spec.toString()).normalize();

			if (uri.isOpaque()) return null;

			// Let us force parsing host, user info and port, or get an exception otherwise.
			if (uri.isAbsolute()) uri.parseServerAuthority();

			// Downcase
			String scheme = uri.getScheme();
			if (scheme != null) {
				if (scheme.indexOf('\0') != -1) return null; // Workaround for URI bug
				scheme = scheme.toLowerCase();
			}

			// No absolute URL without authority (e.g., file://).
			if (uri.isAbsolute() && uri.getAuthority() == null) return null;

			// Workaround for URI bug
			if (uri.getPath() != null && uri.getPath().indexOf('\0') != -1) return null;
			if (uri.getUserInfo() != null && uri.getUserInfo().indexOf('\0') != -1) return null;
			if (uri.getQuery() != null && uri.getQuery().indexOf('\0') != -1) return null;

			// Remove trailing dot in host name if present and downcase
			String host = uri.getHost();
			if (host != null) {
				if (host.indexOf('\0') != -1) return null;  // Workaround for URI bug
				if (host.endsWith(".")) host = host.substring(0, host.length() - 1);
				host = host.toLowerCase();
			}

			// Substitute empty path with slash in absolute URIs.
			String rawPath = uri.getRawPath();

			if (host != null && (rawPath == null || rawPath.length() == 0)) rawPath = "/";



			// Rebuild, discarding fragment, parsing again a purely ASCII string and renormalizing (convoluted, but it does work).
			if (canonicalize)
				return new URI(sanitizeAndRepack(scheme, uri.getRawUserInfo(), host, uri.getPort(), rawPath, canonicalizeQuery(uri.getRawQuery()))).normalize();
			else
				return new URI(sanitizeAndRepack(scheme, uri.getRawUserInfo(), host, uri.getPort(), rawPath, uri.getRawQuery())).normalize();
		}
		catch (URISyntaxException e) {
			return null;
		}
		catch(Exception e) {
			LOGGER.warn("Unexpected exception while parsing " + spec, e);
			return null;
		}
	}

	private static final List<String> blackListedParams;
	static {
		blackListedParams = Arrays.asList(
			// SESSION VARIABLES
			// From OWASP : (jsessionid|aspsessionid|asp.net_sessionid|phpsession|phpsessid|weblogicsession|session_id|session-id|cfid|cftoken|cfsid|jservsession|jwsession
			"PHPSESSID","jsessionid","osCsid","zenid","ncid","SID","sid","CFID","CFTOKEN",
			"aspsessionid","asp.net_sessionid","phpsession","weblogicsession","session_id","session-id","cfsid","jservsession","jwsession",
			// REFERRER VARIABLES
			"referrerPage",
			// TRACKING VARIABLES
			"campaign_id","campaign",
			"affiliation","affiliate",
			"utm_campaign","utm_source","utm_medium","utm_term","utm_content",
			"itm_campaign","itm_source","itm_medium","itm_term","itm_content","itm_channel","itm_audience",
			"pk_campaign","pk_kwd","pk_keyword");
	}

	private static final Pattern canonicalizeQueryPattern;
	private static final Pattern canonicalizeQueryPattern2;
	private static final Pattern canonicalizeQueryPattern3;

	static {
		canonicalizeQueryPattern2 = Pattern.compile("(^[;&]*|[;&]*$)");
		canonicalizeQueryPattern3 = Pattern.compile("([;&])[;&]*");
		var canonicalizeQueryPatternStringBuilder = new StringBuilder();
		canonicalizeQueryPatternStringBuilder.append("(?<=^|[&;])(");
		boolean first = true;
		for (String param : blackListedParams) {
			if (!first)
				canonicalizeQueryPatternStringBuilder.append("|");
			first = false;
			canonicalizeQueryPatternStringBuilder.append(param);
		}
		canonicalizeQueryPatternStringBuilder.append(")=[^;&]*(?=[;&]|$)");
		canonicalizeQueryPattern = Pattern.compile(canonicalizeQueryPatternStringBuilder.toString(),Pattern.CASE_INSENSITIVE);
	}

	/**
	 * Canonicalize Query by removing query parameters that are known to have no effect on content
	 * and may create false duplicate content : SESSION PARAMETERS, TRACKING PARAMETERS, etc.
	 * @param query
	 * @return canonicalized query
	 */
  public static String canonicalizeQuery(String query) {
		if (query == null)
			return null;
		if (query.length() == 0)
			return query;

		String result1 = canonicalizeQueryPattern.matcher(query).replaceAll("");
		String result2 = canonicalizeQueryPattern2.matcher(result1).replaceAll("");
		String result = canonicalizeQueryPattern3.matcher(result2).replaceAll("$1");


		if (LOGGER.isTraceEnabled())
			if (query.length() != result.length())
				LOGGER.trace("Canonicalized {} to {}", query,result);
		return result;
	}

	public static boolean isCanonicalQuery(byte[] query) {
		return isCanonicalQuery(Util.toString(query));
  }

	public static boolean isCanonicalQuery(String query) {
  	return canonicalizeQuery(query).equals(query);
	}

	/** If the argument string does not contain non-ASCII characters, returns the string itself;
	 * otherwise, encodes non-ASCII characters by %XX-encoded UTF-8 sequences.
	 *
	 * @param s a string.
	 * @return <code>c</code> with non-ASCII characters replaced by %XX-encoded UTF-8 sequences.
	 */
	private static String sanitize(final String s) {
		int i = s.length();
    for(i = s.length(); i-- != 0;) if (s.charAt(i) >= (char)128) break;
    if (i == -1) return s;

		final ByteBuffer byteBuffer = Charsets.UTF_8.encode(CharBuffer.wrap(s));
		final StringBuilder stringBuilder = new StringBuilder();

		while (byteBuffer.hasRemaining()) {
			final int b = byteBuffer.get() & 0xff;
			if (b >= 0x80) stringBuilder.append('%').append(HEX_DIGIT[b >> 4 & 0xf]).append(HEX_DIGIT[b & 0xf]);
			else stringBuilder.append((char)b);
		}

		return stringBuilder.toString();
	}

	/** {@linkplain #sanitize(String) Sanitizes} all arguments (any of which may be {@code null})
	 * and repack them as the string representation of a URI. The behaviour of this method is
	 * similar to that of {@link URI#URI(String, String, String, int, String, String, String)}, but
	 * we do not escape reserved characters, and the result is guaranteed to be an ASCII string.
	 *
	 * @return the string representation of a URI formed by the given components with non-ASCII
	 * characters replaced by %XX-encoded UTF-8 sequences.
	 * @see #sanitize(String)
	 */
	private static String sanitizeAndRepack(final String scheme, final String userInfo, final String host, int port, final String path, final String query) {
		final StringBuilder sb = new StringBuilder();
		if (scheme != null) {
			sb.append(sanitize(scheme)).append(':');
			if (scheme.equalsIgnoreCase("http") && port == 80 || scheme.equalsIgnoreCase("https") && port == 443) port = -1;
		}
		if (host != null) {
			sb.append("//");
			if (userInfo != null) sb.append(sanitize(userInfo)).append('@');
			final boolean needBrackets = host.indexOf(':') >= 0 && ! host.startsWith("[") && ! host.endsWith("]");
			if (needBrackets) sb.append('[');
			sb.append(sanitize(host));
			if (needBrackets) sb.append(']');
			if (port != -1) sb.append(':').append(port);
		}
		if (path != null) sb.append(sanitize(path));
		if (query != null) sb.append('?').append(sanitize(query));
		return sb.toString();
	}

	/** Creates a new BUbiNG URL from a normalized ASCII string represented by a byte array.
	 *
	 * <p>The string represented by the argument will <em>not</em> go through {@link #parse(MutableString)}.
	 * {@link URI#create(String)} will be used instead.
	 *
	 * @param normalized a normalized URI string represented by a byte array.
	 * @return the corresponding BUbiNG URL.
	 * @throws IllegalArgumentException if <code>normalized</code> does not parse correctly.
	 */
	public static URI fromNormalizedByteArray(final byte[] normalized) {
		return URI.create(Util.toString(normalized));
	}

	/** Creates a new BUbiNG URL from a normalized ASCII string representing scheme and
	 * authority and a byte-array representation of a normalized ASCII path and query.
	 *
	 * <p>This method is intended to combine the results of {@link #schemeAndAuthority(URI)}/
	 * {@link #schemeAndAuthority(byte[])} and {@link #pathAndQueryAsByteArray(byte[])}(
	 * {@link #pathAndQueryAsByteArray(URI)}.
	 *
	 * @param schemeAuthority an ASCII string representing scheme+authority.
	 * @param normalizedPathQuery the byte-array representation of a normalized ASCII path and query.
	 * @return the corresponding BUbiNG URL.
	 * @throws IllegalArgumentException if the two parts, concatenated, do not parse correctly.
	 */
	public static URI fromNormalizedSchemeAuthorityAndPathQuery(final String schemeAuthority, final byte[] normalizedPathQuery) {
		final char[] array = new char[schemeAuthority.length() + normalizedPathQuery.length];
		schemeAuthority.getChars(0, schemeAuthority.length(), array, 0);
		for(int i = array.length, j = normalizedPathQuery.length; j-- != 0;) array[--i] = (char)normalizedPathQuery[j];
		return URI.create(new String(array));
	}

	/** Creates a new BUbiNG URL from a byte-array representation of a normalized scheme and
	 * authority and a byte-array representation of a normalized ASCII path and query.
	 *
	 * <p>This method is intended to combine the results of {@link #schemeAndAuthority(URI)}/
	 * {@link #schemeAndAuthority(byte[])} and {@link #pathAndQueryAsByteArray(byte[])}(
	 * {@link #pathAndQueryAsByteArray(URI)}.
	 *
	 * @param schemeAuthority the byte-array represention of a normalized scheme+authority.
	 * @param normalizedPathQuery the byte-array representation of a normalized ASCII path and query.
	 * @return the corresponding BUbiNG URL.
	 * @throws IllegalArgumentException if the two parts, concatenated, do not parse correctly.
	 */
	public static URI fromNormalizedSchemeAuthorityAndPathQuery(final byte[] schemeAuthority, final byte[] normalizedPathQuery) {
		final char[] array = new char[schemeAuthority.length + normalizedPathQuery.length];
		for(int i = schemeAuthority.length; i-- != 0;) array[i] = (char)schemeAuthority[i];
		for(int i = array.length, j = normalizedPathQuery.length; j-- != 0;) array[--i] = (char)normalizedPathQuery[j];
		return URI.create(new String(array));
	}

	private static boolean isHexDigit(final char c) {
		return c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f';
	}

	/** Returns an ASCII byte-array representation of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return an ASCII byte-array representation {@code url}.
	 */
	public static byte[] toByteArray(final URI url) {
		return Util.toByteArray(url.toString());
	}

	/** Writes an ASCII representation of a BUbiNG URL in a {@link ByteArrayList}.
	 *
	 * @param url a BUbiNG URL.
	 * @param list a byte list that will contain the byte representation.
	 * @return {@code list}.
	 */
	public static ByteArrayList toByteArrayList(final URI url, final ByteArrayList list) {
		return Util.toByteArrayList(url.toString(), list);
	}

	/** Returns an ASCII byte-array representation of
	 * the {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return an ASCII byte-array representation of
	 * the {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of a BUbiNG URL.
	 */
	public static byte[] pathAndQueryAsByteArray(final URI url) {
		final String query = url.getRawQuery();
		final String path = url.getRawPath();
		final byte[] result = new byte[path.length() + (query != null ? 1 + query.length() : 0)];

		for (int i = path.length(); i-- != 0;) {
			assert path.charAt(i) < (char)0x80 : path.charAt(i);
			result[i] = (byte)(path.charAt(i) & 0x7F);
		}

		if (query != null) {
			result[path.length()] = '?';
			for (int j = query.length(), i = result.length; j-- != 0;) {
				assert query.charAt(j) < (char)0x80 : query.charAt(j);
				result[--i] = (byte)(query.charAt(j) & 0x7F);
			}
		}

		return result;
	}

	/** Returns the concatenated {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return the concatenated {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of <code>uri</code>.
	 */
	public static String pathAndQuery(final URI url) {
		final String query = url.getRawQuery();
		return query != null ? url.getRawPath() + '?' + query : url.getRawPath();
	}

	/** Returns the concatenated {@linkplain URI#getScheme()} and {@link URI#getRawAuthority() raw authority} of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return the concatenated {@linkplain URI#getScheme()} and {@link URI#getRawAuthority() raw authority} of <code>uri</code>.
	 */
	public static String schemeAndAuthority(final URI url) {
		return url.getScheme() + "://" + url.getRawAuthority();
	}

	/** Extracts the path and query of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url a byte-array representation of a BUbiNG URL.
	 * @return the path and query in byte-array representation.
	 */
	public static byte[] pathAndQueryAsByteArray(final byte[] url){
		final int startOfAuthority = Bytes.indexOf(url, (byte)':') + 3;
		assert url[startOfAuthority - 1] == '/' : url[startOfAuthority - 1];
		assert url[startOfAuthority - 2] == '/' : url[startOfAuthority - 2];
		return Arrays.copyOfRange(url, ArrayUtils.indexOf(url, (byte)'/', startOfAuthority), url.length);
	}

	/** Extracts the path and query of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url BUbiNG URL.
	 * @return the path and query in byte-array representation.
	 */
	public static byte[] pathAndQueryAsByteArray(ByteArrayList url){
		final byte[] array = url.elements();
		final int startOfAuthority = Bytes.indexOf(array, (byte)':') + 3;
		assert array[startOfAuthority - 1] == '/' : array[startOfAuthority - 1];
		assert array[startOfAuthority - 2] == '/' : array[startOfAuthority - 2];
		return Arrays.copyOfRange(array, ArrayUtils.indexOf(array, (byte)'/', startOfAuthority), url.size());
	}

	/** Returns the starting position (i.e., the slash) of the path+query in the given BUbiNG URL.
	 *
	 * @param url a byte-array representation of a BUbiNG URL; extra bytes can be present after the URL.
	 * @return the start of the path+query in byte-array representation.
	 */
	public static int startOfpathAndQuery(final byte[] url) {
		int i, j;
		for(i = 0, j = 2; ; i++) if (url[i] == '/' && j-- == 0) return i;
	}

	/** Extracts the scheme+authority of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url an absolute BUbiNG URL.
	 * @return the scheme+authority of <code>url</code> in byte-array representation.
	 */
	public static byte[] schemeAndAuthorityAsByteArray(final byte[] url){
		return Arrays.copyOfRange(url, 0, startOfpathAndQuery(url));
	}

	public static boolean sameSchemeAuthority(final byte[] schemeAuthority, final URI url) {
		final String scheme = url.getScheme();
		int schemeLength = scheme.length();
		if (schemeAuthority.length < schemeLength + 3) return false;
		for(int i = schemeLength; i-- != 0;) if (schemeAuthority[i] != (byte)scheme.charAt(i)) return false;
		if (schemeAuthority[schemeLength++] != (byte)':') return false;
		if (schemeAuthority[schemeLength++] != (byte)'/') return false;
		if (schemeAuthority[schemeLength++] != (byte)'/') return false;

		final String authority = url.getRawAuthority();
		if (schemeAuthority.length != schemeLength + authority.length()) return false;
		for(int i = authority.length(); i-- != 0;) if (schemeAuthority[schemeLength + i] != (byte)authority.charAt(i)) return false;
		return true;
	}

	/** Extracts the host part from a scheme+authority by removing the scheme, the user info and the port number.
	 *
	 * @param schemeAuthority a scheme+authority.
	 * @return the host part.
	 */
	public static String hostFromSchemeAndAuthority(final byte[] schemeAuthority){
		final int length = schemeAuthority.length;
		final int startOfAuthority = ArrayUtils.indexOf(schemeAuthority, (byte)':') + 3;
		int atPosition;
		for(atPosition = startOfAuthority; atPosition < length; atPosition++) if (schemeAuthority[atPosition] == (byte)'@') break;
		final int startOfHost = atPosition != length ? atPosition + 1 : startOfAuthority;

		int endOfHost;
		for(endOfHost = startOfHost; endOfHost < length; endOfHost++) if (schemeAuthority[endOfHost] == (byte)':') break;
		final char[] host = new char[endOfHost - startOfHost];
		for(int i = startOfHost; i < endOfHost; i++) host[i - startOfHost] = (char)schemeAuthority[i];
		return new String(host);
	}

	public static byte[] hostFromSchemeAuthorityAsByteArray( final byte[] schemeAuthority ) {
		final int length = schemeAuthority.length;
		final int startOfAuthority = ArrayUtils.indexOf(schemeAuthority, (byte)':') + 3;
		int atPosition;
		for(atPosition = startOfAuthority; atPosition < length; atPosition++) if (schemeAuthority[atPosition] == (byte)'@') break;
		final int startOfHost = atPosition != length ? atPosition + 1 : startOfAuthority;

		int endOfHost;
		for(endOfHost = startOfHost; endOfHost < length; endOfHost++) if (schemeAuthority[endOfHost] == (byte)':') break;
		return Arrays.copyOfRange( schemeAuthority, startOfHost, endOfHost );

	}


	/** Finds the start of the host part in a URL.
	 *
	 * @param url a byte-array representation of a BUbiNG URL.
	 * @return the starting position of the host in the given url.
	 */
	public static final int startOfHost(final byte[] url) {
		final int startOfAuthority = ArrayUtils.indexOf(url, (byte)':') + 3;
		final int endOfAuthority = ArrayUtils.indexOf(url, (byte)'/', startOfAuthority);
		int atPosition;
		for(atPosition = startOfAuthority; atPosition < endOfAuthority; atPosition++) if (url[atPosition] == (byte)'@') break;
		return atPosition != endOfAuthority ? atPosition + 1 : startOfAuthority;
	}

	/** Finds the length of the host part in a scheme+authority or URL.
	 *
	 * @param url a byte-array representation of a BUbiNG URL or scheme+authority; only the first {@code length} bytes are considered to be valid.
	 * @param startOfHost the starting position of the host part.
	 * @return the length of the host part in {@code url}.
	 */
	public static final int lengthOfHost(final byte[] url, final int startOfHost) {
		int endOfHost = startOfHost;
		do endOfHost++; while(url[endOfHost] != (byte)':' && url[endOfHost] != (byte)'/');
		return endOfHost - startOfHost;
	}

	/** Extracts the scheme+authority of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url an absolute BUbiNG URL.
	 * @return the scheme+authority of <code>url</code>.
	 */
	public static String schemeAndAuthority(final byte[] url){
		int i, j;
		for(i = 0, j = 2; ; i++) if (url[i] == '/' && j-- == 0) break;
		final char[] array = new char[i];
		for(i = array.length; i-- != 0;) array[i] = (char)url[i];
		return new String(array);
	}

	/** Returns the memory usage associated to a byte array.
	 *
	 * <p>This method is useful in establishing the memory footprint of URLs in byte-array representation.
	 *
	 * @param array a byte array.
	 * @return its memory usage in bytes.
	 */
	public static int memoryUsageOf(final byte[] array) {
		return ((16 + array.length + 7) & -1 << 3) + // Obtained by Classmexer on a 64-bit Sun JVM for Intel.
				8; // This accounts for the space used by the FIFO queue.
	}

	public static void main(String[] args) {
		// Tests for canonicalization
		if (args.length > 0) {
			for (var arg : args) {
				System.out.print(arg + " -> ");
				System.out.print(BURL.parse(arg).toString() + " -> ");
				System.out.println(BURL.parseAndCanonicalize(arg).toString());
			}
		} else {
			var origin = Arrays.asList(
				"http://test.com/t?a=b;c=jsessionid",
				"http://test.com/t?&a=b&PHPSESSID=0abc234def010101&",
				"http://test.com/t?&a=b&PHPSESSID",
				"http://test.com/t?utm_source=abc&utm_medium=web&utm_campaign=email&toto=1"

			);
			var expected = Arrays.asList(
				"http://test.com/t?a=b;c=jsessionid",
				"http://test.com/t?a=b",
				"http://test.com/t?a=b&PHPSESSID",
				"http://test.com/t?toto=1"

			);
			for (int i = 0; i < origin.size(); i++) {
				if (!BURL.parseAndCanonicalize(origin.get(i)).toString().equals(expected.get(i)))
					System.out.println("Error : " + origin.get(i) + " -> " + BURL.parseAndCanonicalize(origin.get(i)).toString());
			}
		}
	}
}
