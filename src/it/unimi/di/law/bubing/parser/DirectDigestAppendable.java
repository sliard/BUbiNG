package it.unimi.di.law.bubing.parser;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import it.unimi.di.law.bubing.util.MurmurHash3_128;
import it.unimi.di.law.bubing.util.Util;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import net.htmlparser.jericho.EndTag;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.HTMLElements;
import net.htmlparser.jericho.StartTag;

import java.net.URI;
import java.util.List;

/**
 * A class computing the digest of a page.
 * <p>
 * <p>The page is somewhat simplified before being passed (as a sequence of bytes obtained
 * by breaking each character into the upper and lower byte) to a {@link Hasher}.
 * All start/end tags are case-normalized, and their whole content (except for the
 * element-type name) is removed.
 * An exception is made for <code>SRC</code> attribute of
 * <code>FRAME</code> and <code>IFRAME</code> elements, as they are necessary to
 * distinguish correctly framed pages without alternative text. The attributes will be resolved
 * w.r.t. the {@linkplain #init(URI) URL associated to the page}.
 * Moreover, non-HTML tags are substituted with a special tag <code>unknown</code>.
 * <p>
 * <p>For what concerns the text, all digits are substituted by a whitespace, and nonempty whitespace maximal sequences are coalesced
 * to a single space. Tags are considered as a non-whitespace character.
 * <p>
 * <p>To avoid clashes between digests coming from different sites, you can optionally set a URL
 * (passed to the {@link #init(URI)} method) whose scheme+authority will be used to update the digest before adding the actual text page.
 * <p>
 * <p>Additionally, since BUbiNG 0.9.10 location redirect URLs (both from headers and from META elements),
 * if present, are mixed in to avoid collapsing 3xx pages with boilerplate text.
 */
public final class DirectDigestAppendable implements Appendable {

  /**
   * Cached byte representations of all opening tags. The map must be queried using {@linkplain HTMLElementName Jericho names}.
   */
  protected static final Reference2ObjectOpenHashMap<String, byte[]> startTags;

  /**
   * Cached byte representations of all closing tags. The map must be queried using {@linkplain HTMLElementName Jericho names}.
   */
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

  /**
   * True iff the last character appended was a space.
   */
  protected boolean lastAppendedWasSpace;
  /**
   * The last returne digest, or {@code null} if {@link #init(URI)} has been called but {@link #digest()} hasn't.
   */
  protected byte[] digest;
  protected MurmurHash3_128.LongPair tempDigest = null;
  protected static final int DIGEST_BUFFER_SIZE = 64 * 1024;
  protected char[] buffer = null;
  protected int bufferLen = 0;
  protected int totalLen = 0;
  /**
   * Create a digest appendable using a given hash function.
   *
   * @param hashFunction the hash function used to digest.
   */
  public DirectDigestAppendable(final HashFunction hashFunction) {
    this.hashFunction = hashFunction;
    buffer = new char[DIGEST_BUFFER_SIZE];
    tempDigest = new MurmurHash3_128.LongPair();
  }

  /**
   * Initializes the digest computation.
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
      for (int i = 0; i < host.length(); i++)
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
    char[] chars = csq.toString().toCharArray();
    for (char c : chars)
      append(c);
    return this;
  }

  public void append(byte[] a) {
    for (byte b : a) appendChar((char) b);
  }

  public void append(char[] a, int offset, int length) {
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

