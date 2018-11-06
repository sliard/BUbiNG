package it.unimi.di.law.bubing.parser;

import com.google.common.hash.HashFunction;
import it.unimi.di.law.bubing.util.MurmurHash3_128;
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
 * <p>The page is somewhat simplified before being passed to MurmurHash3.
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
public final class DigestAppendable implements Appendable
{
  private static final int DIGEST_BUFFER_SIZE = 64 * 1024;
  private static final int DIGEST_SEED = 128945;

  /**
   * Cached byte representations of all opening tags. The map must be queried using {@linkplain HTMLElementName Jericho names}.
   */
  private static final Reference2ObjectOpenHashMap<String, char[]> startTags;

  /**
   * Cached byte representations of all closing tags. The map must be queried using {@linkplain HTMLElementName Jericho names}.
   */
  private static final Reference2ObjectOpenHashMap<String, char[]> endTags;

  static {
    final List<String> elementNames = HTMLElements.getElementNames();
    startTags = new Reference2ObjectOpenHashMap<>( elementNames.size() );
    endTags = new Reference2ObjectOpenHashMap<>( elementNames.size() );

    // Set up defaults for bizarre element types
    startTags.defaultReturnValue( "<unknown>".toCharArray() );
    endTags.defaultReturnValue( "</unknown>".toCharArray() );

    // Scan all known element types and fill startTag/endTag
    for ( final String name : elementNames ) {
      startTags.put( name, ("<"+name+">").toCharArray() );
      endTags.put( name, ("</"+name+">").toCharArray() );
    }
  }

  public final HashFunction hashFunction; // FIXME: useless

  private final char[] buffer;
  private final MurmurHash3_128.LongPair digest;
  private boolean lastAppendedWasSpace;
  private int position;
  private int length;
  private byte[] result;

  public DigestAppendable( final HashFunction hashFunction ) {
    this.hashFunction = hashFunction;
    this.buffer = new char[ DIGEST_BUFFER_SIZE ];
    this.digest = new MurmurHash3_128.LongPair();
    this.lastAppendedWasSpace = false;
    this.position = 0;
    this.length = 0;
    this.result = null;
  }

  @Override
  public final Appendable append( final CharSequence seq ) {
    return append( seq, 0, seq.length() );
  }

  @Override
  public final Appendable append( final CharSequence seq, final int start, final int end ) {
    for ( int i=start; i<end; ++i )
      appendChar( seq.charAt(i) );
    return this;
  }

  @Override
  public final Appendable append( final char c ) {
    appendChar( c );
    return this;
  }

  public final void append( final StartTag startTag ) {
    final String name = startTag.getName();
    append( startTags.get(name) );
    // IFRAME or FRAME + SRC
    if ( name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME ) {
      final String src = startTag.getAttributeValue( "src" );
      if ( src != null ) {
        appendChar( '\"' );
        append( src );
        appendChar( '\"' );
      }
    }
    lastAppendedWasSpace = false;
  }

  public final void append( final EndTag endTag ) {
    append( endTags.get(endTag.getName()) );
    lastAppendedWasSpace = false;
  }

  public final void append( final char[] chars ) {
    append( chars, 0, chars.length );
  }

  public final void append( final char[] chars, final int offset, final int length ) {
    for ( int i=offset; i<offset+length; ++i )
      appendChar( chars[i] );
  }

  /**
   * Initializes the digest computation.
   *
   * @param url a URL, or {@code null} for no URL. In the former case, the host name will be used to initialize the digest.
   */
  public final void init( final URI url ) {
    MurmurHash3_128.murmurhash3_x64_128_init( DIGEST_SEED, digest );
    lastAppendedWasSpace = false;
    position = 0;
    length = 0;
    result = null;
    if ( url != null )
      append( url );
  }

  public final byte[] digest() {
    if ( result != null )
      return result;
    if ( position > 0 )
      flush();
    MurmurHash3_128.murmurhash3_x64_128_finalize( length, digest );
    result = digest.asBytes();
    return result;
  }

  private void append( final URI url ) {
    append( url.getHost() );
    appendChar( ' ' );
  }

  private void appendChar( final char c ) {
    if ( !Character.isWhitespace(c) && !Character.isDigit(c) ) {
      lastAppendedWasSpace = false;
      appendCharImpl( c );
    }
    else
    if ( !lastAppendedWasSpace ) {
      lastAppendedWasSpace = true;
      appendCharImpl( ' ' );
    }
  }

  private void appendCharImpl( final char c ) {
    buffer[ position++ ] = c;
    if ( position == DIGEST_BUFFER_SIZE )
      flush();
  }

  private void flush() {
    MurmurHash3_128.murmurhash3_x64_128_update( buffer, 0, position, digest );
    length += position;
    position = 0;
  }
}
