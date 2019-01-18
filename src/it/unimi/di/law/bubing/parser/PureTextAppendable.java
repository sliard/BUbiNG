package it.unimi.di.law.bubing.parser;


public final class PureTextAppendable implements Appendable
{
  private static final int BUFFER_SIZE = 64 * 1024;

  private final StringBuilder stringBuilder;
  private final char[] buffer;
  private int position;
  private boolean lastAppendedWasSpace;
  private boolean lastAppendedWasNewLine;

  public PureTextAppendable() {
    this.stringBuilder = new StringBuilder();
    this.buffer = new char[ BUFFER_SIZE ];
    this.position = 0;
    this.lastAppendedWasSpace = false;
    this.lastAppendedWasNewLine = false;
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

  public final void append( final char[] buffer, final int offset, final int length ) {
    for ( int i=offset; i<offset+length; ++i )
      appendChar( buffer[i] );
  }

  public final void flush() {
    stringBuilder.append( buffer, 0, position );
    position = 0;
  }

  public final void init() {
    stringBuilder.setLength( 0 );
    position = 0;
    lastAppendedWasSpace = false;
    lastAppendedWasNewLine = false;
  }

  public final StringBuilder getContent() {
    flush();
    return stringBuilder;
  }

  private void appendChar( final char c ) {
    if ( !Character.isWhitespace(c) ) {
      lastAppendedWasSpace = false;
      lastAppendedWasNewLine = false;
      appendCharImpl( c );
    }
    else
      appendWhiteSpace( c );
  }

  private void appendWhiteSpace( final char c ) {
    final int charType = Character.getType( c );
    if ( charType == Character.SPACE_SEPARATOR ) {
      if ( lastAppendedWasSpace ) return;
      lastAppendedWasSpace = true;
      appendCharImpl( ' ' );
    }
    else
    if ( charType == Character.LINE_SEPARATOR || charType == Character.PARAGRAPH_SEPARATOR || charType == Character.CONTROL ) {
      if ( lastAppendedWasNewLine ) return;
      lastAppendedWasSpace = true;
      lastAppendedWasNewLine = true;
      appendCharImpl( '\n' );
    }
  }

  private void appendCharImpl( final char c ) {
    buffer[ position++ ] = c;
    if ( position == BUFFER_SIZE )
      flush();
  }
}
