package it.unimi.di.law.bubing.parser;

public final class PureTextAppendable implements Appendable, CharSequence {
  /**
   * True iff the last character appended was a space.
   */
  protected boolean lastAppendedWasSpace;
  protected boolean lastAppendedWasNewLine;

  protected final StringBuilder textContent;

  public PureTextAppendable() {
    textContent = new StringBuilder();
    lastAppendedWasSpace = true;
    lastAppendedWasNewLine = true;
  }

  /**
   * Initializes the digest computation.
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
    char[] chars = csq.toString().toCharArray();
    for (char c : chars)
      append(c);
    return this;
  }

  public void append( final char[] buffer, final int offset, final int length ) {
    for ( int i=offset; i<offset+length; ++i ) {
      final char c = buffer[i];
      if ( Character.isWhitespace(c) )
        appendWhiteSpace( Character.getType(c) );
      else {
        textContent.append( c );
        lastAppendedWasSpace = false;
        lastAppendedWasNewLine = false;
      }
    }
  }

  private void appendWhiteSpace( final int charType ) {
    if ( charType == Character.SPACE_SEPARATOR ) {
      if ( lastAppendedWasSpace ) return;
      textContent.append( ' ' );
      lastAppendedWasSpace = true;
    }
    else
    if ( charType == Character.LINE_SEPARATOR || charType == Character.PARAGRAPH_SEPARATOR || charType == Character.CONTROL ) {
      if ( lastAppendedWasNewLine ) return;
      textContent.append( '\n' );
      lastAppendedWasSpace = true;
      lastAppendedWasNewLine = true;
    }
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
