package it.unimi.di.law.bubing.parser.html;

import net.htmlparser.jericho.EndTag;
import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.StartTag;
import net.htmlparser.jericho.StreamedSource;

import java.nio.CharBuffer;

import static net.htmlparser.jericho.HTMLElementName.*;


public final class JerichoHtmlBackup implements JerichoParser.Handler
{
  private static final int DEFAULT_INITIAL_CAPACITY = 128 * 1024;
  private final StringBuilder backup;
  private int skip;

  public JerichoHtmlBackup() {
    this( DEFAULT_INITIAL_CAPACITY );
  }

  public JerichoHtmlBackup( final int initialCapacity ) {
    this.backup = new StringBuilder( initialCapacity );
    this.skip = 0;
  }

  public JerichoHtmlBackup( final StringBuilder backup ) {
    this.backup = backup;
    this.skip = 0;
  }

  @Override
  public void startDocument( final StreamedSource source ) throws JerichoParser.ParseException {
    // nothing to do
  }

  @Override
  public void endDocument( final StreamedSource source ) throws JerichoParser.ParseException {
    // nothing to do
  }

  @Override
  public void process( final StreamedSource source, final Segment segment ) throws JerichoParser.ParseException {
    if ( segment instanceof StartTag ) {
      final StartTag startTag = (StartTag) segment;
      final String name = startTag.getName();
      if ( name == SCRIPT || name == STYLE || name == OPTION || name == SELECT ) { // FIXME: skip SELECT tag ?
        if ( !startTag.isSyntacticalEmptyElementTag() )
          skip += 1;
        return;
      }
      else
      if ( name == SPAN || name == FONT ) {
        return;
      }
    }
    else
    if ( segment instanceof EndTag ) {
      final EndTag endTag = (EndTag) segment;
      final String name = endTag.getName();
      if ( name == SCRIPT || name == STYLE || name == OPTION || name == SELECT ) {
        skip = Math.max( 0, skip-1 );
        return ;
      }
      else
      if ( name == SPAN || name == FONT ) {
        return;
      }
    }

    if ( skip == 0 ) {
      final CharBuffer buffer = source.getCurrentSegmentCharBuffer();
      backup.append( buffer.array(), buffer.position(), buffer.remaining() );
    }
  }

  public StringBuilder getBackup() {
    return backup;
  }
}
