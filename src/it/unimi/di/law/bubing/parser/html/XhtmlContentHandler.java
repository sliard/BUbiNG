/*
 * Copyright (c) 2013-2023 eXenSa.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.unimi.di.law.bubing.parser.html;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import static net.htmlparser.jericho.HTMLElementName.*;


public class XhtmlContentHandler implements ContentHandler
{
  private final ContentHandler handler;
  private final Metadata metadata;
  private final LinksHandler linksHandlerOpt;
  private final StringBuilder title;
  private int titleLevel;
  private int bodyLevel;

  public XhtmlContentHandler( final Metadata metadata, final ContentHandler handler, final LinksHandler linksHandlerOpt ) {
    this.metadata = metadata;
    this.handler = handler;
    this.linksHandlerOpt = linksHandlerOpt;
    this.title = new StringBuilder();
    this.titleLevel = 0;
    this.bodyLevel = 0;
  }

  public XhtmlContentHandler( final ContentHandler handler, final LinksHandler linksHandlerOpt ) {
    this( new Metadata(), handler, linksHandlerOpt );
  }

  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public void setDocumentLocator( Locator locator ) {
    handler.setDocumentLocator( locator );
  }

  @Override
  public void startDocument() throws SAXException {
    handler.startDocument();
  }

  @Override
  public void endDocument() throws SAXException {
    handler.endDocument();
  }

  @Override
  public void startPrefixMapping( String prefix, String uri ) throws SAXException {
    handler.startPrefixMapping( prefix, uri );
  }

  @Override
  public void endPrefixMapping( String prefix ) throws SAXException {
    handler.endPrefixMapping( prefix );
  }

  @Override
  public void startElement( String uri, String localName, String qName, Attributes atts ) throws SAXException {
    if ( localName == TITLE || titleLevel > 0 ) {
      titleLevel += 1;
      title.setLength( 0 );
    }
    else
    if ( localName == BODY /*|| tagName == FRAMESET*/ || bodyLevel > 0 )
      bodyLevel += 1;
    else
    if ( /*bodyLevel == 0 &&*/ localName == META )
      startTagMeta( localName, atts );

    if ( linksHandlerOpt != null )
      linksHandlerOpt.startTag( localName, atts );

    handler.startElement( uri, localName, qName, atts );
  }

  @Override
  public void endElement( String uri, String localName, String qName ) throws SAXException {
    if ( bodyLevel > 0 )
      bodyLevel -= 1;

    if ( linksHandlerOpt != null )
      linksHandlerOpt.endTag( localName );

    handler.endElement( uri, localName, qName );

    if ( titleLevel > 0 ) {
      titleLevel -= 1;
      if ( titleLevel == 0 && metadata.get("title") == null )
        metadata.set( "title", title.toString().trim() );
    }
  }

  @Override
  public void characters( char[] ch, int start, int length ) throws SAXException {
    if ( titleLevel > 0 && bodyLevel == 0 )
      title.append( ch, start, length );

    if ( linksHandlerOpt != null )
      linksHandlerOpt.characters( ch, start, length );

    handler.characters( ch, start, length );
  }

  @Override
  public void ignorableWhitespace( char[] ch, int start, int length ) throws SAXException {
    if ( linksHandlerOpt != null )
      linksHandlerOpt.ignorableWhitespace( ch, start, length );

    handler.ignorableWhitespace( ch, start, length );
  }

  @Override
  public void processingInstruction( String target, String data ) throws SAXException {
    handler.processingInstruction( target, data );
  }

  @Override
  public void skippedEntity( String name ) throws SAXException {
    handler.skippedEntity( name );
  }

  private void startTagMeta( final String name, final Attributes attributes ) {
    boolean dummy =
      tryMetaWithContent( attributes, attributes.getValue("content") ) ||
      tryMetaCharset( attributes );
  }

  private boolean tryMetaWithContent( final Attributes attributes, final String content ) {
    return content != null && (
      tryAddMetadata( attributes, "http-equiv", content ) ||
      tryAddMetadata( attributes, "name", content ) ||
      tryAddMetadata( attributes, "property", content )
    );
  }

  private boolean tryMetaCharset( final Attributes attributes ) {
    final String charset = attributes.getValue( "charset" );
    if ( charset == null ) return false;
    metadata.add( "charset", charset );
    return true;
  }

  private boolean tryAddMetadata( final Attributes attributes, final String name, final String content ) {
    final String key = attributes.getValue( name );
    if ( key == null ) return false;
    metadata.add( key, content );
    return true;
  }

  public static final class Metadata
  {
    private final Object2ObjectOpenHashMap<String,String[]> map;

    public Metadata() {
      this.map = new Object2ObjectOpenHashMap<>();
    }

    public void set( final String key, final String value ) {
      if ( value == null )
        map.remove( key );
      else
        map.put( key, new String[]{value} );
    }

    public void add( final String key, final String value ) {
      if ( value != null ) {
        final String[] existing = map.get( key );
        if ( existing  == null )
          map.put( key, new String[]{value} );
        else
          addExisting( key, value, existing );
      }
    }

    public String get( final String key ) {
      final String[] values = map.get( key );
      return values == null ? null : values[0];
    }

    public String[] getValues( final String key ) {
      return map.get( key );
    }

    public void clear() {
      map.clear();
    }

    private void addExisting( final String key, final String value, final String[] existing ) {
      final String[] values = new String[ existing.length+1 ];
      System.arraycopy( existing, 0, values, 0, existing.length );
      values[ existing.length ] = value;
      map.put( key, values );
    }
  }
}
