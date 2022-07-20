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


public class WhiteCharsCleaner
{
  private static final Segment WHITESPACE_SEGMENT = new Segment( new char[] {' '}, 0, 1 );

  private static boolean isWhiteSpace( final char c) {
    return net.htmlparser.jericho.Segment.isWhiteSpace(c);
  }

  public static class Segment
  {
    private Segment( final char[] array, final int offset, final int length ) {
      this.array = array;
      this.offset = offset;
      this.length = length;
    }

    public final char[] array;
    public final int offset;
    public final int length;
  }

  public final class Iterator
  {
    private Iterator( final char[] chars, final int offset, final int length ) {
      this.chars = chars;
      this.position = offset;
      this.length = offset+length;
      this.segment = length == 0 ? null : prefetch();
    }

    public final boolean hasNext() {
      return segment != null;
    }

    public final Segment next() {
      final Segment next = segment;
      segment = prefetch();
      return next;
    }

    private Segment prefetch() {
      lastIsWhite |= !start && position < length && isWhiteSpace(chars[position]);
      skipWhites();

      if ( position == length )
        return null;
      start = false;

      if ( lastIsWhite ) {
        lastIsWhite = false;
        return WHITESPACE_SEGMENT;
      }

      final int start = position;
      skipNonWhite();
      lastIsWhite = position < length;
      return new Segment( chars, start, position-start );
    }

    private void skipWhites() {
      while ( position < length && isWhiteSpace(chars[position]) )
        position += 1;
    }

    private void skipNonWhite() {
      while ( position < length && !isWhiteSpace(chars[position]) )
        position += 1;
    }

    private final char[] chars;
    private final int length;
    private int position;
    private Segment segment;
  }

  public WhiteCharsCleaner() {
    this.lastIsWhite = false;
    this.start = true;
  }

  public Iterator getSegments( final char[] chars, final int offset, final int length ) {
    return new Iterator( chars, offset, length );
  }

  private boolean lastIsWhite;
  private boolean start;
}
