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
/*
package it.unimi.di.law.bubing.parser.html;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;


public class NoOpDecoder extends CharsetDecoder
{
  public NoOpDecoder() {
    super( Charset.defaultCharset(), 1.0f, 1.0f );
  }


  @Override
  protected CoderResult decodeLoop( ByteBuffer in, CharBuffer out ) {
    int pos = in.position();
    try {
      while ( in.hasRemaining() ) {
        final byte b = in.get();
        final char c = b >= 0 ? (char)b : (char)(b+256);
        if ( !out.hasRemaining() )
          return CoderResult.OVERFLOW;
        out.put( c );
        pos += 1;
      }
      return CoderResult.UNDERFLOW;
    }
    finally {
      in.position( pos );
    }
  }
}
*/
