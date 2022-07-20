package it.unimi.di.law.bubing.store;

import it.unimi.di.law.bubing.RuntimeConfiguration;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.URI;
import java.util.Map;


public final class NullStore implements Store, Closeable
{
  private final static Logger LOGGER = LoggerFactory.getLogger( NullStore.class );

  public NullStore( final RuntimeConfiguration rc ) {
    LOGGER.warn( "using NullStore" );
  }

  @Override
  public final void store( final URI uri, final HttpResponse response, final boolean isDuplicate, final byte[] contentDigest,
                     final String guessedCharset, final String guessedLanguage,
                     final Map<String, String> extraHeaders, final StringBuilder textContent ) {
    // nothing
  }

  @Override
  public final void close() {
    // nothing
  }
}
