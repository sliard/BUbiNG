package it.unimi.di.law.bubing.parser.html;

import net.htmlparser.jericho.Logger;
import net.htmlparser.jericho.LoggerProvider;

import java.util.regex.Pattern;


/**
 * This class is used to count errors and warnings from Jericho parser.
 */
public final class JerichoLoggerWrapper implements Logger
{
  private static final Pattern TAG_REJECTED_PATTERN = Pattern.compile( "rejected", Pattern.CASE_INSENSITIVE );
  private static final Pattern TAG_NOT_REGISTERED_PATTERN = Pattern.compile( "does not match a registered", Pattern.CASE_INSENSITIVE );

  private final Logger logger;
  private int errorCount;
  private int tagRejected;
  private int tagNotRegistered;
  private int warnCount;

  public JerichoLoggerWrapper( final Logger logger ) {
    this.logger = logger != null ? logger : LoggerProvider.DISABLED.getSourceLogger();
    this.errorCount = 0;
    this.warnCount = 0;
  }

  public int getErrorCount() {
    return errorCount;
  }

  public int getTagRejected() {
    return tagRejected;
  }

  public int getTagNotRegistered() {
    return tagNotRegistered;
  }

  public int getWarnCount() {
    return warnCount;
  }

  @Override
  public void error( final String s ) {
    errorCount += 1;
    if ( TAG_REJECTED_PATTERN.matcher(s).find() )
      tagRejected += 1;
    if ( TAG_NOT_REGISTERED_PATTERN.matcher(s).find() )
      tagNotRegistered += 1;
    if ( logger.isErrorEnabled() )
      logger.error( s );
  }

  @Override
  public void warn( final String s ) {
    warnCount += 1;
    if ( logger.isErrorEnabled() )
      logger.warn( s );
  }

  @Override
  public void info( final String s ) {
    logger.info( s );
  }

  @Override
  public void debug( final String s ) {
    logger.debug( s );
  }

  @Override
  public boolean isErrorEnabled() {
    return true;
  }

  @Override
  public boolean isWarnEnabled() {
    return true;
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }
}
