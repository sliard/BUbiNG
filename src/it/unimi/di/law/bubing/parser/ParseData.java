package it.unimi.di.law.bubing.parser;

import java.net.URI;
import java.util.List;


public final class ParseData
{
  public final URI baseUri;
  public final String title;
  public final PageInfo pageInfo;
  public final byte[] digest;
  public final StringBuilder textContent;
  public final StringBuilder boilerpipedContent;
  public final StringBuilder rewritten;
  public final List<HTMLLink> links;

  public ParseData( final URI baseUri,
                    final String title,
                    final PageInfo pageInfo,
                    final byte[] digest,
                    final StringBuilder textContent,
                    final StringBuilder boilerpipedContent,
                    final StringBuilder rewritten,
                    final List<HTMLLink> links ) {
    this.baseUri = baseUri;
    this.title = title;
    this.pageInfo = pageInfo;
    this.digest = digest;
    this.textContent = textContent;
    this.boilerpipedContent = boilerpipedContent;
    this.rewritten = rewritten;
    this.links = links;
  }

  public String getCharsetName() {
    return pageInfo.getGuessedCharset() != null
      ? pageInfo.getGuessedCharset().name()
      : null;
  }

  public String getLanguageName() {
    return pageInfo.getGuessedLanguage() != null
      ? pageInfo.getGuessedLanguage().getLanguage()
      : null;
  }
}
