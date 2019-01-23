package it.unimi.di.law.bubing.parser;

import net.htmlparser.jericho.HTMLElementName;


public class HTMLLink
{
  public final String type;
  public final String uri;
  public final String title;
  public final String text;
  public final String rel;

  public HTMLLink( final String type, final String uri, final String title, final String text, final String rel ) {
    this.type = type;
    this.uri = uri;
    this.title = title;
    this.text = text;
    this.rel = rel;
  }

  public static final class Type
  {
    public static final String A = HTMLElementName.A;
    public static final String IMG = HTMLElementName.IMG;
    public static final String LINK = HTMLElementName.LINK;
    public static final String SCRIPT = HTMLElementName.SCRIPT;
    public static final String EMBED = HTMLElementName.EMBED;
    public static final String IFRAME = HTMLElementName.IFRAME;
    public static final String FRAME = HTMLElementName.FRAME;
    public static final String REDIRECT = "redirect";
  }
}

