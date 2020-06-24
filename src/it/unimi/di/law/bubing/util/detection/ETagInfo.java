package it.unimi.di.law.bubing.util.detection;

import java.net.URI;


public class ETagInfo
{
  public String httpHeaderETag = null;
  public String htmlMetaETag = null;


  @Override
  public String toString() {
    return httpHeaderETag + "," + htmlMetaETag;
  }
}
