package it.unimi.di.law.bubing.categories;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import it.unimi.di.law.bubing.RuntimeConfiguration;

public class NullClassifier implements TextClassifier
{
  public NullClassifier(RuntimeConfiguration rc) {}

  public MsgCrawler.Categorization predict(String textContent, String lang, float threshold) {
    return null;
  }

  public void close() { }
}
