package it.unimi.di.law.bubing.categories;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import javafx.util.Pair;

public class NullClassifier implements TextClassifier
{
  public NullClassifier(RuntimeConfiguration rc) {}

  public MsgCrawler.Categorization predict(String textContent, String lang) {
    return null;
  }

  public TextInfo predictTokenizedInfo(String[] textContent, TextInfo tinfo) {
    return null;
  }

  public void close() { }
}
