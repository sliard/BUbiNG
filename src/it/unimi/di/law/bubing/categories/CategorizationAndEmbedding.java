package it.unimi.di.law.bubing.categories;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;

public interface CategorizationAndEmbedding {
  public Float[] getEmbedding();
  public MsgCrawler.Categorization getCategorization();
}
