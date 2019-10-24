package it.unimi.di.law.bubing.categories;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;

public class TextInfo
{
  float textQuality;
  int vocabSize;
  int textSize;
  boolean gotCategorization;
  MsgCrawler.Categorization categorization;
  boolean gotEmbedding;
  Float[] embedding;
  String lang;

  public TextInfo() {
    this.textQuality = 0.0f;
    this.vocabSize = 0;
    this.textSize = 0;
    this.gotCategorization = false;
    this.categorization = null;
    this.gotEmbedding = false;
    this.embedding = null;
    this.lang = null;
  }

  public void setCategorization(MsgCrawler.Categorization categorization) {
    this.categorization = categorization;
    this.gotCategorization = true;
  }
  public MsgCrawler.Categorization getCategorization() { return categorization; }
  public boolean gotCategorization() { return this.gotCategorization; }

  public void setEmbedding(Float[] embedding) {
    this.embedding = embedding;
    this.gotEmbedding = true;
  }
  public Float[] getEmbedding() { return embedding; }
  public boolean gotEmbedding() { return this.gotEmbedding; }

  public void setTextQuality(float textQuality) { this.textQuality = textQuality; }
  public float getTextQuality() { return textQuality; }

  public void setVocabSize(int vocabSize) { this.vocabSize = vocabSize; }
  public int getVocabSize() { return vocabSize; }

  public void setTextSize(int textSize) { this.textSize = textSize; }
  public int getTextSize() { return textSize; }

  public void setLang(String lang) { this.lang = lang; }
  public String getLang() { return this.lang; }
}
