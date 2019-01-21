package it.unimi.di.law.bubing.categories;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import java.io.Closeable;
import java.io.IOException;

/** A classifier for text content. */
public interface TextClassifier extends Closeable
{
	/** Classify the text content of a page
	 *
	 * @return A set of topic which match the text content along with a normalized score.
	 */
	MsgCrawler.Categorization predict(String textContent, String lang);

	void close() throws IOException;
}
