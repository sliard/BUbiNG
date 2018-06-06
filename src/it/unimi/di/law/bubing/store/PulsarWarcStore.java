package it.unimi.di.law.bubing.store;

/*		 
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.protobuf.WebPageTextContentProto;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.apache.pulsar.client.api.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


//RELEASE-STATUS: DIST

/** A {@link Store} implementation using the {@link it.unimi.di.law.warc} package. */

public class PulsarWarcStore implements Closeable, Store {
	private final static Logger LOGGER = LoggerFactory.getLogger( WarcStore.class );

	private final PulsarClient pulsarClient;
	private final Producer pulsarWARCProducer;
	private final Producer pulsarPlainTextProducer;
	private final ByteArrayOutputStream outputStream;
	private final WarcWriter warcWriter;
	public PulsarWarcStore(final RuntimeConfiguration rc ) throws IOException {
		ClientConfiguration conf = new ClientConfiguration();

		pulsarClient = PulsarClient.create(rc.pulsarClientConnection,conf);
		ProducerConfiguration producerConfig = new ProducerConfiguration();
		producerConfig.setBatchingEnabled(true);
		producerConfig.setBatchingMaxMessages(128);
		producerConfig.setBatchingMaxPublishDelay(100, TimeUnit.MILLISECONDS);
		producerConfig.setBlockIfQueueFull(true);
		producerConfig.setSendTimeout(30000, TimeUnit.MILLISECONDS);
		producerConfig.setCompressionType(CompressionType.LZ4);
		producerConfig.setProducerName(rc.name + UUID.randomUUID().toString());
		pulsarWARCProducer = pulsarClient.createProducer(rc.pulsarWARCTopic,  producerConfig);
		pulsarPlainTextProducer = pulsarClient.createProducer(rc.pulsarPlainTextTopic,  producerConfig);
		outputStream = new ByteArrayOutputStream();
		warcWriter = new UncompressedWarcWriter(outputStream);

	}

	@Override
	public void store(final URI uri,
					  final HttpResponse response,
					  final boolean isDuplicate,
					  final byte[] contentDigest,
					  final String guessedCharset,
					  final String guessedLanguage,
					  final Map<String,String> extraHeaders,
					  final StringBuilder textContent) throws IOException, InterruptedException {
		if ( contentDigest == null ) throw new NullPointerException( "Content digest is null" );
		LOGGER.debug("PulsarWarc:Store Uri = "+ uri.toString());
		final HttpResponseWarcRecord record = new HttpResponseWarcRecord( uri, response );
		HeaderGroup warcHeaders = record.getWarcHeaders();
		warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.WARC_PAYLOAD_DIGEST, "bubing:" + Hex.encodeHexString( contentDigest ) ) );
		if ( guessedCharset != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_CHARSET, guessedCharset ) );
		if ( guessedLanguage != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_LANGUAGE, guessedLanguage ) );

		for(String header: extraHeaders.keySet()) {
			warcHeaders.updateHeader(new BasicHeader(header, extraHeaders.get(header)));
		}
		if ( isDuplicate ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_IS_DUPLICATE, "true" ) );

		warcWriter.write( record );
		pulsarWARCProducer.sendAsync(outputStream.toByteArray());
		outputStream.reset();

		pulsarPlainTextProducer.sendAsync(WebPageTextContentProto.WebPage.newBuilder()
				.setUrl(uri.toString())
				.setContent(textContent.toString())
				.build()
				.toByteArray());
	}

	private void realClose() throws IOException {
		try {
			warcWriter.close();
		}
		catch ( IOException shouldntHappen ) {
			LOGGER.error( "Interrupted while closing parallel output stream" );
		}
		outputStream.close();
	}

	@Override
	public synchronized void close() throws IOException {
		realClose();
	}
}
