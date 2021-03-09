package it.unimi.di.law.bubing;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.stringparsers.IntSizeStringParser;

import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.Helpers;

public class RuntimeConfigurationTest {

	@Test
	public void testOptional() throws ConfigurationException, IllegalArgumentException, ClassNotFoundException, NoSuchFieldException, SecurityException, ParseException {
		final StartupConfiguration startupConf = Helpers.getTestStartupConfiguration(this, null);
		assertEquals(((Integer)IntSizeStringParser.getParser().parse(StartupConfiguration.class.getDeclaredField("dnsCacheMaxSize").getAnnotation(StartupConfiguration.OptionalSpecification.class).value())).intValue(), startupConf.dnsCacheMaxSize);
	}

	@Test
	public void testParseTime() {
		assertEquals(3500, StartupConfiguration.parseTime("  3500"));
		assertEquals(3500000000L, StartupConfiguration.parseTime("3500000000"));
		assertEquals(50000L, StartupConfiguration.parseTime("50 s"));
		assertEquals(5000L, StartupConfiguration.parseTime("5s"));
		assertEquals(3, StartupConfiguration.parseTime("3ms"));
		assertEquals(3 * 60 * 60 * 1000 + 4 * 1000, StartupConfiguration.parseTime("3h 4s"));
		assertEquals(3L * 24 * 60 * 60 * 1000, StartupConfiguration.parseTime(" 3   d   "));
		assertEquals((long)(1.5 * 24 * 60 * 60 * 1000 + 5.5 * 60 * 1000), StartupConfiguration.parseTime("1.5d 5.5m"));
		assertEquals(3 * 60 * 60 * 1000 + 4 * 1000 + 5, StartupConfiguration.parseTime("3h 4s 5   ms "));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseTimeWithExc1() {
		assertEquals(3L * 24 * 60 * 60 * 1000, StartupConfiguration.parseTime("3h 4d"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseTimeWithExc2() {
		assertEquals(3L * 24 * 60 * 60 * 1000, StartupConfiguration.parseTime("3h 4u"));
	}

	@Test
	public void testMatcher() {
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0.0.0.0").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("192.0.2.235").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0300.0000.0002.0353").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("3221226219").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("030000001353").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("2001:0db8:0000:0000:0000:ff00:0042:8329").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("2001:db8:0:0:0:ff00:42:8329").matches());

		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC00002EB").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC0.0x00.0x02.0xEB").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC0.00.02.0xEB").matches());

		assertFalse(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC0.00.02.EB").matches());
	}

}
