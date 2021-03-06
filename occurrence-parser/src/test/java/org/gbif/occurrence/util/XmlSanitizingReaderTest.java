/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
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
package org.gbif.occurrence.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class XmlSanitizingReaderTest {

  private String doSingleReads(String test) throws IOException {
    StringReader reader = new StringReader(test);
    XmlSanitizingReader xmlReader = new XmlSanitizingReader(reader);

    StringBuilder sb = new StringBuilder();
    while (xmlReader.ready()) {
      int nextIntChar = xmlReader.read();
      char nextChar = (char) nextIntChar;
      if (nextIntChar != -1) sb.append(nextChar);
    }
    String result = sb.toString();

    // System.out.println("Single reads result [" + result + "]");
    return result;
  }

  private String doSimpleBufferRead(String test) throws IOException {
    StringReader reader = new StringReader(test);
    XmlSanitizingReader xmlReader = new XmlSanitizingReader(reader);

    StringBuilder sb = new StringBuilder();
    while (xmlReader.ready()) {
      char[] buffer = new char[12];
      xmlReader.read(buffer);
      sb.append(new String(buffer));
    }
    String result = sb.toString().trim();

    // System.out.println("Simple buffer read result [" + result + "]");
    return result;
  }

  private String doOffsetBufferRead(String test) throws IOException {
    StringReader reader = new StringReader(test);
    XmlSanitizingReader xmlReader = new XmlSanitizingReader(reader);

    StringBuilder sb = new StringBuilder();
    int totalCharsRead = 0;
    while (xmlReader.ready()) {
      char[] buffer = new char[12];
      int charsRead = xmlReader.read(buffer, 0, 12);
      totalCharsRead += charsRead;
      sb.append(new String(buffer));
    }
    String result = sb.toString().trim();

    // System.out.println("Offset buffer read result [" + result + "]");
    return result;
  }

  @Test
  public void testAsciiSingleReads() throws Exception {
    String test = "No bad chars and no funny chars.";
    String result = doSingleReads(test);
    assertTrue(result.equals(test));
  }

  @Test
  public void testAsciiSimpleBufferRead() throws Exception {
    String test = "No bad chars and no funny chars.";
    String result = doSimpleBufferRead(test);
    assertTrue(result.equals(test));
  }

  @Test
  public void testAsciiOffsetBufferRead() throws Exception {
    String test = "No bad chars and no funny chars.";
    String result = doOffsetBufferRead(test);
    assertTrue(result.equals(test));
  }

  @Test
  public void testUtf8SingleReads() throws Exception {
    String test = "No bad chars and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية";
    String result = doSingleReads(test);
    assertTrue(result.equals(test));
  }

  @Test
  public void testUtf8SimpleBufferRead() throws Exception {
    String test = "No bad chars and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية";
    String result = doSimpleBufferRead(test);
    assertTrue(result.equals(test));
  }

  @Test
  public void testUtf8OffsetBufferRead() throws Exception {
    String test = "No bad chars and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية";
    String result = doOffsetBufferRead(test);
    assertTrue(result.equals(test));
  }

  @Test
  public void testBadXmlSingleReads() throws Exception {
    char bad1 = 0xb;
    char bad2 = 0x7;
    char goodWeird = 0xa;
    String test =
      "Some bad chars " + bad1 + goodWeird + " and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية " + bad2 +
      "end";
    String goal = "Some bad chars " + goodWeird + " and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية end";
    String result = doSingleReads(test);
    assertTrue(result.equals(goal));
  }

  @Test
  public void testBadXmlSimpleBufferRead() throws Exception {
    char bad1 = 0xb;
    char bad2 = 0x7;
    char goodWeird = 0xa;
    String test =
      "Some bad chars " + bad1 + goodWeird + " and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية " + bad2 +
      "end";
    String goal = "Some bad chars " + goodWeird + " and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية end";
    String result = doSimpleBufferRead(test);
    assertTrue(result.equals(goal));
  }

  @Test
  public void testBadXmlOffsetBufferRead() throws Exception {
    char bad1 = 0xb;
    char bad2 = 0x7;
    char goodWeird = 0xa;
    String test =
      "Some bad chars " + bad1 + goodWeird + " and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية " + bad2 +
      "end";
    String goal = "Some bad chars " + goodWeird + " and some seriously funny chars: äåáàæœčéèêëïñøöüßšž北京العربية end";
    String result = doOffsetBufferRead(test);
    assertTrue(result.equals(goal));
  }

  @Test
  public void testBadXmlFileRead() throws Exception {
    String fileName = getClass().getResource("/responses/problematic/spanish_bad_xml.gz").getFile();
    File file = new File(fileName);
    FileInputStream fis = new FileInputStream(file);
    GZIPInputStream inputStream = new GZIPInputStream(fis);

    StringBuilder sb = new StringBuilder();
    XmlSanitizingReader xmlReader = new XmlSanitizingReader(new InputStreamReader(inputStream, "UTF-8"));
    while (xmlReader.ready()) {
      char[] buff = new char[8192];
      xmlReader.read(buff, 0, 8192);
      sb.append(buff);
    }
    assertEquals(6210, sb.toString().trim().length());
  }

  @Test
  public void testBadXmlFileReadWithBufferedReaderCharArray() throws Exception {
    String fileName = getClass().getResource("/responses/problematic/spanish_bad_xml.gz").getFile();
    File file = new File(fileName);
    FileInputStream fis = new FileInputStream(file);
    GZIPInputStream inputStream = new GZIPInputStream(fis);

    StringBuilder sb = new StringBuilder();
    BufferedReader xmlReader = new BufferedReader(new XmlSanitizingReader(new InputStreamReader(inputStream, "UTF-8")));
    while (xmlReader.ready()) {
      char[] buff = new char[8192];
      xmlReader.read(buff, 0, 8192);
      sb.append(buff);
    }
    assertEquals(6210, sb.toString().trim().length());
  }

  @Test
  public void testBadXmlFileReadWithBufferedReaderReadLines() throws Exception {
    String fileName = getClass().getResource("/responses/problematic/spanish_bad_xml.gz").getFile();
    File file = new File(fileName);
    FileInputStream fis = new FileInputStream(file);
    GZIPInputStream inputStream = new GZIPInputStream(fis);

    StringBuilder sb = new StringBuilder();
    BufferedReader buffReader =
      new BufferedReader(new XmlSanitizingReader(new InputStreamReader(inputStream, "UTF-8")));
    while (buffReader.ready()) {
      String line = buffReader.readLine();
      // System.out.println("adding line [" + line + "]");
      sb.append(line);
    }

    // System.out.println("\nFinal string output is:\n" + sb.toString());

    // drops newline chars vs chararray test, above
    assertEquals(6097, sb.toString().trim().length());
  }
}
