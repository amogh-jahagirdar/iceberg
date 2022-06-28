/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestInMemoryInputFile {
  @Test
  public void testReadAfterClose() throws IOException {
    InMemoryInputFile inputFile = new InMemoryInputFile("abc".getBytes(StandardCharsets.ISO_8859_1));
    InputStream inputStream = inputFile.newStream();
    Assert.assertEquals('a', inputStream.read());
    inputStream.close();
    Assertions.assertThatThrownBy(inputStream::read)
        .hasMessage("Stream is closed");
  }
}
