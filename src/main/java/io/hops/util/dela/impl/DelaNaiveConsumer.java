/*
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

package io.hops.util.dela.impl;

import io.hops.util.dela.DelaConsumer;
import io.hops.util.DelaHelper;

/**
 *
 */
public class DelaNaiveConsumer {

  public static void main(final String[] args) throws Exception {

    int pId = 1;
    String topicN = "1_test_test_record";
    String brokerE = "10.0.2.15:9091";
    String restE = "http://10.0.2.15:8080";
    String keyS = "/tmp/usercerts/demo/keystore.jks";
    String trustS = "/tmp/usercerts/demo/truststore.jks";
    String keysP = "adminpw";
    String trustsP = "adminpw";
    DelaConsumer consumer = DelaHelper.getHopsConsumer(pId, topicN, brokerE, restE, keyS, trustS, keysP, trustsP);
    consumer.consume();
    while (true) {
      Thread.sleep(1000);
    }
  }
}
