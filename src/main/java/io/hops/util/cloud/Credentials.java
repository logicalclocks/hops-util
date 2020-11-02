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
package io.hops.util.cloud;

public class Credentials {
  private String accessKeyId;
  private String secretAccessKey;
  private String sessionToken;
  
  public Credentials() {
  }
  
  public Credentials(String accessKeyId, String secretAccessKey, String sessionToken) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
  }
  
  public String getAccessKeyId() {
    return accessKeyId;
  }
  
  public void setAccessKeyId(String accessKeyId) {
    this.accessKeyId = accessKeyId;
  }
  
  public String getSecretAccessKey() {
    return secretAccessKey;
  }
  
  public void setSecretAccessKey(String secretAccessKey) {
    this.secretAccessKey = secretAccessKey;
  }
  
  public String getSessionToken() {
    return sessionToken;
  }
  
  public void setSessionToken(String sessionToken) {
    this.sessionToken = sessionToken;
  }
  
  @Override
  public String toString() {
    return "Credentials{" +
      "accessKeyId='" + accessKeyId + '\'' +
      ", secretAccessKey='" + secretAccessKey + '\'' +
      ", sessionToken='" + sessionToken + '\'' +
      '}';
  }
}
