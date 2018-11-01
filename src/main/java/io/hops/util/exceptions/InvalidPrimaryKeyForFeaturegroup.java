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

package io.hops.util.exceptions;

/**
 * Exception thrown when an invalid primary key for creation of a new featuregroup was provided by the user
 * 
 */
public class InvalidPrimaryKeyForFeaturegroup extends Exception {

  Integer status;

  public InvalidPrimaryKeyForFeaturegroup(String message) {
    super(message);
  }

  public InvalidPrimaryKeyForFeaturegroup(Integer status, String message) {
    super(message);
    this.status = status;
  }

  public Integer getStatus() {
    return status;
  }

  public void setStatus(Integer status) {
    this.status = status;
  }
}
