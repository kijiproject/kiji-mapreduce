/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.mapreduce;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/** Base interface for all reducers used by Kiji. */
@ApiAudience.Public
@Inheritance.Extensible
public interface KijiReducer {
  /**
   * Gets the type of the output keys from the reducer.
   *
   * @return The Java class of the reduce output keys.
   */
  Class<?> getOutputKeyClass();

  /**
   * Gets the type of the output values from the reducer.
   *
   * @return The Java class of the reduce output values.
   */
  Class<?> getOutputValueClass();
}
