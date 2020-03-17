/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.di.trans.streaming.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Atomic Count Semaphore
 * <br>
 * This class implements just a subset of the functionality of {@link Semaphore} but provides faster performance. Also,
 * {@link Semaphore#release(int)} in testing was found not to always be thread safe.
 *
 * Note that this implementation has a very narrow usage scope with the following restrictions:
 * 1. Acquire may only be called by one thread, otherwise more permits can be acquired than permitCount
 * 2. Release should not be called more times than acquire; release is not bounded by zero and permitCount can go negative.
 */
public class AtomicCountSemaphore {

  private Semaphore semaphore = new Semaphore( 0 );
  private AtomicInteger permitCount;

  public AtomicCountSemaphore( int count ) {
    this.permitCount = new AtomicInteger( count );
  }

  /**
   * Release
   *
   * @param count
   * @return the permit count after the release
   */
  public int release( int count ) {
    //release the semaphore if the count is more than 0 and the permit count was zero before the add
    int afterAddCount = 0;
    if ( count > 0 ) {
      if ( ( afterAddCount = permitCount.getAndAdd( count ) ) == 0 ) {
        this.semaphore.release();
      }
      afterAddCount = afterAddCount + count;
    } else {
      afterAddCount = permitCount.get();
    }
    return afterAddCount;
  }

  /**
   * Acquire
   *
   * @return the permit count after the acquire
   * @throws InterruptedException
   */
  public int acquire() throws InterruptedException {
    int afterDecrementCount;
    if ( ( afterDecrementCount = permitCount.decrementAndGet() ) == 0 ) {
      this.semaphore.acquire();
    }
    return afterDecrementCount;
  }
}
