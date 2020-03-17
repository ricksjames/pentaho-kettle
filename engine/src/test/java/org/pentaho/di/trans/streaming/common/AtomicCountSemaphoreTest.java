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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class AtomicCountSemaphoreTest {

  private static final boolean DEBUG_ON = false;

  private AtomicInteger count = new AtomicInteger( 0 );
  private AtomicCountSemaphore semaphore;
  private LinkedBlockingDeque<Integer> deque = new LinkedBlockingDeque();

  private long threadTimeoutSeconds;

  @Before
  public void setUp() {
    threadTimeoutSeconds = 10;
  }

  @Test
  public void smallTest() {
    int totalItems = 50;
    assertFalse( threadTest( 3, 10, totalItems, 1, 50, 50 ) );
    assertEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() );
  }

  @Test
  public void heavyThreadTest() {
    int totalItems = 100000;
    assertFalse( threadTest( 100, 1000, totalItems, 10, 1, 1 ) );
    assertEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() );
  }

  /**
   * This test will timeout
   *
   * The following is recommended or it could be possible to get a timeout because there is no prioritization of which
   * threads get the items:
   *
   * semaphoreSize > (threadCount * releaseSize)
   *
   */
  @Test
  public void heavyThreadTimeout() {
    threadTimeoutSeconds = 4;
    int totalItems = 100000;
    assertTrue( threadTest( 128, 15, totalItems, 10, 1, 1 ) );
    assertNotEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() ); //deque is empty because all the items have been spread across consumers
  }

  @Test
  public void releaseSizeEqualToSemaphoreTimeout() {
    threadTimeoutSeconds = 4;
    int totalItems = 1010;
    assertTrue( threadTest( 4, 10, totalItems, 10, 1, 1 ) );
    assertNotEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() ); //deque is empty because all the items have been spread across consumers
  }

  @Test
  public void slowConsumerHeavyThread() {
    int totalItems = 11000;
    assertFalse( threadTest( 128, 10000, totalItems, 10, 0, 100 ) );
    assertEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() );
  }

  @Test
  public void slowProducerHeavyThread() {
    int totalItems = 11000;
    assertFalse( threadTest( 128, 10000, totalItems, 10, 2, 0 ) );
    assertEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() );
  }

  @Test
  public void smallSemaphoreAndReleaseSize() {
    int totalItems = 200000;
    assertFalse( threadTest( 10, 10, totalItems, 1, 0, 0 ) );
    assertEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() );
  }

  @Test
  public void smallSemaphoreAndReleaseSizeAndThreads() {
    int totalItems = 100000;
    assertFalse( threadTest( 4, 3, totalItems, 1, 0, 0 ) );
    assertEquals( totalItems, count.get() );
    assertEquals( 0, deque.size() );
  }

  private boolean threadTest( int threadCount, int semaphoreSize, int totalItems, int releaseSize, int producerMaxDelay,
                              int consumerMaxDelay ) {
    final ExecutorService executorService = Executors.newFixedThreadPool( threadCount );
    semaphore = new AtomicCountSemaphore( semaphoreSize );
    final List<Future<?>> futures = new ArrayList<>();

    //AtomicCountSemaphore only supports a single acquire thread
    futures.add( executorService.submit( () -> randomWaitAndAcquire( totalItems, producerMaxDelay ) ) );

    //Release with multiple thread
    IntStream.range( 0, ( totalItems / releaseSize ) )
      .forEach( i -> futures
        .add( executorService.submit( () -> randomWaitAndRelease( releaseSize, consumerMaxDelay ) ) ) );

    //Wait until all threads complete
    final boolean timedOut = futures.stream().map( this::safeWaitForCompletion ).anyMatch( n -> !n );

    return timedOut;
  }

  private void randomWaitAndRelease( int releaseSize, int maxWaitMillis ) {
    for ( int i = 0; i < releaseSize; i++ ) {
      try {
        Thread.sleep( (long) ( Math.random() * maxWaitMillis ) );
        //This blocks ensuring that release is not called more times than acquire
        deque.take();
      } catch ( InterruptedException e ) {
        fail();
      }
    }
    //Release in batches of release size
    final int release = semaphore.release( releaseSize );
    debugPrint( "Release - available permits after = " + release );
  }

  private void randomWaitAndAcquire( int totalItems, int maxWaitMillis ) {
    for ( int i = 0; i < totalItems; i++ ) {
      try {
        Thread.sleep( (long) ( Math.random() * maxWaitMillis ) );
        final int acquire = semaphore.acquire();
        debugPrint( "Acquire - available permits = " + acquire );
        this.count.incrementAndGet();
        debugPrint( "Item count = " + this.count.get() );
        deque.put( i );
      } catch ( InterruptedException e ) {
        fail();
      }
    }
  }

  /**
   * @param future
   * @return false if timed out, otherwise true
   */
  private boolean safeWaitForCompletion( Future future ) {
    try {
      future.get( threadTimeoutSeconds, TimeUnit.SECONDS );
    } catch ( TimeoutException te ) {
      debugPrint( "Thread timed out." );
      return false;
    } catch ( InterruptedException | ExecutionException e ) {
      fail();
    }
    return true;
  }

  private void debugPrint( String output ) {
    if ( DEBUG_ON ) {
      System.out.println( output );
    }
  }
}
