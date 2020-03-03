/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Flowable;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.processors.ReplayProcessor;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.streaming.api.StreamSource;
import org.reactivestreams.Subscription;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.pentaho.di.i18n.BaseMessages.getString;

/**
 * Implementation of StreamSource which handles pause/resume logic, as well as creation of .rows() which generates a
 * blocking iterable.
 * <p>
 * Child classes should implement {@link #open()} to connect to a datasource.  The child .open implementation will
 * typically start a new thread that feeds rows of data to the {@link #acceptRows(List)} method. Any resource cleanup
 * should be included in a .close() implementation, along with a call to super.close() to complete the blocking
 * iterable.
 */
public abstract class BlockingQueueStreamSource<T> implements StreamSource<T> {


  private static final Class<?> PKG = BlockingQueueStreamSource.class;


  private final AtomicBoolean paused = new AtomicBoolean( false );

  private final PublishProcessor<T> publishProcessor = PublishProcessor.create();

  private LinkedBlockingDeque<T> blockingDeque;

  protected final BaseStreamStep streamStep;

  // binary semaphore used to block acceptance of rows when paused
  @VisibleForTesting Semaphore acceptingRowsSemaphore = new Semaphore( 1 );
  @VisibleForTesting LogChannel logChannel = new LogChannel( this );

  protected BlockingQueueStreamSource( BaseStreamStep streamStep ) {
    this.streamStep = streamStep;
    //TODO: this value doesn't have to be one, but 1 seems to work just fine, since rX has buffers also
    this.blockingDeque = new LinkedBlockingDeque<>( 1 );

    publishProcessor.onSubscribe( new Subscription() {
      @Override public void request( long l ) {
        Thread thread = new Thread( () -> processMessages() );
        thread.start();
      }

      @Override public void cancel() {
      }
    } );
  }

  private void processMessages() {
    try {
      T take = blockingDeque.take();

      //TODO: note that even though this gets called from the onSubscribe, the subscribers aren't registered yet
      //TODO: On my laptop this usually only takes about 60 milliseconds to be ready, but messages can still be lost
      //TODO: since we can't get around this sleep anyway, its probably simpler to not implement onSubscribe
      //TODO: and just put the sleep in the acceptRows method, removing the need for the deque and the extra thread also
      while ( !publishProcessor.hasSubscribers() ) {
        logChannel.logBasic( "Waiting for subscribers" );
        Thread.sleep( 10 );
      }
      while ( publishProcessor.hasSubscribers() && !publishProcessor.hasComplete() ) {
        publishProcessor.onNext( take );
        take = blockingDeque.take();
      }
    } catch ( InterruptedException e ) {
      logChannel.logError( e.getMessage() );
    }
  }


  @Override public Flowable<T> flowable() {
    return Flowable
      .fromPublisher( publishProcessor );
  }

  @Override public void close() {
    if ( !publishProcessor.hasComplete() ) {

      publishProcessor.onComplete();
    }
  }

  /**
   * Marks the source paused (if not already) and acquires the permit, which will cause acceptRows to block.
   */
  @Override public synchronized void pause() {
    if ( !paused.getAndSet( true ) ) {
      try {
        assert acceptingRowsSemaphore.availablePermits() == 1;
        acceptingRowsSemaphore.acquire();
      } catch ( InterruptedException e ) {
        logChannel.logError( getString( PKG, "BlockingQueueStream.PauseInterrupt" ) );
      }
    }
  }

  @Override public synchronized void resume() {
    if ( paused.getAndSet( false ) ) {
      assert acceptingRowsSemaphore.availablePermits() == 0;
      acceptingRowsSemaphore.release();
    }
  }


  /**
   * Accept rows, blocking if currently paused.
   * <p>
   * Implementations should implement the open() function to pass external row events to the acceptRows method.
   * <p>
   */
  protected void acceptRows( List<T> rows ) {
    try {
      acceptingRowsSemaphore.acquire();
      rows.forEach( ( row ) -> {
        try {
          this.streamStep.subtransExecutor.acquireBufferPermit();
          blockingDeque.put( row );
        } catch ( InterruptedException e ) {
          logChannel.logError( e.getMessage() );
        }
        streamStep.incrementLinesInput();
      } );
    } catch ( InterruptedException e ) {
      logChannel.logError(
        getString( PKG, "BlockingQueueStream.AcceptRowsInterrupt",
          Arrays.toString( rows.toArray() ) ) );
    } finally {
      acceptingRowsSemaphore.release();
    }
  }

  /**
   * Child implementations of this class can call .error() when an unexpected event occurs while passing rows to the
   * acceptRows() method.  For example, if an implementation includes a poll loop which retrieves data from a message
   * queue and passes chunks of rows to .acceptRows, an connection failure to the message queue should be handled by
   * calling error() with the connection exception. This will make sure that any consumers of the rows() iterable will
   * receive that error.
   */
  public void error( Throwable throwable ) {
    publishProcessor.onError( throwable );
  }

  protected Object readBytes( byte[] bytes ) {
    if ( streamStep.getVariablizedStepMeta().getMessageDataType() == ValueMetaInterface.TYPE_STRING ) {
      return new String( bytes, StandardCharsets.UTF_8 );
    } else {
      return bytes;
    }
  }
}
