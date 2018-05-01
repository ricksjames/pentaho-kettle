package org.pentaho.di.trans.step.amqp;

import com.google.common.base.Preconditions;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;


/**
 * An example step plugin for purposes of demonstrating a strategy for handling streams of data.
 */
public class AmqpConsumer extends BaseStreamStep implements StepInterface {

  private static Class<?> PKG = AmqpConsumer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$


  public AmqpConsumer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    boolean init = super.init( stepMetaInterface, stepDataInterface );
    Preconditions.checkNotNull( stepMetaInterface );
    AmqpConsumerMeta streamMeta = (AmqpConsumerMeta) stepMetaInterface;

    window = new FixedTimeStreamWindow<>( subtransExecutor, streamMeta.getRowMeta( getStepname(), this ), getDuration(), getBatchSize() );

    source = new AmqpSource( streamMeta, this );

    return init;
  }
}
