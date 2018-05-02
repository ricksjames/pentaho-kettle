package org.pentaho.di.trans.step.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;


import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.Charset.defaultCharset;


/**
 * An example step plugin for purposes of demonstrating a strategy for handling streams of data.
 */
public class AmqpProducer extends BaseStep implements StepInterface {

  private static Class<?> PKG = AmqpProducer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private AmqpProducerMeta meta;

  //AMQP specific
  private Channel channel;
  private AMQP.BasicProperties basicProperties = null;

  public AmqpProducer( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    super.init( stepMetaInterface, stepDataInterface );

    meta = (AmqpProducerMeta) stepMetaInterface;
    return true;
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (AmqpProducerMeta) smi;
    Object[] row = getRow();

    if ( null == row ) {
      setOutputDone();
      return false;  // indicates done
    }

    //TODO: create arguments meta
    Map<String, Object> arguments = null;

    int messageIndex = 0;

    boolean defaultExchangeType = "default".equals( meta.getExchangeType() );

    String routingKey;
    String exchange;
    if ( defaultExchangeType ) {
      routingKey = meta.getQueue();
      exchange = "";
    } else {
      routingKey = meta.getRoutingKey();
      exchange = meta.getExchange();
    }

    if ( first ) {

      messageIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getFieldToSend() ) );

      try {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( meta.getHostname() );
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        if ( defaultExchangeType ) {
          channel
            .queueDeclare( meta.getQueue(), meta.isQueueDurable(), meta.isQueueExclusive(), meta.isQueueAutoDelete(),
              null );
        } else {
          channel
            .exchangeDeclare( exchange, meta.getExchangeType(), meta.isExchangeDurable(), meta.isExchangeExclusive(),
              meta.isExchangeAutoDelete(), null );
        }

        if ( arguments != null ) {
          basicProperties = new AMQP.BasicProperties.Builder().headers( arguments ).build();
        }

      } catch ( IOException | TimeoutException e ) {
        stopAll();
        logError( e.toString() );
        return false;
      }
      first = false;
    }

    try {
      final byte[] messageBytes = ( row[ messageIndex ] ).toString().getBytes( defaultCharset() );
      channel.basicPublish( exchange, routingKey, basicProperties, messageBytes );

      incrementLinesOutput();
      putRow( getInputRowMeta(), row ); // copy row to possible alternate rowset(s).


    } catch ( IOException e ) {
      logError( e.getMessage() );
      setErrors( 1 );
      stopAll();
    }
    return true;
  }

  //TODO: disconnect from rabbitmq after finished producing, or stopped.
}
