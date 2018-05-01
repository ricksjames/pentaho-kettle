package org.pentaho.di.trans.step.amqp;

import com.google.common.collect.ImmutableList;
import com.rabbitmq.client.*;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;

/**
 * A simple example implementation of StreamSource which streams rows from a specified file to an iterable.
 * <p>
 * Note that this class is strictly meant as an example and not intended for real use. It uses a simplistic strategy of
 * leaving a BufferedReader open in order to load rows as they come in, without real consideration of error conditions.
 */
public class AmqpSource extends BlockingQueueStreamSource<List<Object>> {

  private static Class<?> PKG = AmqpSource.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private final AmqpConsumerMeta streamMeta;
  private final AmqpConsumer amqpConsumer;

  private LogChannelInterface logChannel = new LogChannel( this );
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private Future<?> future;

  private Connection connection;
  private Channel channel;

  //TODO: implement header args
  Map<String, Object> arguments = null;

  public AmqpSource(AmqpConsumerMeta streamMeta, AmqpConsumer streamStep ) {
    super( streamStep );
    this.streamMeta = streamMeta;
    this.amqpConsumer = streamStep;
  }


  @Override public void open() {
    if ( future != null ) {
      logChannel.logError( "open() called more than once" );
      return;
    }

    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(streamMeta.getHostname());
      connection = factory.newConnection();
      channel = connection.createChannel();


      logChannel.logDebug("streamMeta.getExchange(): " + streamMeta.getExchange());
      logChannel.logDebug("streamMeta.getExchangeType(): " + streamMeta.getExchangeType());
      logChannel.logDebug("streamMeta.getQueue():" + streamMeta.getQueue());

      boolean defaultExchangeType = "default".equals(streamMeta.getExchangeType());

      if (!defaultExchangeType) {
        channel.exchangeDeclare(streamMeta.getExchange(), streamMeta.getExchangeType(), streamMeta.isExchangeDurable(), streamMeta.isExchangeExclusive(),streamMeta.isExchangeAutoDelete(), null);
      }

      //TODO: handle temporary queues
      channel.queueDeclare(streamMeta.getQueue(), streamMeta.isQueueDurable(), streamMeta.isQueueExclusive(), streamMeta.isQueueAutoDelete(), null);

      if (!defaultExchangeType) {
        if (streamMeta.getRoutingKeys() != null) {
          for (String routingKey : streamMeta.getRoutingKeys()) {

            logChannel.logDebug("streamMeta.routingKeys():" + routingKey);
            channel.queueBind(streamMeta.getQueue(), streamMeta.getExchange(), routingKey, arguments);
          }
        }
      }

      if(arguments != null ) {
        channel.queueBind(streamMeta.getQueue(), streamMeta.getExchange(), "", arguments);
      }

    } catch ( IOException | TimeoutException e ) {
      logChannel.logError("Exception while setting up consumer...");
      amqpConsumer.logError( e.toString() );
      amqpConsumer.stopAll();

    }

    future = executorService.submit( this::readLoop );

  }

  @Override public void close() {

    future.cancel( true );

    try {
      if (channel.isOpen()) {
        channel.close();
      }
      if(connection.isOpen()) {
        connection.close();
      }

    } catch (IOException | TimeoutException e) {
      logChannel.logError("Error Closing connection");
    }
    super.close();


  }

  private void readLoop() {
      //TODO: implement autoAck
      boolean autoAck = true;

      logChannel.logDebug( "Waiting for messages. ");
      Consumer consumer = new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
          String message = new String(body, Charset.defaultCharset());

          acceptRows( singletonList(ImmutableList.of(message)));


          logChannel.logDebug( "Received Message:" + message);

          if ( !autoAck ) {
            System.out.println(" [x] Done");
            channel.basicAck(envelope.getDeliveryTag(), false);
          }
        }
      };

      try {
        channel.basicConsume(streamMeta.getQueue(), autoAck, arguments, consumer);
      } catch (IOException e) {
        logChannel.logDebug( "Error with basic consumer" + e.getMessage());
      }

  }

}
