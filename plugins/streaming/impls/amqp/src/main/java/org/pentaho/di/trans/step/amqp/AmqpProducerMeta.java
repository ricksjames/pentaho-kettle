package org.pentaho.di.trans.step.amqp;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;

@Step( id = "AmqpProducer", image = "Amqp.svg", name = "AMQP Producer",
  description = "AMQP Producer Streaming Step", categoryDescription = "Streaming" )
@InjectionSupported( localizationPrefix = "FileStreamMeta.Injection." )
public class AmqpProducerMeta extends BaseStreamStepMeta implements StepMetaInterface, Cloneable {

  private static Class<?> PKG = AmqpProducer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  public static final String HOSTNAME = "hostname";
  public static final String EXCHANGE = "exchange";
  public static final String EXCHANGE_TYPE = "exchangeType";
  public static final String ROUTING_KEY = "routingKey";
  public static final String EXCHANGE_DURABLE = "exchangeDurable";
  public static final String EXCHANGE_EXCLUSIVE = "exchangeExclusive";
  public static final String EXCHANGE_AUTO_DELETE = "exchangeAutoDelete";
  public static final String QUEUE_DURABLE = "queueDurable";
  public static final String QUEUE_EXCLUSIVE = "queueExclusive";
  public static final String QUEUE_AUTO_DELETE = "queueAutoDelete";
  public static final String FIELD_TO_SEND = "fieldToSend";
  public static final String QUEUE = "queue";

  //use injection annotation to automagically get load/save behavior
  //example
  @Injection( name = HOSTNAME )
  public String hostname;

  @Injection( name = EXCHANGE )
  public String exchange;

  @Injection( name = EXCHANGE_TYPE )
  public String exchangeType;

  @Injection( name = ROUTING_KEY )
  public String routingKey;

  @Injection( name = EXCHANGE_DURABLE )
  public boolean exchangeDurable;

  @Injection( name = EXCHANGE_EXCLUSIVE )
  public boolean exchangeExclusive;

  @Injection( name = EXCHANGE_AUTO_DELETE )
  public boolean exchangeAutoDelete;

  @Injection( name = QUEUE )
  public String queue;

  @Injection( name = QUEUE_DURABLE )
  public boolean queueDurable;

  @Injection( name = QUEUE_EXCLUSIVE )
  public boolean queueExclusive;

  @Injection( name = QUEUE_AUTO_DELETE )
  public boolean queueAutoDelete;

  @Injection( name = FIELD_TO_SEND )
  private String fieldToSend = "";

  public AmqpProducerMeta() {
    super();
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    hostname = "127.0.0.1";
    exchange = null;
    exchangeType = "default";
    routingKey = "";
    exchangeDurable = true;
    exchangeExclusive = false;
    exchangeAutoDelete = false;
    queue = "";
    queueDurable = true;
    queueExclusive = false;
    queueAutoDelete = false;
  }

  @Override
  public RowMeta getRowMeta( String origin, VariableSpace space ) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "message" ) );
    return rowMeta;
  }


  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new AmqpProducer( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new AmqpProducerData();
  }

  public String getDialogClassName() {
    return "org.pentaho.di.trans.step.amqp.AmqpProducerDialog";
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  public String getExchange() {
    return exchange;
  }

  public void setExchange( String exchange ) {
    this.exchange = exchange;
  }

  public String getExchangeType() {
    return exchangeType;
  }

  public void setExchangeType( String exchangeType ) {
    this.exchangeType = exchangeType;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue( String queue ) {
    this.queue = queue;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public void setRoutingKey( String routingKey ) {
    this.routingKey = routingKey;
  }

  public boolean isExchangeDurable() {
    return exchangeDurable;
  }

  public void setExchangeDurable( boolean exchangeDurable ) {
    this.exchangeDurable = exchangeDurable;
  }

  public boolean isExchangeExclusive() {
    return exchangeExclusive;
  }

  public void setExchangeExclusive( boolean exchangeExclusive ) {
    this.exchangeExclusive = exchangeExclusive;
  }

  public boolean isExchangeAutoDelete() {
    return exchangeAutoDelete;
  }

  public void setExchangeAutoDelete( boolean exchangeAutoDelete ) {
    this.exchangeAutoDelete = exchangeAutoDelete;
  }

  public boolean isQueueDurable() {
    return queueDurable;
  }

  public void setQueueDurable( boolean queueDurable ) {
    this.queueDurable = queueDurable;
  }

  public boolean isQueueExclusive() {
    return queueExclusive;
  }

  public void setQueueExclusive( boolean queueExclusive ) {
    this.queueExclusive = queueExclusive;
  }

  public boolean isQueueAutoDelete() {
    return queueAutoDelete;
  }

  public void setQueueAutoDelete( boolean queueAutoDelete ) {
    this.queueAutoDelete = queueAutoDelete;
  }

  public String getFieldToSend() {
    return fieldToSend;
  }

  public void setFieldToSend( String fieldToSend ) {
    this.fieldToSend = fieldToSend;
  }
}
