package org.pentaho.di.trans.step.amqp;

import org.pentaho.di.core.ObjectLocationSpecificationMethod;
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

import java.util.ArrayList;
import java.util.List;

@Step( id = "AmqpConsumer", image = "Amqp.svg", name = "AMQP Consumer",
  description = "AMQP Consumer Streaming Step", categoryDescription = "Streaming" )
@InjectionSupported( localizationPrefix = "FileStreamMeta.Injection." )
public class AmqpConsumerMeta extends BaseStreamStepMeta implements StepMetaInterface, Cloneable {

  private static Class<?> PKG = AmqpConsumerMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public static final String HOSTNAME = "hostname";
  public static final String EXCHANGE = "exchange";
  public static final String EXCHANGE_TYPE = "exchangeType";
  public static final String ROUTING_KEYS = "routingKeys";
  public static final String EXCHANGE_DURABLE = "exchangeDurable";
  public static final String EXCHANGE_EXCLUSIVE = "exchangeExclusive";
  public static final String EXCHANGE_AUTO_DELETE = "exchangeAutoDelete";
  public static final String QUEUE_DURABLE = "queueDurable";
  public static final String QUEUE_EXCLUSIVE = "queueExclusive";
  public static final String QUEUE_AUTO_DELETE = "queueAutoDelete";
  public static final String QUEUE = "queue";

  //use injection annotation to automagically get load/save behavior
  //example
  @Injection( name = HOSTNAME)
  public String hostname;

  @Injection( name = EXCHANGE )
  public String exchange;

  @Injection( name = EXCHANGE_TYPE )
  public String exchangeType;

  @Injection( name = ROUTING_KEYS )
  public List<String> routingKeys = new ArrayList<>();

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

  public AmqpConsumerMeta() {
    super();
    //TODO: not sure what this is for but get a null pointer without it - research
    setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    //todo: set defaults for all fields
  }

  @Override
  public RowMeta getRowMeta( String origin, VariableSpace space ) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "line" ) );
    return rowMeta;
  }


  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new AmqpConsumer( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new AmqpConsumerData();
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getExchange() {
    return exchange;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public String getExchangeType() {
    return exchangeType;
  }

  public void setExchangeType(String exchangeType) {
    this.exchangeType = exchangeType;
  }

  public List<String> getRoutingKeys() {
    return routingKeys;
  }

  public void setRoutingKeys(List<String> routingKeys) {
    this.routingKeys = routingKeys;
  }

  public boolean isExchangeDurable() {
    return exchangeDurable;
  }

  public void setExchangeDurable(boolean exchangeDurable) {
    this.exchangeDurable = exchangeDurable;
  }

  public boolean isExchangeExclusive() {
    return exchangeExclusive;
  }

  public void setExchangeExclusive(boolean exchangeExclusive) {
    this.exchangeExclusive = exchangeExclusive;
  }

  public boolean isExchangeAutoDelete() {
    return exchangeAutoDelete;
  }

  public void setExchangeAutoDelete(boolean exchangeAutoDelete) {
    this.exchangeAutoDelete = exchangeAutoDelete;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public boolean isQueueDurable() {
    return queueDurable;
  }

  public void setQueueDurable(boolean queueDurable) {
    this.queueDurable = queueDurable;
  }

  public boolean isQueueExclusive() {
    return queueExclusive;
  }

  public void setQueueExclusive(boolean queueExclusive) {
    this.queueExclusive = queueExclusive;
  }

  public boolean isQueueAutoDelete() {
    return queueAutoDelete;
  }

  public void setQueueAutoDelete(boolean queueAutoDelete) {
    this.queueAutoDelete = queueAutoDelete;
  }



  public String getDialogClassName() {
    return "org.pentaho.di.trans.step.amqp.AmqpConsumerDialog";
  }

}
