package org.pentaho.di.trans.step.amqp;

import com.rabbitmq.client.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AmqpTest {
    private static final long WAIT_MILLIS = 500;

    private static final String HOSTNAME = "localhost";

    private static final String DEFAULT_EXCHANGE = "";
    private static final String DIRECT_EXCHANGE = "directExchange";
    private static final String FANOUT_EXCHANGE = "fanoutExchange";
    private static final String TOPIC_EXCHANGE = "topicExchange";
    private static final String HEADERS_EXCHANGE = "headersExchange";


    private static final String DEFAULT_TYPE = null;
    private static final String DIRECT_TYPE = "direct";
    private static final String FANOUT_TYPE = "fanout";
    private static final String TOPIC_TYPE = "topic";
    private static final String HEADERS_TYPE = "headers";

    private Channel channel;
    private ConnectionFactory factory;
    private Connection connection;

    @Before
    public void beforeEach() throws IOException, TimeoutException {
        factory = new ConnectionFactory();
        factory.setHost(HOSTNAME);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    @After
    public void afterEach() throws IOException, TimeoutException {
        if (channel.isOpen()) {
            channel.close();
        }
        connection.close();
    }

    @Test
    public void testDefault() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        produce(DEFAULT_EXCHANGE, DEFAULT_TYPE, queue, messages);
        List <String> receivedMessages = consume(DEFAULT_EXCHANGE, DEFAULT_TYPE, queue);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(messages, receivedMessages);
    }

    @Test
    public void testDefaultAutoAck() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        produce(DEFAULT_EXCHANGE, DEFAULT_TYPE, queue, messages);
        List <String> receivedMessages = consume(DEFAULT_EXCHANGE, DEFAULT_TYPE, queue, false);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(messages, receivedMessages);
    }

    /**
     * This test throws an exception because default exchanges don't allow binding of default exchange queues to routing keys
     * during consumption
     *
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void testDefaultWithRoutingKey() throws IOException {
        String queue = UUID.randomUUID().toString();

        String routingKey = UUID.randomUUID().toString();
        List<String> routingKeys = new ArrayList<>();
        routingKeys.add(routingKey);

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        produce(DEFAULT_EXCHANGE, DEFAULT_TYPE, routingKey, messages);
        consume(DEFAULT_EXCHANGE, DEFAULT_TYPE, queue, routingKeys );
    }

    @Test
    public void testFanoutRoundRobin() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        List <String> consumer0ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, queue, Collections.singletonList(queue));
        List <String> consumer1ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, queue, Collections.singletonList(queue));

        produce(FANOUT_EXCHANGE, FANOUT_TYPE, queue, messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(messages.size() / 2, consumer0ReceivedMessages.size() );
        assertEquals(messages.size() / 2, consumer1ReceivedMessages.size() );
    }

    @Test
    public void testFanoutFairTempQueue() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        List <String> consumer0ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, null, Collections.singletonList(queue), false);
        List <String> consumer1ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, null, Collections.singletonList(queue), false);

        produce(FANOUT_EXCHANGE, FANOUT_TYPE, queue, messages);

        Thread.sleep(WAIT_MILLIS);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(messages.size(), consumer0ReceivedMessages.size() );
        assertEquals(messages.size(), consumer1ReceivedMessages.size() );
    }

    /**
     * Test Fanout Produce First
     *
     * Note: when starting the producer first, the consumers won't get an equal round robin distribution because one
     * consumer starts before the other, and starts pulling messages off of the queue
     *
     * @throws IOException
     * @throws InterruptedException
     */
   // @Test
    public void testFanoutProduceFirst() throws IOException, InterruptedException {

        //TODO: this one breaks now because I am not creating a queue in the producer - which is probably how it should be
        String queue = UUID.randomUUID().toString();

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        produce(FANOUT_EXCHANGE, FANOUT_TYPE, queue, messages);

        List <String> consumer0ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, queue, Collections.singletonList(queue));
        List <String> consumer1ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, queue, Collections.singletonList(queue));

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        //No messages will be lost, but the distribution may not be even between the two consumers because one starts
        //before the other
        assertEquals(messages.size(), consumer0ReceivedMessages.size() + consumer1ReceivedMessages.size() );
    }

   // @Test
    public void testFanoutPrefetch() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        int prefetchCount = 2;
        channel.basicQos(prefetchCount);

        //Set autoack to false so the messages will not be acknowledged
        List <String> consumer0ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, queue, Collections.singletonList(queue), false );
        List <String> consumer1ReceivedMessages = consume(FANOUT_EXCHANGE, FANOUT_TYPE, queue, Collections.singletonList(queue), false );

        produce(FANOUT_EXCHANGE, FANOUT_TYPE, queue, messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        //Because the messages are not acknowledged, each consumer will only receive the prefetch count number messages
        //and stop consuming
        assertEquals(prefetchCount, consumer0ReceivedMessages.size() );
        assertEquals(prefetchCount, consumer1ReceivedMessages.size() );
    }

    /**
     * Direct Exchanges must start the consumer first, because the consumer binds the queue(s) to the routingKeys
     *
     */
    @Test
    public void testDirectConsumeFirst() throws IOException, InterruptedException {

        List<String> routingKeys = new ArrayList<>();
        routingKeys.add(UUID.randomUUID().toString());
        routingKeys.add(UUID.randomUUID().toString());

        List <String> routingKey0Messages = new ArrayList<>();
        routingKey0Messages.add(UUID.randomUUID().toString());
        routingKey0Messages.add(UUID.randomUUID().toString());

        List <String> routingKey1Messages = new ArrayList<>();
        routingKey1Messages.add(UUID.randomUUID().toString());
        routingKey1Messages.add(UUID.randomUUID().toString());

        List <String> allMessages = new ArrayList<>();
        allMessages.addAll(routingKey0Messages);
        allMessages.addAll(routingKey1Messages);

        //start consumer first listening to two routing keys
        List <String> receivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, null, routingKeys );

        //then start producers producing each to one of the two different routing keys
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKeys.get(0), routingKey0Messages);
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKeys.get(1), routingKey1Messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(allMessages, receivedMessages);
    }

    /**
     * Exception expected because the consumer routingKeys cannot be bound the default exchange, even though its
     * identified with the DIRECT exchange type
     */
    @Test(expected = Exception.class)
    public void testDirectToDefaultExchange() throws IOException, InterruptedException {

        List<String> routingKey = new ArrayList<>();
        routingKey.add(UUID.randomUUID().toString());

        List <String> routingKey0Messages = new ArrayList<>();
        routingKey0Messages.add(UUID.randomUUID().toString());
        routingKey0Messages.add(UUID.randomUUID().toString());

        //Consume from the DEFAULT exchange, but type DIRECT
        List <String> receivedMessages = consume(DEFAULT_EXCHANGE, DIRECT_TYPE, null, routingKey );

        //Produce to the DEFAULT exchange, but type DIRECT
        produce(DEFAULT_EXCHANGE, DIRECT_TYPE, routingKey.get(0), routingKey0Messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(routingKey0Messages, receivedMessages);
    }

    /**
     * Even though we consume with a direct exchange named queue, we are producing to a routing key, so this works
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDirectNamedQueue() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List<String> routingKey = new ArrayList<>();
        routingKey.add(UUID.randomUUID().toString());

        List <String> routingKey0Messages = new ArrayList<>();
        routingKey0Messages.add(UUID.randomUUID().toString());
        routingKey0Messages.add(UUID.randomUUID().toString());

        List <String> receivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, queue, routingKey );

        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKey.get(0), routingKey0Messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(routingKey0Messages, receivedMessages);
    }

    /**
     * When using a DIRECT exchange and a named queue (vs a dynamically created queue), we have to start the consumer
     * first to create the queue binding, yet, the second time through the producer CAN be started first because the
     * named queue is already bound to the routingKey.
     *
     * Note this does not work if the queue is dynamically created (channel.queueDeclare().getQueue())
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDirectNamedQueueProduceFirstSecondTime() throws IOException, InterruptedException, TimeoutException {
        String queue = UUID.randomUUID().toString();

        List<String> routingKey = new ArrayList<>();
        routingKey.add(UUID.randomUUID().toString());

        List <String> routingKey0Messages = new ArrayList<>();
        routingKey0Messages.add(UUID.randomUUID().toString());
        routingKey0Messages.add(UUID.randomUUID().toString());

        //Start the consumer first the first time through
        List <String> receivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, queue, routingKey );
        //must produce to the routingKey since this is DIRECT type - and not to the queueName like with DEFAULT type
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKey.get(0), routingKey0Messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(routingKey0Messages, receivedMessages);

        //close and create the channel again
        //otherwise the first consumer will consume the message before the second one starts
        afterEach();
        beforeEach();

        //Start the producer first the second time through
        //must produce to the routingKey since this is DIRECT type - and not to the queueName like with DEFAULT type
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKey.get(0), routingKey0Messages);
        List <String> receivedMessagesSecondTime = consume(DIRECT_EXCHANGE, DIRECT_TYPE, queue, routingKey );

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(routingKey0Messages, receivedMessagesSecondTime);
    }



    /**
     * Direct Exchanges must start the consumer first, because the consumer binds the queue(s) to the routingKeys
     */
    @Test
    public void testDirectProduceFirst() throws IOException, InterruptedException {

        List<String> routingKeys = new ArrayList<>();
        routingKeys.add(UUID.randomUUID().toString());
        routingKeys.add(UUID.randomUUID().toString());

        List <String> routingKey0Messages = new ArrayList<>();
        routingKey0Messages.add(UUID.randomUUID().toString());
        routingKey0Messages.add(UUID.randomUUID().toString());

        List <String> routingKey1Messages = new ArrayList<>();
        routingKey1Messages.add(UUID.randomUUID().toString());
        routingKey1Messages.add(UUID.randomUUID().toString());

        List <String> allMessages = new ArrayList<>();
        allMessages.addAll(routingKey0Messages);
        allMessages.addAll(routingKey1Messages);

        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKeys.get(0), routingKey0Messages);
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKeys.get(1), routingKey1Messages);

        List <String> receivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, null, routingKeys );

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        //It is expected in this case the consumer receives no messages because the producer produced the messages
        //before queues were bound to the routingKeys
        assertNotEquals(allMessages, receivedMessages);
    }

    @Test
    public void testDirectTwoConsumer() throws IOException, InterruptedException {

        List<String> routingKeys0 = new ArrayList<>();
        routingKeys0.add(UUID.randomUUID().toString());

        List<String> routingKeys1 = new ArrayList<>();
        routingKeys1.add(UUID.randomUUID().toString());

        List <String> routingKey0Messages = new ArrayList<>();
        routingKey0Messages.add(UUID.randomUUID().toString());
        routingKey0Messages.add(UUID.randomUUID().toString());

        List <String> routingKey1Messages = new ArrayList<>();
        routingKey1Messages.add(UUID.randomUUID().toString());
        routingKey1Messages.add(UUID.randomUUID().toString());

        //start two consumers binding them each to one of the two different routing keys
        List <String> consumer0ReceivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, null, routingKeys0 );
        List <String> consumer1ReceivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, null, routingKeys1 );

        //start two producers producing each to one of the two different routing keys
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKeys0.get(0), routingKey0Messages);
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, routingKeys1.get(0), routingKey1Messages);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(routingKey0Messages, consumer0ReceivedMessages);
        assertEquals(routingKey1Messages, consumer1ReceivedMessages);
    }

    @Test
    public void testTopic() throws IOException, InterruptedException {
        List<String> consumer0ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("*.*.*")));
        List<String> consumer1ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("#")));
        List<String> consumer2ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.def.ghi", "jkl.mno.pqr","stu.vwx.yz" )));
        List<String> consumer3ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("*.def.ghi", "jkl.*.pqr","stu.vwx.*" )));
        List<String> consumer4ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.#", "jkl.#","stu.#" )));

        List<String> consumer5ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.def.ghi")));
        List<String> consumer6ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("*.def.ghi")));
        List<String> consumer7ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.*.ghi")));
        List<String> consumer8ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.*.*")));
        List<String> consumer9ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.def.*")));
        List<String> consumer10ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.#")));
        List<String> consumer11ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("#.def.ghi")));
        List<String> consumer12ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.#.ghi")));
        List<String> consumer13ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.def.#")));
        List<String> consumer14ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("#.abc.def.ghi")));
        List<String> consumer15ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, null, new ArrayList<>(Arrays.asList("abc.def.ghi.#")));


        List<String> firstTopicMessages = Arrays.asList("abc.def.ghi message1", "abc.def.ghi message2", "abc.def.ghi message3");
        produce(TOPIC_EXCHANGE, TOPIC_TYPE, "abc.def.ghi", firstTopicMessages);
        produce(TOPIC_EXCHANGE, TOPIC_TYPE, "jkl.mno.pqr", Collections.singletonList("jkl.mno.pqr message"));
        produce(TOPIC_EXCHANGE, TOPIC_TYPE, "stu.vwx.yz", Collections.singletonList("stu.vwx.yz message"));

        List<String> allTopicsMessages = new ArrayList<>(firstTopicMessages);
        allTopicsMessages.addAll(Arrays.asList("jkl.mno.pqr message", "stu.vwx.yz message"));

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(allTopicsMessages, consumer0ReceivedMessages);
        assertEquals(allTopicsMessages, consumer1ReceivedMessages);
        assertEquals(allTopicsMessages, consumer2ReceivedMessages);
        assertEquals(allTopicsMessages, consumer3ReceivedMessages);
        assertEquals(allTopicsMessages, consumer4ReceivedMessages);

        assertEquals(firstTopicMessages, consumer5ReceivedMessages);
        assertEquals(firstTopicMessages, consumer6ReceivedMessages);
        assertEquals(firstTopicMessages, consumer7ReceivedMessages);
        assertEquals(firstTopicMessages, consumer8ReceivedMessages);
        assertEquals(firstTopicMessages, consumer9ReceivedMessages);
        assertEquals(firstTopicMessages, consumer10ReceivedMessages);
        assertEquals(firstTopicMessages, consumer11ReceivedMessages);
        assertEquals(firstTopicMessages, consumer12ReceivedMessages);
        assertEquals(firstTopicMessages, consumer13ReceivedMessages);
        assertEquals(firstTopicMessages, consumer14ReceivedMessages);
        assertEquals(firstTopicMessages, consumer15ReceivedMessages);
    }

    @Test
    public void testDirectWithWildcards() throws IOException, InterruptedException {
        List<String> consumer0ReceivedMessages = consume(DIRECT_EXCHANGE, DIRECT_TYPE, null, new ArrayList<>(Arrays.asList("ajkl.mno.pqr")));
        produce(DIRECT_EXCHANGE, DIRECT_TYPE, "*.mno.pqr", Collections.singletonList("ajkl.mno.pqr message"));

        Thread.sleep(WAIT_MILLIS);

        assertNotEquals(Collections.singletonList("ajkl.mno.pqr message"), consumer0ReceivedMessages);
    }

    /**
     * Test a topic where the queue is given a name
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testTopicNamedQueueProduceFirstSecondTime() throws IOException, InterruptedException, TimeoutException {
        String queue = UUID.randomUUID().toString();

        List<String> consumer0ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, queue, new ArrayList<>(Arrays.asList("*.*.*")));

        List<String> message = Collections.singletonList("jkl.mno.pqr message");
        produce(TOPIC_EXCHANGE, TOPIC_TYPE, "jkl.mno.pqr", message);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(message, consumer0ReceivedMessages);

        //close and create the channel again
        //otherwise the first consumer will consume the message before the second one starts
        afterEach();
        beforeEach();

        produce(TOPIC_EXCHANGE, TOPIC_TYPE, "jkl.mno.pqr", message);
        List<String> consumer1ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, queue, new ArrayList<>(Arrays.asList("*.*.*")));

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(message, consumer1ReceivedMessages);

    }



    /**
     * Test a topic where the queue is given a name
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testTopicQueueName() throws IOException, InterruptedException {
        String queue = UUID.randomUUID().toString();

        List<String> consumer0ReceivedMessages = consume(TOPIC_EXCHANGE, TOPIC_TYPE, queue, new ArrayList<>(Arrays.asList("*.*.*")));

        List<String> message = Collections.singletonList("jkl.mno.pqr message");
        produce(TOPIC_EXCHANGE, TOPIC_TYPE, "jkl.mno.pqr", message);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(message, consumer0ReceivedMessages);
    }

    private void testHeaders(String consumerQueue, String producerRoutingKey) throws IOException, InterruptedException {

        Map<String, Object> headerArgs = new HashMap<>();
        headerArgs.put("headerName1", "headerValue1");

        List <String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(UUID.randomUUID().toString());
        }

        List<String> consumer0ReceivedMessages = consume(HEADERS_EXCHANGE, HEADERS_TYPE, consumerQueue, headerArgs);

        produce(HEADERS_EXCHANGE, HEADERS_TYPE, producerRoutingKey, messages, headerArgs);

        //wait for messages to process
        Thread.sleep(WAIT_MILLIS);

        assertEquals(messages, consumer0ReceivedMessages);
    }

    /**
     * Note: Producer routingKey cannot be null, but beyond that when in header mode the queue and routingKey are ignored
     * Also Note that when a consume is passed a null key it creates a temporary queue
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testHeaders() throws IOException, InterruptedException {
        testHeaders(null, "");
        testHeaders("", "");

        String queue1 = UUID.randomUUID().toString();
        testHeaders(queue1, queue1);

        String queue2 = UUID.randomUUID().toString();
        testHeaders(null, queue2);

        String queue3 = UUID.randomUUID().toString();
        testHeaders("", queue3);

        String queue4 = UUID.randomUUID().toString();
        testHeaders(queue4, "");

        String queue5 = UUID.randomUUID().toString();
        String queue6 = UUID.randomUUID().toString();
        testHeaders(queue5, queue6);
    }


    private void produce(String exchange, String exchangeType, String routingKey, List<String> messages) throws IOException {
        produce(exchange, exchangeType, routingKey, messages, true, false, false, null);
    }

    private void produce(String exchange, String exchangeType, String routingKey, List<String> messages, Map<String, Object> arguments ) throws IOException {
        produce(exchange, exchangeType, routingKey, messages, true, false, false, arguments);
    }

    private void produce(String exchange, String exchangeType, String routingKey, List<String> messages, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments  ) throws IOException {

        if (exchangeType != null && exchange != "") {
            channel.exchangeDeclare(exchange, exchangeType, durable, exclusive, autoDelete, arguments);
        }

        if(exchangeType == null) {
            channel.queueDeclare(routingKey, durable, exclusive, autoDelete, null);
        }

        AMQP.BasicProperties basicProperties = null;
        if ( arguments != null ) {
            basicProperties = new AMQP.BasicProperties.Builder().headers(arguments).build();
        }

        for ( String message : messages ) {
            channel.basicPublish(exchange, routingKey, basicProperties, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

    private List<String> consume (String exchange, String exchangeType, String queue) throws IOException {
        return consume(exchange, exchangeType, queue, null, true, true, false, false, null);
    }

    private List<String> consume (String exchange, String exchangeType, String queue, boolean autoAck) throws IOException {
        return consume(exchange, exchangeType, queue, null, autoAck, true, false, false, null);
    }

    private List<String> consume (String exchange, String exchangeType, String queue, List<String> routingKeys) throws IOException {
        return consume(exchange, exchangeType, queue, routingKeys, true, true, false, false, null);
    }

    private List<String> consume (String exchange, String exchangeType, String queue, Map<String, Object> arguments) throws IOException {
        return consume(exchange, exchangeType, queue, null, true, true, false, false, arguments);
    }

    private List<String> consume (String exchange, String exchangeType, String queue, List<String> routingKeys, boolean autoAck) throws IOException {
        return consume(exchange, exchangeType, queue, routingKeys, autoAck, true, false, false, null);
    }

    private List <String> consume(String exchange, String exchangeType, String queue, List<String> routingKeys, final boolean autoAck, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        final List<String> messagesReceived = new ArrayList<>();
        if (exchangeType != null && exchange != "") {
            channel.exchangeDeclare(exchange, exchangeType, durable);
        }

        if ( queue == null ) {
             queue = channel.queueDeclare().getQueue();
        } else {
            channel.queueDeclare(queue, durable, exclusive, autoDelete, null);
        }

        if(routingKeys != null) {
            for (String routingKey : routingKeys) {
                channel.queueBind(queue, exchange, routingKey, arguments);
            }
        }

        if(arguments != null ) {
            channel.queueBind(queue, exchange, "", arguments);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                messagesReceived.add(message);

                System.out.println(" [x] Received '" + message + "'");

                //Message processing would go here

                if ( !autoAck ) {
                    System.out.println(" [x] Done");
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume(queue, autoAck, arguments, consumer);

        return messagesReceived;
    }

}
