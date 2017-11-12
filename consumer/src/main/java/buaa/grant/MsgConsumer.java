package buaa.grant;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * Created by grant on 2016/12/4.
 */
public class MsgConsumer implements MessageListener,Runnable{
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PTPReceiver.class);
    //连接账号
    private String userName = "grant";
    //连接密码
    private String password = "grant@123";

    private static final String SUBJECT = "test-activemq-queue";

    private int size = 2;

    //connection的工厂
    private ConnectionFactory factory;
    //连接对象
    private Connection connection;
    //一个操作会话
    private Session session;
    //目的地，其实就是连接到哪个队列，如果是点对点，那么它的实现是Queue，如果是订阅模式，那它的实现是Topic
    private Destination destination;
    //消费者，就是接收数据的对象
    private MessageConsumer consumer;

    private String tcpURI = "tcp://localhost:61616";
    private String udpURI = "failover:(udp://localhost:61618)";

    public void start(){
        System.out.print("PerMsg size:"+getSize()+"\n");
        try {
            //根据用户名，密码，url创建一个连接工厂
            //factory = new ActiveMQConnectionFactory(userName, password, ActiveMQConnection.DEFAULT_BROKER_URL);
            //factory = new ActiveMQConnectionFactory(userName, password, tcpURI);
            factory = new ActiveMQConnectionFactory(userName, password, udpURI);
            try{
                //从工厂中获取一个连接
                connection = factory.createConnection();
                //测试过这个步骤不写也是可以的，但是网上的各个文档都写了
                connection.start();
            }catch(JMSException e){
                e.printStackTrace();
            }

            //创建一个session
            //第一个参数:是否支持事务，如果为true，则会忽略第二个参数，被jms服务器设置为SESSION_TRANSACTED
            //第二个参数为false时，paramB的值可为Session.AUTO_ACKNOWLEDGE，Session.CLIENT_ACKNOWLEDGE，DUPS_OK_ACKNOWLEDGE其中一个。
            //Session.AUTO_ACKNOWLEDGE为自动确认，客户端发送和接收消息不需要做额外的工作。哪怕是接收端发生异常，也会被当作正常发送成功。
            //Session.CLIENT_ACKNOWLEDGE为客户端确认。客户端接收到消息后，必须调用javax.jms.Message的acknowledge方法。jms服务器才会当作发送成功，并删除消息。
            //DUPS_OK_ACKNOWLEDGE允许副本的确认模式。一旦接收方应用程序的方法调用从处理消息处返回，会话对象就会确认消息的接收；而且允许重复确认。
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            //创建一个到达的目的地，其实想一下就知道了，activemq不可能同时只能跑一个队列吧，这里就是连接了一个名为"text-msg"的队列，这个会话将会到这个队列，当然，如果这个队列不存在，将会被创建
            destination = session.createQueue(SUBJECT);
            //根据session，创建一个接收者对象
            consumer = session.createConsumer(destination);
            consumer.setMessageListener(this);

            //关闭接收端，也不会终止程序哦
           /* consumer.close();
            connection.close();*/
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * */
    String createTextMessage(byte[] bytes){
            String messageString = new String(bytes);

        return messageString;
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage){
                //获取到接收的数据
                String text = ((TextMessage)message).getText();
                System.out.println(text);
                logger.debug("TextMessage");
            }else if(message instanceof BytesMessage){
                //logger.debug("BytesMessage");
                BytesMessage bytesMessage = (BytesMessage) message;
                long bodylength = bytesMessage.getBodyLength();
                //logger.debug("MessageBody length:"+bodylength);
                byte[] bytes = new byte[(int)bodylength];
                int readlength = bytesMessage.readBytes(bytes);
                System.out.print("readLegth:"+readlength+"\n");
                if (readlength > 0){
                    String string = createTextMessage(bytes);
                    //System.out.print("messageString:"+string+"\n");
                }
            }else{
                logger.debug("Error");
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Thread currentThread = Thread.currentThread();
        logger.debug("Start thread "+currentThread.getId());
        start();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int msgSize){
        size = msgSize;
    }
}
