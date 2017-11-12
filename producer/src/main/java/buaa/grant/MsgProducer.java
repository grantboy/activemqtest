package buaa.grant;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.LoggerFactory;

import javax.jms.*;


/**
 * Created by grant on 2016/12/4.
 */
public class MsgProducer implements Runnable{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PTPSender.class);
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
    //生产者，就是产生数据的对象
    private MessageProducer producer;

    private String tcpURI = "tcp://localhost:61616";
    private String udpURI = "udp://localhost:61618";
    private String vmUdpURI = "udp://127.0.0.1:61618";

    public void start(){
        System.out.print("PerMsg size:"+getSize()+"\n");
        try {
            //根据用户名，密码，url创建一个连接工厂
            //factory = new ActiveMQConnectionFactory(userName, password, ActiveMQConnection.DEFAULT_BROKER_URL);
            //factory = new ActiveMQConnectionFactory(userName, password, udpURI);
            //factory = new ActiveMQConnectionFactory(userName, password, tcpURI);
            factory = new ActiveMQConnectionFactory(userName, password, vmUdpURI);

            //从工厂中获取一个连接
            connection = factory.createConnection();
            connection.start();

            //创建一个session
            //第一个参数:是否支持事务，如果为true，则会忽略第二个参数，被jms服务器设置为SESSION_TRANSACTED
            //第二个参数为false时，paramB的值可为Session.AUTO_ACKNOWLEDGE，Session.CLIENT_ACKNOWLEDGE，DUPS_OK_ACKNOWLEDGE其中一个。
            //Session.AUTO_ACKNOWLEDGE为自动确认，客户端发送和接收消息不需要做额外的工作。哪怕是接收端发生异常，也会被当作正常发送成功。
            //Session.CLIENT_ACKNOWLEDGE为客户端确认。客户端接收到消息后，必须调用javax.jms.Message的acknowledge方法。jms服务器才会当作发送成功，并删除消息。
            //DUPS_OK_ACKNOWLEDGE允许副本的确认模式。一旦接收方应用程序的方法调用从处理消息处返回，会话对象就会确认消息的接收；而且允许重复确认。
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            //创建一个到达的目的地，其实想一下就知道了，activemq不可能同时只能跑一个队列吧，这里就是连接了一个名为"text-msg"的队列，这个会话将会到这个队列，当然，如果这个队列不存在，将会被创建
            destination = session.createQueue(SUBJECT);
            //从session中，获取一个消息生产者
            producer = session.createProducer(destination);
            //设置生产者的模式，有两种可选
            //DeliveryMode.PERSISTENT 当activemq关闭的时候，队列数据将会被保存
            //DeliveryMode.NON_PERSISTENT 当activemq关闭的时候，队列里面的数据将会被清空
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            //创建一条消息，当然，消息的类型有很多，如文字，字节，对象等,可以通过session.create..方法来创建出来
            BytesMessage bytesMsg = session.createBytesMessage();
            byte[] bytes = createByteMessage();
            if (bytes.length > 0) {
                bytesMsg.writeBytes(bytes);
            }
            int messageLength = bytes.length;
            System.out.print("BytesLength:"+messageLength+"\n");
            long timeBefore = System.currentTimeMillis();
            long timeAfter;

            /* 持续发送10s */
            long timeInterval = 10 * 1000;

            /* 消息计数 */
            long msgCount = 0;
            for (;;) {
                timeAfter = System.currentTimeMillis();
                if ((timeAfter - timeBefore) > timeInterval)
                    break;
                //发送一条消息
                producer.send(bytesMsg);
                Thread.sleep(3);
                msgCount++;
            }
            logger.debug("发送消息成功");
            System.out.print("发送的消息数量："+msgCount+" "+"发送的消息大小："+ ((messageLength * msgCount)/(1024*1024)) +"M");
            //即便生产者的对象关闭了，程序还在运行哦
            producer.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * create a test text.
     * */
    byte[] createByteMessage(){
        StringBuffer test = new StringBuffer();

        for(int i = 0; i < 1024 * size; i++){
            test.append('a');
        }

        String testString = test.toString();

        return testString.getBytes();
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
    public void run() {
        start();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int msgSize){
        this.size = msgSize;
    }
}
