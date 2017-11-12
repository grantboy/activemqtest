package buaa.grant;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by grant on 2016/12/10.
 */
public class PTPSender {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PTPSender.class);

    public static void main(String[] args) {
        int threadNum = 1;
        PropertyConfigurator.configure("D:\\code\\activemqtest\\src\\main\\resources\\log4j.properties");

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

        for (int i = 0; i < threadNum; i++){
            MsgProducer msgProducer = new MsgProducer();
            msgProducer.setSize(Integer.parseInt(args[0]));
            executorService.execute(msgProducer);
        }
        while(true){
        }
    }
}
