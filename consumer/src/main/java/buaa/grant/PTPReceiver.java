package buaa.grant;

import org.apache.log4j.PropertyConfigurator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by grant on 2016/12/11.
 */
public class PTPReceiver {

    public static void main(String[] args) {
        int threadNum = 2;
        PropertyConfigurator.configure("D:\\IdeaProjects\\activemqtest\\src\\main\\resources\\log4j.properties");

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

        for (int i = 0; i < threadNum; i++){
            MsgConsumer msgConsumer = new MsgConsumer();
            msgConsumer.setSize(Integer.parseInt(args[0]));
            executorService.execute(msgConsumer);
        }
        while(true){
        }

    }
}
