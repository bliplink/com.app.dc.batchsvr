
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.gw.common.utils.Consts;
import com.gw.common.utils.ServerGwWrapper;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@ComponentScan({ "com.gw.common.utils", "com.app.common.db", "com.app.dc"})
@SpringBootApplication
public class BatchSvr {
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		PropertyConfigurator.configureAndWatch("./config/log4j.ini", 1000);
		Logger logger = LoggerFactory.getLogger(BatchSvr.class);
 		try {
			ServerGwWrapper.Start(BatchSvr.class, "./config/application.properties", args);

		} catch (Exception e) {
			logger.error(Consts.ServerName + " start error.", e);
		}
	}

}
