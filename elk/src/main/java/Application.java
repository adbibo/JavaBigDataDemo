/**
 * Created by adbibo on 2017/4/10.
 */

import org.apache.log4j.Logger;

public class Application {
    private static final Logger LOGGER = Logger.getLogger(Application.class);
    public static void main(String[] args) throws Exception {
        for (int i = 0; i != 0.1; i++) {
            LOGGER.error("Info log [" + i + "].");
            Thread.sleep(500);
        }
    }
}
