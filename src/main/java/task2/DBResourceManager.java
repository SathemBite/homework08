package task2;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Created by anton on 17.11.17.
 */
public class DBResourceManager {
    private final static DBResourceManager instance = new DBResourceManager();
    private final static ResourceBundle bundle =
            ResourceBundle.getBundle("keyConnPoolDBData", new Locale("en"));

    public static DBResourceManager getInstance() {
        return instance;
    }

    public String getValue(String key){
        return bundle.getString(key);
    }
}
