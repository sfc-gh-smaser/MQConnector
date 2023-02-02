package snowflake.mq;
import java.io.FileInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.simple.SimpleLogger;
import java.lang.reflect.Field;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;

/**
 * Example on how to use the Streaming Ingest client APIs
 * Re-used for subscribing to an MQ-Series Queue Demonstration (Feb, 2023)
 *            Author:  Steven.Maser@snowflake.com
 *
 * <p>Please read the README.md file for detailed steps and expanding beyond this example
 *   https://github.com/snowflakedb/snowflake-ingest-java
 */
public class MQConnector {
    private static String PROPERTIES_FILE = "./mqconnector.properties";
    private static boolean DEBUG=false;
    private static int BATCH_WAIT_SECONDS=30;
    private static String QMNAME = "QM1";
    private static String QNAME = "SYSTEM.DEFAULT.LOCAL.QUEUE";
    private Logger LOGGER = new Logging(this.getClass()).getLogger();

    public static void main(String[] args) throws Exception {
        if(args!=null && args.length>0) PROPERTIES_FILE=args[0];
        //Load properties file
        Properties props=loadProperties();

        MQQueueManager qMgr = null;
        MQQueue queue = null;
        int id=1;
        String offsetTokenFromSnowflake="-1";
        // Create a streaming ingest client
        try (SnowflakeStreamingIngestClient client =
                     SnowflakeStreamingIngestClientFactory.builder("CLIENT").setProperties(props).build()) {

            // Create an open channel request on table
            OpenChannelRequest request1 =
                    OpenChannelRequest.builder(props.getProperty("channel_name"))
                            .setDBName(props.getProperty("database"))
                            .setSchemaName(props.getProperty("schema"))
                            .setTableName(props.getProperty("table"))
                            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                            .build();

            // Open a streaming ingest channel from the given client
            SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

            // open MQ Client
            if(DEBUG) System.out.println("Connecting to queue manager: " + QMNAME);
            qMgr = new MQQueueManager(QMNAME);
            int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE;
            if(DEBUG) System.out.println("Accessing queue: " + QNAME);
            queue = qMgr.accessQueue(QNAME, openOptions);
            System.out.println("Connected to Queue: " + QNAME);
            // loop for micro-batching
            do {
                boolean msgsProcessed=false;
                while (queue.getCurrentDepth()>0) {
                    // MQ get msg
                    MQMessage rcvMessage = new MQMessage();
                    MQGetMessageOptions gmo = new MQGetMessageOptions();
                    queue.get(rcvMessage, gmo);
                    //String msg = imsg.readUTF();
                    int strLen = rcvMessage.getMessageLength();
                    byte[] strData = new byte[strLen];
                    rcvMessage.readFully(strData);
                    String msg = new String(strData);
                    if(DEBUG) System.out.println("<msg>" + msg+"</msg>");

                    //Snowpipe Streaming, build row
                    Map<String, Object> row = new HashMap<>();
                    //row.put("RECORD_METADATA",meta); // msg metadata
                    row.put("RECORD_CONTENT", msg);  // data column

                    //Snowpipe Streaming, send row
                    InsertValidationResponse response = channel1.insertRow(row, String.valueOf(id));
                    if (response.hasErrors()) {
                        // Simply throw exception at first error
                        throw response.getInsertErrors().get(0).getException();
                    }
                    else id++;
                    msgsProcessed=true;
                }
                // Wait here before checking MQ again
                TimeUnit.SECONDS.sleep(BATCH_WAIT_SECONDS);
                if(msgsProcessed) {
                    // validate sent
                    int retryCount = 0;
                    int maxRetries = 100;
                    String expectedOffsetTokenInSnowflake = String.valueOf(id);
                    for (offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken(); offsetTokenFromSnowflake == null
                            || !offsetTokenFromSnowflake.equals(expectedOffsetTokenInSnowflake); ) {
                        if (DEBUG) System.out.println("Offset Needed:  "+id+" - Offset from Snowflake:  " + offsetTokenFromSnowflake);
                        Thread.sleep(1000);
                        offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
                        retryCount++;
                        if (retryCount >= maxRetries) {
                            System.out.println(
                                    String.format(
                                            "Failed to receive required OffsetToken in Snowflake:%s after MaxRetryCounts:%s (%S) at ID=%d",
                                            expectedOffsetTokenInSnowflake, maxRetries, offsetTokenFromSnowflake,id));
                            System.exit(1);
                        }
                    }
                }
            } while (true);
        }
        catch (MQException ex) {
            System.out.println("An IBM MQ Error occurred : Completion Code " + ex.completionCode
                    + " Reason Code " + ex.reasonCode);
            ex.printStackTrace();
            for (Throwable t = ex.getCause(); t != null; t = t.getCause()) {
                System.out.println("... Caused by ");
                t.printStackTrace();
            }

        }
        finally{
            if(DEBUG) System.out.println("Disconnecting from the MQ");
            if(queue!=null) queue.close();
            if(qMgr!=null) qMgr.disconnect();
            System.out.println("Total #Messages read from MQ:  "+id);
            System.out.println("Final Offset in Snowflake:  "+offsetTokenFromSnowflake);
        }
    }

    private static Properties loadProperties() throws Exception {
        Properties props = new Properties();
        try {
            File f = new File(PROPERTIES_FILE);
            if (!f.exists()) throw new Exception("Unable to find properties file:  " + PROPERTIES_FILE);
            FileInputStream resource = new FileInputStream(PROPERTIES_FILE);
            props.load(resource);
            String waitTime=props.getProperty("batch_seconds");
            if(waitTime!=null) BATCH_WAIT_SECONDS=Integer.parseInt(waitTime);
            else System.out.println("Property 'BATCH_SECONDS' missing from properties file, using default:  "+BATCH_WAIT_SECONDS);
            String qm=props.getProperty("queue_manager");
            if(qm!=null) QMNAME=qm;
            else  System.out.println("Property 'QUEUE_MANAGER' missing from properties file, using default:  "+QMNAME);
            String q=props.getProperty("queue");
            if(q!=null) QNAME=q;
            else System.out.println("Property 'QUEUE' missing from properties file, using default:  "+QNAME);
            String debug = props.getProperty("debug");
            if (debug != null) DEBUG = Boolean.parseBoolean(debug);
            if (DEBUG) {
                for (Object key : props.keySet())
                    System.out.println("  * DEBUG: " + key + ": " + props.getProperty(key.toString()));
            }
            else System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
            if (props.getProperty("private_key_file") != null) {
                String keyfile = props.getProperty("private_key_file");
                File key = new File(keyfile);
                if (!(key).exists()) throw new Exception("Unable to find key file:  " + keyfile);
                String pkey = readPrivateKey(key);
                props.setProperty("private_key", pkey);
            }
            props.setProperty("scheme","https");
            props.setProperty("port","443");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
        return props;
    }

    private static String readPrivateKey(File file) throws Exception {
        String key = new String(Files.readAllBytes(file.toPath()), Charset.defaultCharset());
        String privateKeyPEM = key
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");
        if(DEBUG) {  // check key file is valid
            byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
            RSAPrivateKey k = (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
            System.out.println("* DEBUG: Provided Private Key is Valid:  ");
        }
        return privateKeyPEM;
    }
}
