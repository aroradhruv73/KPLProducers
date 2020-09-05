package MarketPriceASKBID;

import com.thomsonreuters.platformservices.elektron.objects.marketprice.MarketPrice;
import com.thomsonreuters.ema.access.EmaFactory;
import com.thomsonreuters.ema.access.Map;
import com.thomsonreuters.ema.access.ElementList;
import com.thomsonreuters.ema.access.MapEntry;
import com.thomsonreuters.ema.access.OmmConsumer;
import com.thomsonreuters.ema.access.OmmConsumerConfig;
import com.thomsonreuters.ema.access.OmmConsumerConfig.OperationModel;
import com.thomsonreuters.ema.access.OmmException;
import com.thomsonreuters.platformservices.elektron.objects.common.Dispatcher;
import com.thomsonreuters.platformservices.elektron.objects.data.Field;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

import java.io.BufferedReader;
import java.io.IOException;
import static java.lang.System.exit;
import static java.lang.System.out;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import java.util.*;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class SwapRates 
{
	static String userName;
	static String password;
	static String clientId;
	static String proxyHostName;
	static String proxyPort = "-1";
	static String proxyUserName;
	static String proxyPassword;
	static String proxyDomain;
	static String proxyKrb5Configfile;
	
    // TREP or Elektron Service name used request MarketPrice instruments
    // IMPORTANT NOTE:  You may need to change this value to match the
    // appropriate service name to be used in your environment
    private static final String SERVICE_NAME = "ELEKTRON_DD";
    
    // If the Data Access Control System (DACS) is activated on your TREP 
    // and if your DACS username is different than your operating system user 
    // name, you may need to hardcode your DACS user name in this application.
    // To do so, you just have to set it in the following field. 
    // Note: DACS user names are usualy provided by the TREP administration 
    // team of your company. 
    private static final String DACS_USER_NAME = "";

    // Indicate is MarketPrice objects must dispatch EMA events themselves or
    // not when they are built using the synchronous mode.
    private static final boolean AUTO_DISPATCH = true;

    // The OmmConsumer used to request the MarketPrice
    private static OmmConsumer ommConsumer;
    private static int operationModel = OperationModel.USER_DISPATCH;  //API_DISPATCH; //.USER_DISPATCH; //API_DISPATCH;

    // The OmmConsumer dispatcher
    private static Dispatcher dispatcher;  
    
    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());
    
    private static Logger logger = LogManager.getLogger(SwapRates.class);
    //final AtomicLong completed = new AtomicLong(0);
    private static AtomicLong completed = new AtomicLong(0);
    //final AtomicLong sequenceNumber = new AtomicLong(0);
    private static AtomicLong sequenceNumber = new AtomicLong(0);
    //final SampleProducerConfig config = new SampleProducerConfig();
    private static ProducerConfig config = new ProducerConfig();
    //final KinesisProducer producer = new KinesisProducer(config.transformToKinesisProducerConfiguration());
    private static KinesisProducer producer = new KinesisProducer(config.transformToKinesisProducerConfiguration());
    
    private static ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
    public static ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
    
    /**
     * Main method. 
     */      
    public static void main(String[] args)
    {
    	try{
    		// Steps using an OmmConsumer configured with the USER_DISPATCH OperationModel
        
            createOmmConsumer(OperationModel.USER_DISPATCH);
            
            // Dispatcher used by the Steps to dispatch events from the main thread
            dispatcher = new Dispatcher.Builder()
                    .withOmmConsumer(ommConsumer)
                    .build();
            
            fetchSwapData();
            
            uninitializeOmmConsumer();
            dispatcher = null;
    	}
    	catch(Exception e){
    		logger.fatal("Exception in Main method",e);
    	}
        finally{
            // Wait for puts to finish. After this statement returns, we have
	        // finished all calls to putRecord, but the records may still be
	        // in-flight. We will additionally wait for all records to actually
	        // finish later.
			try {
				EXECUTOR.awaitTermination(config.getSecondsToRun() + 1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.fatal("Exception in awaitTermination",e);
			}
	        
	        // If you need to shutdown your application, call flushSync() first to
	        // send any buffered records. This method will block until all records
	        // have finished (either success or fail). There are also asynchronous
	        // flush methods available.
	        //
	        // Records are also automatically flushed by the KPL after a while based
	        // on the time limit set with Configuration.setRecordMaxBufferedTime()
	        //log.info("Waiting for remaining puts to finish...");
	        producer.flushSync();
	        producer.destroy();
        }

        out.println("  >>> Exiting the application");
    }

    public static void fetchSwapData()
    {
    	JSONObject jsonResponse = null;
    	//final AmazonKinesisClient kClient;
    	//AWSCredentials cred = new com.amazonaws.auth.profile.ProfileCredentialsProvider().getCredentials(); 
		//kClient = new AmazonKinesisClient(cred);
		
    	String line = null;
    	SwapRates cons = new SwapRates();
    	ClassLoader classLoader = cons.getClass().getClassLoader();
        InputStream stream = classLoader.getResourceAsStream("SwapRICS.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    	
    	MarketPrice theMarketPrice;
        
        try
        {
        	do
	        {
	        	while ((line = reader.readLine()) != null) {
				      String[] values = line.split(",");
				      for (String str : values) {
		          		  theMarketPrice = new MarketPrice.Builder()
				              .withOmmConsumer(ommConsumer)
				              .withName(str)
				              .withServiceName(SERVICE_NAME)
				              .withSynchronousMode(AUTO_DISPATCH)
				              .build();
		          		  theMarketPrice.open();
		          		  SimpleDateFormat formater = new SimpleDateFormat("HH:mm:ss");
		          		  String timeStamp = formater.format(Calendar.getInstance().getTime());        
		          		  out.println("    >>> Fields at " + timeStamp);
		          		  
		          		jsonResponse = createJsonObject(theMarketPrice.getField("DSPLY_NAME").value().toString(),theMarketPrice.getField("BID").value().toString(),theMarketPrice.getField("ASK").value().toString(),theMarketPrice.getField("CTBTR_1").value().toString(),theMarketPrice.getField("VALUE_TS1").value().toString(),theMarketPrice.getField("MID_PRICE").value().toString());
		          		System.out.println(jsonResponse.toString(1));
		          		
		          		try {
		        			readRecordsAndSubmitToKPL(jsonResponse);
		        		} catch (InterruptedException e) {
		        			e.printStackTrace();
		        			logger.error("Exception during the record submit in RefreshMsg",e);
		        		}
		          		theMarketPrice.close();
				      }
				    
					reader.close();
					classLoader = cons.getClass().getClassLoader();
			        stream = classLoader.getResourceAsStream("SwapRICS.csv");
			        reader = new BufferedReader(new InputStreamReader(stream));
	        	}
	        Thread.sleep(100);
	        }
        	while(true);
        } catch(IOException e) {
        	e.printStackTrace();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    private static JSONObject createJsonObject(String displayName,String bid,String ask,String contributor,String time,String mid)
    {
    	
    	String dispName = displayName; //EUR 1Y AB6E IRS  EUR 13Y AB6E IRS
    	   	
    	Matcher matcher = Pattern.compile("\\d+").matcher(dispName);
    	matcher.find();
    	String tenor = matcher.group(); //Integer.valueOf(matcher.group());
    	
    	JSONObject mainObj = new JSONObject();
    	mainObj.put("RIC",dispName);
    	mainObj.put("Tenor",tenor);
    	mainObj.put("Contributor",contributor);
    	mainObj.put("Bid",bid);
    	mainObj.put("Ask",ask);
    	mainObj.put("Mid",mid);
    	mainObj.put("Time",time);
    	
    	return mainObj;
    }
  
    /**
     * Creates the <code>OmmConsumer</code> used by the different steps of this 
     * example application. This method only sets the operation model and user 
     * name used by the OmmConsumer. Other parameters must be set via the 
     * EmaConfig.xml configuration file that comes with this application.
     * @param operationModel the EMA operation model the OmmConsumer should use.
     * It can be either <code>OperationModel.API_DISPATCH</code> or
     * <code>OperationModel.USER_DISPATCH</code>
     * @return the created <code>OmmConsumer</code>.
     */     
    private static void createOmmConsumer(int operationModel)
    {
        OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();
        Map configDb = EmaFactory.createMap();
        
        if (!readCommandlineArgs(config)) return;
        
        createProgramaticConfig(configDb);
        
        ommConsumer  = EmaFactory.createOmmConsumer(config.consumerName("Consumer_1").username(userName).password(password)
				.clientId(clientId).config(configDb).tunnelingProxyHostName(proxyHostName).tunnelingProxyPort(proxyPort)
				.tunnelingCredentialUserName(proxyUserName).tunnelingCredentialPasswd(proxyPassword).tunnelingCredentialDomain(proxyDomain)
				.tunnelingCredentialKRB5ConfigFile(proxyKrb5Configfile).operationModel(operationModel));
    } 
    
    static void createProgramaticConfig(Map configDb)
	{
		Map elementMap = EmaFactory.createMap();
		ElementList elementList = EmaFactory.createElementList();
		ElementList innerElementList = EmaFactory.createElementList();
		
		innerElementList.add(EmaFactory.createElementEntry().ascii("Channel", "Channel_1"));
		
		elementMap.add(EmaFactory.createMapEntry().keyAscii("Consumer_1", MapEntry.MapAction.ADD, innerElementList));
		innerElementList.clear();
		
		elementList.add(EmaFactory.createElementEntry().map("ConsumerList", elementMap));
		elementMap.clear();
		
		configDb.add(EmaFactory.createMapEntry().keyAscii("ConsumerGroup", MapEntry.MapAction.ADD, elementList));
		elementList.clear();
		
		innerElementList.add(EmaFactory.createElementEntry().ascii("ChannelType", "ChannelType::RSSL_ENCRYPTED"));
		innerElementList.add(EmaFactory.createElementEntry().ascii("Host", "amer-3.pricing.streaming.edp.thomsonreuters.com"));
		innerElementList.add(EmaFactory.createElementEntry().ascii("Port", "14002"));
		innerElementList.add(EmaFactory.createElementEntry().intValue("EnableSessionManagement", 1));
		
		elementMap.add(EmaFactory.createMapEntry().keyAscii("Channel_1", MapEntry.MapAction.ADD, innerElementList));
		innerElementList.clear();
		
		elementList.add(EmaFactory.createElementEntry().map("ChannelList", elementMap));
		elementMap.clear();
		
		configDb.add(EmaFactory.createMapEntry().keyAscii("ChannelGroup", MapEntry.MapAction.ADD, elementList));
	}
    
    static boolean readCommandlineArgs(OmmConsumerConfig config)
	{
	    try
	    {
	        int argsCount = 0;
	        SwapRates cons = new SwapRates();
	        
	        Properties properties = new Properties();
	        InputStream fs = cons.getClass().getClassLoader().getResourceAsStream("default_config.properties"); 
	        properties.load(fs);
	        
	        userName = properties.getProperty("userName");
	        password = properties.getProperty("password");
	        clientId = properties.getProperty("clientId");
	        config.tunnelingKeyStoreFile(properties.getProperty("keyfile"));
			config.tunnelingSecurityProtocol("TLS");
	        config.tunnelingKeyStorePasswd(properties.getProperty("keypasswd"));
	        proxyHostName = null;
	        proxyPort = null;
	        proxyUserName = null;
	        proxyPassword = null;
	        proxyDomain = null;
	        proxyKrb5Configfile = null;
	        
	        if ( userName == null || password == null || clientId == null)
			{
				System.out.println("Username, password, and clientId must be specified on the command line. Exiting...");
				logger.error("Username, password, and clientId must be specified on the command line. Exiting...");
				return false;
			}
	    }
	    catch (Exception e)
	    {
	         return false;
	    }
	return true;
	}
    
    public static void readRecordsAndSubmitToKPL(JSONObject jsonObject) throws InterruptedException
	{
		//System.out.println("hello");
		final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
		final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                // If we see any failures, we will log them.
                int attempts = ((UserRecordFailedException) t).getResult().getAttempts().size()-1;
                if (t instanceof UserRecordFailedException) {
                    Attempt last = ((UserRecordFailedException) t).getResult().getAttempts().get(attempts);
                    if(attempts > 1) {
                        Attempt previous = ((UserRecordFailedException) t).getResult().getAttempts().get(attempts - 1);
                        logger.error(String.format(
                                "Record failed to put - %s : %s. Previous failure - %s : %s",
                                last.getErrorCode(), last.getErrorMessage(), previous.getErrorCode(), previous.getErrorMessage()));
                    }else{
                        logger.error(String.format(
                                "Record failed to put - %s : %s.",
                                last.getErrorCode(), last.getErrorMessage()));
                    }

                }
                logger.error("Exception during put", t);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                //completed.getAndIncrement();
                //logger.info("Sucessfully done");
            }
        };
        
        //final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();

        // The lines within run() are the essence of the KPL API.
        final Runnable putOneRecord = new Runnable() {
            @Override
            public void run() {
                // TIMESTAMP is our partition key
                try {
                	ListenableFuture<UserRecordResult> f = producer.addUserRecord(config.getStreamName(), TIMESTAMP, Utils.randomExplicitHashKey(), ByteBuffer.wrap(jsonObject.toString().getBytes("UTF-8")));
					Futures.addCallback(f, callback, callbackThreadPool);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("Exception during the record submit",e);
				}
            }
        };
        EXECUTOR.schedule(putOneRecord,0,TimeUnit.SECONDS);
	}
    
    

    /**
     * Uninitializes the <code>OmmConsumer</code> object used by this 
     * application.
     */     
    private static void uninitializeOmmConsumer()
    {
        out.println();
        out.println("  .............................................................................");
        out.println("  >>> Uninitializing the OmmConsumer");

        if(ommConsumer != null)
        {
            ommConsumer.uninitialize();        
            ommConsumer = null;
        }
    }
}
