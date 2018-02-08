package ai.vital.service.websocket

import groovy.lang.Closure

import java.net.URL;
import java.util.Map;
import java.util.Map.Entry

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import ai.vital.service.websocket.binary.ResponseMessage

import java.util.concurrent.Executors;
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit;

/**
 * External Asynchronous version of vital service on vertx over websocket
 * All closures are called with {@link ai.vital.service.vertx.binary.ResponseMessage}
 * @author Derek
 *
 */
class VitalServiceAsyncWebsocketLiteClient extends VitalServiceAsyncClientBase {

	
	public final static String GROOVY_REGISTER_STREAM_HANDLER = 'groovy-register-stream-handler';
	
	public final static String GROOVY_UNREGISTER_STREAM_HANDLER = 'groovy-unregister-stream-handler';
	
	public final static String GROOVY_LIST_STREAM_HANDLERS = 'groovy-list-stream-handlers';
	
	public final static String VERTX_STREAM_SUBSCRIBE = 'vertx-stream-subscribe';
	
	public final static String VERTX_STREAM_UNSUBSCRIBE = 'vertx-stream-unsubscribe';
	
	public final static String TYPE_ResultList = 'ai.vital.vitalservice.query.ResultList';
	
	public final static String TYPE_VitalStatus = 'ai.vital.vitalservice.VitalStatus';
	
	
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	private final static Logger log = LoggerFactory.getLogger(VitalServiceAsyncWebsocketLiteClient.class)
	
	String endpointURL = null
	
	//throwable
	Closure completeHandler	
	
	//returns reconnect count // integer
	Closure connectionErrorHandler
	
	//this handler gets notified of reconnection event // no params
	Closure reconnectHandler
	
	Map<String, Closure> callbacksMap = [:]
	Map<String, Runnable> callbacks2TimerTasks = [:]
	
	
	
	Map<String, Closure> streamCallbacksMap = [:]
	
	Map<String, Closure> registeredHandlers = [:]
	
	Map<String, Closure> currentHandlers = [:]
	
	boolean eventbusListenerActive = false
	Closure eventbusHandler = null
	
	static ObjectMapper objectMapper = new ObjectMapper()
	
	OkHttpClient httpClient = null
	
	WebSocket webSocket
	
	Future periodicTask = null
	
	long pingInterval = 5000L;
	
	String sessionID
	
	//append this to webservice object (auth)
	String appSessionID
	
	int reconnectCount = 5
	int reconnectIntervalMillis = 3000
	
	
	private int attempt = 0
	
	private boolean closed = false
	
	private URL url
	
	
	/**
	 * 
	 * @param vertx - will *not* be closed when closeWebsocket is called or connect / reconnect exceptions occur
	 * @param app
	 * @param addressPrefix 'vitalservice.' or 'endpoint.'
	 * @param endpointURL
	 * @param reconnectCount
	 * @param reconnectIntervalMillis
	 * 
	 */
	VitalServiceAsyncWebsocketLiteClient(String appID, String addressPrefix, String endpointURL, int reconnectCount, int reconnectIntervalMillis) {
		if(appID == null) throw new NullPointerException("Null appID")
		this.address = addressPrefix + appID
		this.endpointURL = endpointURL
		if(reconnectCount < 0 ) throw new Exception("reconnectCount must be >= 0")
		if(reconnectIntervalMillis < 1) throw new Exception("reconnectIntervalMillis must be > 0")
		this.reconnectCount = reconnectCount
		this.reconnectIntervalMillis = reconnectIntervalMillis
		objectMapper = new ObjectMapper()
		sessionID = UUID.randomUUID().toString()
		
		
		
	}
	
	VitalServiceAsyncWebsocketLiteClient(String appID, String addressPrefix, String endpointURL) {
		this(appID, addressPrefix, endpointURL, 5, 3000)
	}
	
	public static String toJson(Object o) {
		return objectMapper.writeValueAsString(o)
	}
	
	protected void sendPing() {
		if(webSocket != null) {
			webSocket.send(toJson([type: 'ping']))
			log.debug("Ping sent")
		}
	}
	
	void onOpen() {
		
		attempt = 0
		
		VitalServiceAsyncWebsocketLiteClient parent = this
		
		periodicTask = scheduler.scheduleAtFixedRate(new Runnable(){
			
			@Override
			public void run() {
				parent.sendPing()
			} 
			
		}, 1000, pingInterval, TimeUnit.MILLISECONDS)
		
		
		if(completeHandler != null) {
			completeHandler.call(null)
			//handler notified only once
			completeHandler = null
		}
		
		if(currentHandlers.size() > 0) {

			List keys = new ArrayList(currentHandlers.keySet())
			log.info("Re-subscribing ${keys.size()} stream handlers: ${keys}")

			callFunctionSuper(VERTX_STREAM_SUBSCRIBE, [streamNames: keys, sessionID: this.sessionID]) { ResponseMessage res ->

				if(res.exceptionType) {
					log.error("Error when re-subscribing to streams: " + res.exceptionType + ' - ' + res.exceptionMessage)
					return
				}

				ResultList resRL = res.response
				if(resRL.status.status != VitalStatus.status_ok) {
					log.error("Error when re-subscribing to streams: " + resRL.status.message)
					return
				}

				if( true /*! this.eventbusListenerActive */) {

					log.info("Also refreshing inactive eventbus listener")

					this.eventbusHandler = createNewHandler();
	//				_this.eb.registerHandler('stream.'+ _this.sessionID, _this.eventbusHandler);
					String address = 'stream.'+ this.sessionID
					this.webSocket.send(toJson([
						type: 'register',
						address: address,
						headers: [:]
					]))


					streamCallbacksMap.put(address, this.eventbusHandler)

					this.eventbusListenerActive = true

				}

				if(reconnectHandler != null) {
					reconnectHandler.call()
				}

			}

		} else {

			if(reconnectHandler != null) {
				reconnectHandler.call()
			}

		}
		
	}
	
	void onTextMessage(String text) {
		
		
		if( log.isDebugEnabled() ) log.debug("Text Frame received, length ${text.length()}")
		
		String response = text
		Map envelope = objectMapper.readValue(response, LinkedHashMap.class)

		String mtype = envelope.type

		if(mtype == 'rec') {

			String replyAddress = envelope.address

			if(!replyAddress) {
				log.error("Received response without replyAddress")
				return
			}

			Closure callback = streamCallbacksMap.get(replyAddress)

			boolean isStream = callback != null

			if(callback == null) {
				callback = callbacksMap.remove(replyAddress)
				Future scheduledTask = callbacks2TimerTasks.remove(replyAddress)
				if(scheduledTask != null) {
					scheduledTask.cancel(false)
				}
			}


			if(callback == null) {
				log.warn("Callback not found for address ${replyAddress} - timed out")
				return
			}

			Map body = envelope.body

			ResponseMessage rm = new ResponseMessage()

			if(!body) {
				rm.exceptionType = 'error_no_body'
				rm.exceptionMessage = "No body in response message"
				if(!isStream) {
					callback(rm)
				} else {
					log.error(rm.exceptionType + ' - ' + rm.exceptionMessage)
				}
				return
			}

			Map responseObject = null


			//no wrapping code
			if(body.get('_type') != null) {

				responseObject = body

			} else {

				String status = body.status
				if(status == null) status = "error_no_status"

				String msg = body.message
				if(!msg) msg = "(empty error message)"

				if(!status.equalsIgnoreCase('ok')) {
					rm.exceptionType = status
					rm.exceptionMessage = msg
					if(!isStream) {
						callback(rm)
					} else {
						log.error(rm.exceptionType + ' - ' + rm.exceptionMessage)
					}
					return
				}

				responseObject = body.response

			}

			if(responseObject == null) {
				rm.exceptionType = 'error_no_response'
				rm.exceptionMessage = 'No response object'
				if(!isStream) {
					callback(rm)
				} else {
					log.error(rm.exceptionType + ' - ' + rm.exceptionMessage)
				}
				return
			}

			//remove stream name!
			String streamName = responseObject.remove('streamName')

			if(isStream && !streamName) {
				rm.exceptionType = 'error_no_stream_name_in_stream_response'
				rm.exceptionMessage = 'No streamName in stream response'
				log.error(rm.exceptionType + ' - ' + rm.exceptionMessage)
				return
			}

			try {

				String _type = responseObject._type

				if(_type == null) throw new Exception("No _type property in response object");
				
				Object out = null
				
				if(_type == TYPE_ResultList) {
				
					out = ResultList.fromJSON(responseObject)
						
				} else if(_type == TYPE_VitalStatus){
				
					out = VitalStatus.fromJSON(responseObject)
					
				} else {
				
					throw new Exception("Unexpected object type: " + _type)
				
				}
				
				rm.response = out
				
				if(isStream) {
					if( _type !=  TYPE_ResultList) {
						throw new Exception("Stream handler expects only result list messages")
						return
					}
					callback(streamName, rm.response)
				} else {
					callback(rm)
				}
				
				
//				rm.response = responseObject
				
			} catch(Exception e) {
				e.printStackTrace()
				log.error(e.localizedMessage, e)
				rm.exceptionType = e.getClass().getCanonicalName()
				rm.exceptionMessage = e.localizedMessage
				if(!isStream) {
					callback(rm)
				} else {
					log.error(rm.exceptionType + ' - ' + rm.exceptionMessage)
				}
				return
			}




		} else {

			log.warn("Unknown msg type: ${mtype}, ${envelope}")

		}

	}
	
	void onFailure(Throwable t) {
		
		VitalServiceAsyncWebsocketLiteClient parent = this
		
		if(periodicTask != null) {
			periodicTask.cancel(false)
			periodicTask = null
		}
		
		if(completeHandler != null) {
			
			closeWebsocket();
			completeHandler.call(t)
			completeHandler = null
				
			return
		}

		if(attempt >= reconnectCount) {
			log.error("Error when reopening a websocket connection: ${t.localizedMessage}, attempt ${attempt} of ${reconnectCount}, notifying error handler", t)
			closeWebsocket()
			connectionErrorHandler.call(attempt)
			return
		}		
		
		attempt++
		log.error("Error when reopening a websocket connection: ${t.localizedMessage}, attempt ${attempt} of ${reconnectCount} retrying in ${reconnectIntervalMillis} milliseconds", t)
		
		scheduler.schedule(new Runnable(){
			@Override
			public void run() {
				parent.openWebSocket()
			}
		}, reconnectIntervalMillis, TimeUnit.MILLISECONDS)
		
	}
	
	void openWebSocket() {
		
		if(httpClient != null) {
			try {
				if( httpClient.cache() != null ) {
					httpClient.cache().close()
				}
			} catch(Exception e) {
				log.error(e.localizedMessage)
			}

			try {
				if(httpClient.connectionPool() != null) {
					httpClient.connectionPool().evictAll()
				}
			} catch(Exception e) {
				log.error(e.localizedMessage)
			}

		}
		
		httpClient = new OkHttpClient.Builder()
//			.readTimeout(0,  TimeUnit.MILLISECONDS)
			.build();

		VitalServiceAsyncWebsocketLiteClient parent = this
		
		Request request = new Request.Builder()
		.url(endpointURL)
		.build();
		
		if(periodicTask != null) {
			periodicTask.cancel(false)
			periodicTask = null
		}
		
		httpClient.newWebSocket(request, new WebSocketListener(){
			
			@Override 
			public void onOpen(WebSocket _webSocket, Response response) {

				println "WEBSOCKET OPENED"
				log.info("Websocket opened")
				
				webSocket = _webSocket 
				
				parent.onOpen()
				
//				webSocket.send("Hello...");
//				webSocket.send("...World!");
//				webSocket.send(ByteString.decodeHex("deadbeef"));
//				webSocket.close(1000, "Goodbye, World!");
			}

			@Override			
			public void onMessage(WebSocket webSocket, String text) {
				
				parent.onTextMessage(text)
							
			}
			
			@Override
			public void onMessage(WebSocket webSocket, ByteString bytes) {
//				log.warn("Ignoring binary frame ${bytes.utf8()}")
				parent.onTextMessage(bytes.utf8())
			}
			
			@Override
			public void onClosing(WebSocket webSocket, int code, String reason) {
				log.info("onClosing, code: ${code}, reason: ${reason}")
			}
			
			@Override
			public void onClosed(WebSocket webSocket, int code, String reason) {
				log.info("onClosed, code: ${code}, reason: ${reason}")
			}
			
			@Override
			public void onFailure(WebSocket webSocket, Throwable t, Response response) {
				log.error("onFailure, ${t.localizedMessage}", t)
				
				parent.onFailure(t)				

				
			}
			
		});
	
		httpClient.dispatcher().executorService().shutdown();
		
//		public abstract class WebSocketListener {

//			public void onOpen(WebSocket webSocket, Response response) {
//			}
//		  
//			/** Invoked when a text (type {@code 0x1}) message has been received. */
//			public void onMessage(WebSocket webSocket, String text) {
//			}
//		  
//			/** Invoked when a binary (type {@code 0x2}) message has been received. */
//			public void onMessage(WebSocket webSocket, ByteString bytes) {
//			}
//		  
//			/** Invoked when the peer has indicated that no more incoming messages will be transmitted. */
//			public void onClosing(WebSocket webSocket, int code, String reason) {
//			}
//		  
//			/**
//			 * Invoked when both peers have indicated that no more messages will be transmitted and the
//			 * connection has been successfully released. No further calls to this listener will be made.
//			 */
//			public void onClosed(WebSocket webSocket, int code, String reason) {
//			}
//		  
//			/**
//			 * Invoked when a web socket has been closed due to an error reading from or writing to the
//			 * network. Both outgoing and incoming messages may have been lost. No further calls to this
//			 * listener will be made.
//			 */
//			public void onFailure(WebSocket webSocket, Throwable t, Response response) {
//			}
//		  }
		
	}
	
	/**
	 * Connects to an endpoint. 
	 * Complete handler gets notified of success (null throwable), error otherwise
	 * The connection error handler is triggered when N reconnect attempts have failed
	 */
	public void	connect(Closure completeHandler, Closure connectionErrorHandler) {
		
		if(completeHandler == null) throw new NullPointerException("complete handler must not be null")
		if(connectionErrorHandler == null) throw new NullPointerException("connectionErrorHandler must not be null")
		this.completeHandler = completeHandler
		this.connectionErrorHandler = connectionErrorHandler
		
		this.url = new URL(endpointURL)
		
		openWebSocket()
		
	}
	
	public VitalStatus closeWebsocket()  {
		
		if( this.closed ) {
			return VitalStatus.withOKMessage("Client already closed");
		}
		
		this.closed = true
		
		if(periodicTask != null) {
			periodicTask.cancel(false)
			periodicTask = null
		}
		
		VitalStatus status = null
		
		//just close the websocket connection
		if(this.webSocket != null) {
		
			try {
				
				this.webSocket.close(1000, "DONE")
				
//						status = VitalStatus.withOKMessage("async websocket client closed")
				
				
			} catch(Exception e) {

				log.error("Error when closing websocket: ${e.localizedMessage}", e)
//						status = VitalStatus.withError("Error when closing websocket: ${e.localizedMessage}")
			
			}
			
			this.webSocket = null
			
		}
		
		
		if(this.httpClient != null) {
			try {
				if( httpClient.cache() != null ) {
					httpClient.cache().close()
				}
			} catch(Exception e) {
				log.error("Error when closing http client cache: ${e.localizedMessage}", e)
			}
			
			try {
				if(httpClient.connectionPool() != null) {
					httpClient.connectionPool().evictAll()
				}
			} catch(Exception e) {
				log.error("Error when closing http client connections pool: ${e.localizedMessage}", e)
			}
			
			this.httpClient = null
		}
		
//		if(vertx != null) {
//			try {
//				vertx.close()
//			} catch(Exception e) {
//				log.error("Error when closing vertx: ${e.localizedMessage}", e)
//			}
//			this.vertx = null
//		}
		
		status = VitalStatus.withOKMessage("async websocket client closed")
		
		return status;
		
	}
	
	@Override
	protected void impl(Closure closure, String method, List args) {
	
		if(this.webSocket == null) {
			ResponseMessage res = new ResponseMessage("error_websocket_failed", "Websocket connection unavailable, retry")
			closure(res)
			return
		}
	
		if(method == 'close') {
			
			VitalStatus status = closeWebsocket()
			
			ResponseMessage res = new ResponseMessage()
			res.response = status
			
			closure(res)
		
			return
				
		}
	
		//serialize request
		
		//construct a json object and serialize it, binary format to be supported
		
		
		Map data = [
			method: method,
			args: args,
			sessionID: this.appSessionID
		]
		
		String replyAddress = UUID.randomUUID().toString() //makeUUID();
		
		Map envelope = [
			type: 'send',
			address: this.address,
			headers: [:],//mergeHeaders(this.defaultHeaders, headers),
			body: data,
			replyAddress: replyAddress 
			/*
			var replyAddress = makeUUID();
			envelope.replyAddress = replyAddress;
			this.replyHandlers[replyAddress] = callback;
			*/
		];
	
		callbacksMap.put(replyAddress, closure)	
	
//			byte[] bytes = SerializationUtils.serialize(new PayloadMessage(method, args))
//			Buffer buffer = Buffer.buffer(bytes.length)
//			((io.vertx.core.buffer.Buffer)buffer.getDelegate()).appendBytes(bytes)
		this.webSocket.send(toJson(envelope))
		
		Future scheduledTask = scheduler.schedule(new Runnable(){
			@Override
			public void run(){

				if( callbacksMap.remove(replyAddress) != null ) {
					ResponseMessage res = new ResponseMessage('error_request_timeout', "Request timed out (30000ms)")
					closure(res)
				}
								
			}
		}, 30000, TimeUnit.MILLISECONDS)
		
		callbacks2TimerTasks.put(replyAddress, scheduledTask)
		
	}

	public void query(String queryString, Closure closure) {
		impl(closure, 'query', [queryString])
	}

	private void callFunctionSuper(String function, Map<String, Object> arguments, Closure closure) {
		super.callFunction(function, arguments, closure)
	}
	
	@Override
	public void callFunction(String function, Map<String, Object> arguments, Closure closure) {

		if(function == GROOVY_LIST_STREAM_HANDLERS) {
			this.listStreamHandlers(arguments, closure)
			return
		} else if(function == GROOVY_REGISTER_STREAM_HANDLER) {
			this.registerStreamHandler(arguments, closure)
			return
		} else if(function == GROOVY_UNREGISTER_STREAM_HANDLER) {
			this.unregisterStreamHandler(arguments, closure);
			return
		} else if(function == VERTX_STREAM_SUBSCRIBE) {
			this.streamSubscribe(arguments, closure)
			return
		} else if(function == VERTX_STREAM_UNSUBSCRIBE) {
			this.streamUnsubscribe(arguments, closure)
			return
		}
		
		super.callFunction(function, arguments, closure)
		
	}
	
	private void listStreamHandlers(Map<String, Object> params, Closure closure) {
		
		ResultList rl = new ResultList()
		
		for(Entry<String, Closure> entry : this.registeredHandlers.entrySet()) {
			
			String key = entry.getKey()
			
			VitalObject g = new VitalObject();
			g.type = 'http://vital.ai/ontology/vital-core#VITAL_Node'
			g.URI = 'handler:' + key
			g.active = this.currentHandlers.containsKey(key)
			g.name = key

			rl.addResult(g)
			
		}

		ResponseMessage rm = new ResponseMessage()
		rm.response = rl
		
		closure(rl) 		
		
	}
	
	private ResponseMessage errorResponseMessage(Closure closure, String exceptionType, String exceptionMessage) {
		ResponseMessage rm = new ResponseMessage()
		rm.exceptionType = exceptionType
		rm.exceptionMessage = exceptionMessage
		closure(rm)
		return rm
	}
	
	private void registerStreamHandler(Map<String, Object> arguments, Closure closure) {
		
		def streamName = arguments.streamName;
		if(streamName == null) {
			errorResponseMessage(closure, 'error_missing_param_stream_name', "No 'streamName' param")
			return
		}
		
		if(!(streamName instanceof String)) {
			errorResponseMessage(closure, 'error_stream_name_param_type', "streamName param must be a string: " + streamName.getClass().getCanonicalName())
			return
		}
		
		def handlerFunction = arguments.handlerFunction;
		
		if(handlerFunction == null) {
			errorResponseMessage(closure, 'error_missing_param_handler_function', "No 'handlerFunction' param");
			return;
		}
		
		if(!(handlerFunction instanceof Closure)) {
			errorResponseMessage(closure, 'error_handler_function_param_type', "handlerFunction param must be a closure: " + handlerFunction.getClass().getCanonicalName())
			return
		}
		
		
		if( this.registeredHandlers.containsKey(streamName) ) {
			errorResponseMessage(closure, 'error_stream_handler_already_registered', "Handler for stream " + streamName + " already registered.");
			return;
		}
		
		this.registeredHandlers.put(streamName, handlerFunction)
//		
		ResultList rl = new ResultList()
		rl.status = VitalStatus.withOKMessage('Handler for stream ' + streamName + ' registered successfully')

		ResponseMessage rm = new ResponseMessage()
		rm.response = rl
		closure(rm)
		
	}
	
	private void unregisterStreamHandler(Map<String, Object> arguments, Closure closure) {
		
		def streamName = arguments.streamName;
		if(streamName == null) {
			errorResponseMessage(closure, 'error_missing_param_stream_name', "No 'streamName' param")
			return
		}
		
		if(!(streamName instanceof String)) {
			errorResponseMessage(closure, 'error_stream_name_param_type', "streamName param must be a string: " + streamName.getClass().getCanonicalName())
			return
		}
		
		Closure currentHandler = this.registeredHandlers.get(streamName)
		
		if(currentHandler == null) {
			errorResponseMessage(closure, 'error_stream_handler_not_registered', "No handler for stream " + streamName + " registered")
			return;
		}
		
		if(this.currentHandlers.containsKey(streamName)) {
			errorResponseMessage(closure, 'error_handler_in_use', "Handler in use " + streamName)
			return
		}
		
		registeredHandlers.remove(streamName)
		
		ResultList rl = new ResultList()
		rl.status = VitalStatus.withOKMessage('Handler for stream ' + streamName + ' unregistered successfully')

		ResponseMessage rm = new ResponseMessage()
		rm.response = rl
		closure(rm)
		
	}
	
	private void streamSubscribe(Map<String, Object> arguments, Closure closure) {
		
		//first check if we are able to
		def streamName = arguments.streamName
		if(streamName == null) {
			errorResponseMessage(closure, 'error_missing_param_stream_name', "No 'streamName' param")
			return
		}
		
		if(!(streamName instanceof String)) {
			errorResponseMessage(closure, 'error_stream_name_param_type', "streamName param must be a string: " + streamName.getClass().getCanonicalName())
			return
		}

		Closure currentHandler = this.registeredHandlers.get(streamName)
		
		if(currentHandler == null) {
			errorResponseMessage(closure, 'error_stream_handler_not_registered', "No handler for stream " + streamName + " registered")
			return;
		}
		
		Closure activeHandler = this.currentHandlers.get(streamName)
		
		if(activeHandler != null) {
			errorResponseMessage(closure, 'error_stream_handler_already_subscribed', "Handler for stream " + streamName + " already subscribed")
			return;
		}
		
		
		super.callFunction(VERTX_STREAM_SUBSCRIBE, [streamNames: [streamName], sessionID: this.sessionID]) { ResponseMessage res ->
			
			if(res.exceptionType) {
				closure(res)
				return
			}
			
			def resRL = res.response
			if(resRL.status.status != VitalStatus.status_ok) {
				closure(res)
				return
			}
			

			if(! this.eventbusListenerActive ) {
				
				this.eventbusHandler = createNewHandler();
//				_this.eb.registerHandler('stream.'+ _this.sessionID, _this.eventbusHandler);
				String address = 'stream.'+ this.sessionID
				this.webSocket.send(toJson([
					type: 'register',
					address: address,
					headers: [:]
				]))
				
				
				streamCallbacksMap.put(address, this.eventbusHandler)
				
				this.eventbusListenerActive = true
			}
			
			this.currentHandlers.put(streamName, currentHandler)
			
			ResultList rl = new ResultList()
			rl.status = VitalStatus.withOKMessage('Successfully subscribed to stream ' + streamName)

			ResponseMessage rm = new ResponseMessage()
			rm.response = rl
			closure(rm)
			
		}
		
	}

	private Closure createNewHandler() {

		Closure wrapperHandler = { String streamName, ResultList rl ->

			Closure handler = this.currentHandlers.get(streamName)
			
			if(handler == null) {
				log.warn("Received a message for non-existing stream handler: " + streamName)
				return
				
			}			
			
			handler(rl)
			
		};
		
		return wrapperHandler;
		
	}
		
	private void streamUnsubscribe(Map<String, Object> arguments, Closure closure) {
		
		def streamName = arguments.streamName
		if(streamName == null) {
			errorResponseMessage(closure, 'error_missing_param_stream_name', "No 'streamName' param")
			return
		}
		
		if(!(streamName instanceof String)) {
			errorResponseMessage(closure, 'error_stream_name_param_type', "streamName param must be a string: " + streamName.getClass().getCanonicalName())
			return
		}
		
		Closure activeHandler = this.currentHandlers.get(streamName)
		
		if( activeHandler == null ) {
			errorResponseMessage(closure, 'error_no_subscribed_stream_handlers', "No handler subscribed to stream " + streamName);
			return;
		}
		
		super.callFunction(VERTX_STREAM_UNSUBSCRIBE, [streamNames: [streamName], sessionID: this.sessionID]) { ResponseMessage res ->
			
			if(res.exceptionType) {
				closure(res)
				return
			}
			
			def resRL = res.response
			if(resRL.status.status != VitalStatus.status_ok) {
				closure(res)
				return
			}
			
			this.currentHandlers.remove(streamName)
			
			if(this.currentHandlers.size() < 1) {

				this.streamCallbacksMap.clear()
				
//				_this.eb.unregisterHandler('stream.'+ _this.sessionID, _this.eventbusHandler);
				String address = 'stream.'+ this.sessionID
				this.webSocket.send(toJson([
					type: 'unregister',
					address: address,
					headers: [:]
				]))
				
				this.eventbusListenerActive = false;
				
			}
			
			
			ResultList rl = new ResultList()
			rl.status = VitalStatus.withOKMessage('Successfully unsubscribed from stream ' + streamName)

			ResponseMessage rm = new ResponseMessage()
			rm.response = rl
			closure(rm)
			
		}
		
	}	

	public URL getUrl() {
		return url;
	}

		
}
