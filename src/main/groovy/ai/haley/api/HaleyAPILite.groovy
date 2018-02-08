package ai.haley.api

import ai.haley.api.impl.HaleyFileUploadImplementation;
import ai.haley.api.session.HaleySession
import ai.haley.api.session.HaleyStatus
import ai.vital.service.websocket.binary.ResponseMessage
import groovy.transform.CompileStatic;
import ai.vital.service.websocket.ResultList
import ai.vital.service.websocket.VitalObject;
import ai.vital.service.websocket.VitalServiceAsyncWebsocketLiteClient
import ai.vital.service.websocket.VitalStatus;

import java.nio.channels.Channel
import java.util.Map.Entry
import java.util.regex.Matcher
import java.util.regex.Pattern

import org.slf4j.Logger
import org.slf4j.LoggerFactory;

//import java.util.concurrent.Executors


/*
 * Haley API
 * 
 * 
 * Includes synchronous and asynchronous calls
 * 
 * Multiple instances of HaleyAPI may be instantiated.  Howver, there is an underlying
 * singleton for vitalsigns, so domain classes will be shared across all instances.
 * 
 * The initial version may consider a single endpoint only.
 * 
 * There may be a conflict when opening multiple endpoints with conflicting domains.
 * This can throw an exception and not open the session.
 * 
 * 
 */
class HaleyAPILite {
	
	public static final String aimp_endpointURI = 'http://vital.ai/ontology/vital-aimp#hasEndpointURI'
	
	public static final String aimp_lastActivityTime = 'http://vital.ai/ontology/vital-aimp#hasLastActivityTime'
	
	public static final String aimp_masterUserID = 'http://vital.ai/ontology/vital-aimp#hasMasterUserID'
	
	public static final String aimp_requestURI = "http://vital.ai/ontology/vital-aimp#hasRequestURI"
	
	public static final String aimp_userID = 'http://vital.ai/ontology/vital-aimp#hasUserID'
	
	public static final String aimp_userName = 'http://vital.ai/ontology/vital-aimp#hasUserName'
	
	public static final String aimp_sessionID = 'http://vital.ai/ontology/vital-aimp#hasSessionID'
	
	
	 
	public static final String aimp_Channel = 'http://vital.ai/ontology/vital-aimp#Channel'
	
	public static final String aimp_HeartbeatMessage = 'http://vital.ai/ontology/vital-aimp#HeartbeatMessage'
	
	public static final String aimp_ListChannelsRequestMessage = 'http://vital.ai/ontology/vital-aimp#ListChannelsRequestMessage'
	
	public static final String aimp_UserLeftApp = 'http://vital.ai/ontology/vital-aimp#UserLeftApp'
	
	public static final String aimp_UserLoggedIn = 'http://vital.ai/ontology/vital-aimp#UserLoggedIn'
	
	public static final String aimp_UserLoggedOut = 'http://vital.ai/ontology/vital-aimp#UserLoggedOut'
	
	public static final String vitalcore_name = 'http://vital.ai/ontology/vital-core#hasName'
	
	public static final String vitalcore_sessionID = 'http://vital.ai/ontology/vital-core#hasSessionID'
	
	public static final String vitalcore_username = 'http://vital.ai/ontology/vital-core#hasUsername'
	
	public static final String vital_fileScope = 'http://vital.ai/ontology/vital#hasFileScope'
	
	public static final String vital_fileURL = 'http://vital.ai/ontology/vital#hasFileURL'
	
	public static final String vital_Login = 'http://vital.ai/ontology/vital#Login'
	
	public static final String vital_UserSession = 'http://vital.ai/ontology/vital#UserSession'
		
	
	private final static Logger log = LoggerFactory.getLogger(HaleyAPILite.class)
	
	private static class MessageHandler {
		
		Closure callback
		
		List<String> primaryClasses = []
		
//		List<String> classes = []
		
	}
	
//	private List<HaleySession> sessions = []
	
	private HaleySession haleySessionSingleton

	private VitalServiceAsyncWebsocketLiteClient vitalService
	
	private String streamName = 'haley'
	
	
	private List<MessageHandler> handlers = []

	//requestURI -> callback
	private Map<String, Closure> requestHandlers = new HashMap<String, Closure>()
	
	Closure defaultHandler = null;
	
	Closure handlerFunction = null;
	
	
	//private final def mainPool = Executors.newFixedThreadPool(10)
	
	private syncdomains = false
	
	private Map<String, CachedCredentials> cachedCredentials = [:]
	
	
	private static class CachedCredentials {
		String username
		String password
	}
	
	
	private List<Closure> reconnectListeners = []
	
	private Long lastActivityTimestamp = null; 
	
	public boolean addReconnectListener(Closure reconnectListener) {
		if(reconnectListeners.contains(reconnectListener)) {
			return false
		}
		reconnectListeners.add(reconnectListener)
		return true
	}
	
	public boolean removeReconnectListener(Closure reconnectListener) {
		if(!reconnectListeners.contains(reconnectListener)) {
			return false
		}
		reconnectListeners.remove(reconnectListener)
		return true
	}
	
	private String _checkSession(HaleySession haleySession) {
		
		if(this.haleySessionSingleton == null) return 'no active haley session found';
		
		if(this.haleySessionSingleton != haleySession) return 'unknown haley session';
		
		return null;
		
	}
	
	//nodejs callback
	private void _sendLoggedInMsg(Closure callback) {

		VitalObject userLoggedIn = new VitalObject()
		userLoggedIn.type = aimp_UserLoggedIn
		
		this.sendMessage(this.haleySessionSingleton, userLoggedIn, []) { HaleyStatus status ->
			
			if(status.ok) {
				callback(null)
			} else {
			
				log.error("Error when sending loggedin message: ", status.errorMessage);
			
				callback("ERROR: " + status.errorMessage)
			}
			
		}
				
//		var msg = vitaljs.graphObject({type: 'http://vital.ai/ontology/vital-aimp#UserLoggedIn'});
//		
//		this.sendMessage(this.haleySessionSingleton, msg, [], function(error){
//			
//			if(error) {
//				console.error("Error when sending loggedin message: ", error);
//				callback(error);
//			} else {
//				callback(null);
//			}
//			
//		});
	}
	
	// init a new instance
	// default is to use only locally available domains
	
	public HaleyAPILite(VitalServiceAsyncWebsocketLiteClient websocketClient) {
		
		this.vitalService = websocketClient
		
		this.vitalService.reconnectHandler = { Void v -> 
			
			log.info("Notifying reconnect listeners: [${this.reconnectListeners.size()}]")
						
			for(Closure rl : this.reconnectListeners) {
							
				rl.call(this.haleySessionSingleton)
							
			}

		}
		
		this.syncdomains = false
		
	}
	
	/*
	 * utility classes to test parallelization of calls
	 * 
	 */
	
	public int getActiveThreadCount() {
		return 0
	}
	
	public boolean isQuiescent() {
		return true
	}
	
	
	// open session to haley service
	public HaleySession openSession() {
	
		throw new Exception("Blocking version not implemented yet")
		
//		HaleySession session = new HaleySession()
//		
//		sessions.add(session)
//		
//		return session
		
	
	}
	
	// open session to local haley server, given endpoint
	public HaleySession openSession(String endpoint) {
		
		throw new Exception("Blocking version not implemented yet")
//		HaleySession session = new HaleySession()
//		
//		sessions.add(session)
//		
//		return session
		
	}
	
	private void _streamHandler(ResultList msgRL) {

		VitalObject m = msgRL.first();
	
		log.info("Stream " + this.streamName + " received message: {} payload size {}", m.getClass().getCanonicalName(), msgRL.results.size() - 1);
		
		int c = 0;
	
		String typeURI = m.type
		
		//requestURI handler
		String requestURI = m.get(aimp_requestURI)
	
		if(requestURI != null) {
		
			Closure h = this.requestHandlers[requestURI];
		
			if(h != null) {
			
				log.info("Notifying requestURI handler", requestURI);
			
				Object cbRes = h(msgRL);
			
				if(cbRes != null && cbRes == false) {
				
					log.info("RequestURI handler returned false, unregistering");
				
					this.requestHandlers.remove(requestURI);
				
				} else {
				
					log.info("RequestURI handler returned non-false, still regsitered");
				
				}
			
				return;
			
			}
		
		}
		
		//primary classes
		for(MessageHandler h : this.handlers) {
			
			for(String pc : h.primaryClasses) {
				
				if(pc == typeURI) {
				
					log.info("Notifying primary type handler: ", h.primaryClasses);
					
					h.callback.call(msgRL)
					c++;
					return
						
				}
				
			}
			
		}
		
	
		/*XXX unsupported
		for(MessageHandler h : this.handlers) {
		
			for(Class<? extends AIMPMessage> pc : h.classes) {
				
				if(pc.isAssignableFrom(type)) {
					
					log.info("Notifying secondary type handler: ", h.classes);
					
					h.callback(msgRL);
					c++;
					return;
					
				}
				
			}
			
		}
		*/
		
	
		if(this.defaultHandler != null) {
			
			log.info("Notifying default handler");
			
			this.defaultHandler.call(msgRL);
			
			c++
			
			return;
		}
		
		
		log.info("Notified " + c + " msg handlers");

	}
	
	//nodejs callback style: String error, HaleySession sessionObject 
	public void openSession(Closure callback) {
	
//		HaleySession session = new HaleySession()
//		
//		sessions.add(session)
//		
//		callback.call(session)
			
		if(this.haleySessionSingleton != null) {
			callback('active session already detected', null);
			return;
		}

		this.handlerFunction = { ResultList rl ->
			
			log.info("Message received: " + rl)
			
			_streamHandler(rl)
			
		}
		
		log.info('subscribing to stream ', this.streamName);
		
		vitalService.callFunction(VitalServiceAsyncWebsocketLiteClient.GROOVY_REGISTER_STREAM_HANDLER, [streamName: (Object)this.streamName, handlerFunction: this.handlerFunction] ) { ResponseMessage regRes ->
			
			if(regRes.exceptionType) {
				callback(regRes.exceptionType + ' - ' + regRes.exceptionMessage, null)
				return
			}
			
			ResultList regRL = (ResultList) regRes.response
			if(regRL.status.status != VitalStatus.status_ok) {
				callback("ERROR: " + regRL.status.message, null)
				return
			}
			
			log.info('registered handler to ' + this.streamName);
			

			
			vitalService.callFunction(VitalServiceAsyncWebsocketLiteClient.VERTX_STREAM_SUBSCRIBE, [streamName: (Object)this.streamName]) { ResponseMessage subRes ->
				if(subRes.exceptionType) {
					callback(subRes.exceptionType + ' - ' + subRes.exceptionMessage, null)
					return
				}
				
				ResultList subRL = (ResultList) regRes.response
				if(subRL.status.status != VitalStatus.status_ok) {
					callback("ERROR: " + subRL.status.message, null)
					return
				}
				
				log.info('subscribed to ' + this.streamName);
				
				//in groovy session cookie is unavailable - session must not be authenticated
				this.haleySessionSingleton = new HaleySession(sessionID: vitalService.sessionID)
				
				callback(null, this.haleySessionSingleton)
				
			}			
			
		}
		
		/*
		log.info('subscribing to stream ', this.streamName);
			
//			var _this = this;
//		
//			this.handlerFunction = function(msgRL){
//				_this._streamHandler(msgRL);
//			}
			
			//first register stream handler
			this.vitalService.callFunction(VitalService.JS_REGISTER_STREAM_HANDLER, {streamName: this.streamName, handlerFunction: this.handlerFunction}, function(succsessObj){
				
				console.log('registered handler to ' + _this.streamName, succsessObj);
				
				_this.vitalService.callFunction(VitalService.VERTX_STREAM_SUBSCRIBE, {streamName: _this.streamName}, function(succsessObj){
					
					console.log("subscribed to stream " + _this.streamName, succsessObj);
					
					//session opened
					_this.haleySessionSingleton = new HaleySession(_this);
					
					if(_this.haleySessionSingleton.isAuthenticated()) {
						
						_this._sendLoggedInMsg(function(error){
							
							console.log("LoggedIn msg sent successfully");
							
							if(error) {
								callback(error);
							} else {
								callback(null, _this.haleySessionSingleton);
							}
							
						});
						
					} else {
						
						callback(null, _this.haleySessionSingleton);
						
					}
					
					
					
				}, function(errorObj) {
					
					console.error("Error when subscribing to stream", errorObj);
					
					callback(errorObj);
					
				});
		
				
			}, function(error){
		
				console.error('couldn\'t register messages handler', error);
				
				callback(error);
				
			});
			*/
	
	}
	
	
	public HaleyStatus closeSession(HaleySession session) {
		
		throw new Exception("Blocking version not implemented yet")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		
//		sessions.remove(session)
//		
//		return status
		
	}
	
	public void closeSession(HaleySession session, Closure callback) {
	
		throw new Exception("not implemented yet")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		sessions.remove(session)
//		
//		callback.call(status)
		
	}
	
	
	public HaleyStatus closeAllSessions() {
		
		throw new Exception("Blocking version not implemented yet")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		int closed = 0
//		
//		// do close
//		sessions.each { session -> 
//			
//			closed++
//			
//		}
//
//		sessions.clear()
//				
//		
//		return status
		
	}
	
	public void closeAllSessions(Closure callback) {
	
		throw new Exception("not implemented yet")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		int closed = 0
//		
//		// do close
//		sessions.each { session -> 
//			closed++ 
//		}
//		
//		sessions.clear()
//		
//		callback.call(status)
		
	
	}
	
	public Collection<HaleySession> getSessions() {
		
		if(this.haleySessionSingleton != null) {
			return [this.haleySessionSingleton]
		} else {
			return []
		}
		
//		return sessions.asImmutable()
		
	}
	
	
	public HaleyStatus authenticateSession(HaleySession session, String username, String password) {

		throw new Exception("Blocking version not implemented yet") 
				
//		HaleyStatus status = new HaleyStatus()
//		return status
		
	}
	
	public void authenticateSession(HaleySession session, String username, String password, Closure callback) {
		authenticateSessionImpl(session, username, password, true, callback)
	}
	
	private void authenticateSessionImpl(HaleySession session, String username, String password, boolean sendLoggedInMsg, Closure callback) {
		
		String error = this._checkSession(session);
		if(error) {
			callback(HaleyStatus.error(error));
			return;
		}
		
		if(session.isAuthenticated()) {
			callback(HaleyStatus.error('session already authenticated'));
			return;
		}
		
		this.vitalService.callFunction('vitalauth.login', [loginType: (Object)'Login', username: username, password: password]) { ResponseMessage loginRes ->
				
			if(loginRes.exceptionType) {
				callback(HaleyStatus.error("Logging in exception: ${loginRes.exceptionType} - ${loginRes.exceptionMessage}"))
				return
			}
			
			ResultList res = (ResultList) loginRes.response
			
			if(res.status.status != VitalStatus.status_ok) {
				callback(HaleyStatus.error("Logging in failed: ${res.status.message}"))
				return
			}
			
			log.info("auth success: ");
	
			//logged in successfully keep session for further requests
			List userSessions = res.iterator(vital_UserSession).toList()
			VitalObject userSession = userSessions.size() > 0 ? userSessions.first() : null
			if(userSession == null) {
				callback(HaleyStatus.error("No session object in positive login response"))
				return
			}
			
			List logins = res.iterator(vital_Login).toList()
			
			VitalObject userLogin = logins.size() > 0 ? logins.first() : null 
			
			if(userLogin == null) {
				callback(HaleyStatus.error("No login object in positive login response"))
				return
			}
			
			log.info("Session obtained: ${userSession.get(vitalcore_sessionID)}")
			
			cachedCredentials.put(session.sessionID, new CachedCredentials(username: username, password: password))
			
			//set it in the client for future requests
			session.authSessionID = userSession.get(vitalcore_sessionID)
			session.authAccount = userLogin
			session.authenticated = true
			
			//appSessionID must be set in order to send auth messages
			vitalService.appSessionID = userSession.get(vitalcore_sessionID)
			
			if(sendLoggedInMsg) {
				
				_sendLoggedInMsg() { String sendError ->
				
					if(sendError) {
						callback(HaleyStatus.error(sendError))
					} else {
						callback(HaleyStatus.ok())
						
					}
				
				}
				
			} else {
			
				callback(HaleyStatus.ok())
				
			}
			
			
		}
		
}
	
	
	public HaleyStatus unauthenticateSession(HaleySession session) {
		
		throw new Exception("blocking version not implemented yet")
//		HaleyStatus status = new HaleyStatus()
//		
//		
//		
//		return status
		
		
	}
	
	public void unauthenticateSession(HaleySession session, Closure callback) {
	
		throw new Exception("not implemented yet")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		
//		callback.call(status)
		
}
	
	public void sendMessageWithRequestCallback(HaleySession haleySession, VitalObject aimpMessage, List<VitalObject> payload, Closure requestCallback, Closure sendOpCallback) {
		
		HaleyStatus status = this.registerRequestCallback(aimpMessage, requestCallback)
		if(!status.ok) {
			sendOpCallback(status)
			return
		}
		
		this.sendMessage(haleySession, aimpMessage, payload, sendOpCallback)
			
	}
	
	
	//haley status callback
	public void sendMessage(HaleySession haleySession, VitalObject aimpMessage, List<VitalObject> payload, Closure callback) {
		sendMessageImpl(haleySession, aimpMessage, payload, 0, callback)
	}
		
	//internal call
	private void sendMessageImpl(HaleySession haleySession, VitalObject aimpMessage, List<VitalObject> payload, int retry, Closure callback) {
		
		String error = this._checkSession(haleySession);
		if(error) {
			callback(HaleyStatus.error(error));
			return;
		}
		
		if(aimpMessage == null) {
			callback(HaleyStatus.error("aimpMessage must not be null"));
			return;
		}
		
		if(aimpMessage.URI == null) {
			aimpMessage.generateURI()
		}
		
		String sessionID = haleySession.getSessionID();
	
		VitalObject authAccount = haleySession.getAuthAccount();
		
		String masterUserID = aimpMessage.get(aimp_masterUserID)
		
		if(masterUserID) {
			
			if(authAccount == null) {
				callback(HaleyStatus.error("No auth account available - cannot use masterUserID"));
				return
			}
			
			String currentUserID = authAccount.get(vitalcore_username)
			if(currentUserID != masterUserID) {
				callback(HaleyStatus.error("Master and current userID are different: " + masterUserID + " vs " + currentUserID))
				return
			}
			
			String effectiveUserID = aimpMessage.get(aimp_userID)
			if(effectiveUserID == null) {
				callback(HaleyStatus.error("No userID in the message, it is required when using masterUserID tunneling"))
				return
			}
			
			String endpointURI = aimpMessage.get(aimp_endpointURI)
			if(!endpointURI) {
				callback(HaleyStatus.error("masterUserID may only be used with endpointURI"))
				return
			}
			
			if(masterUserID == effectiveUserID) {
				callback(HaleyStatus.error("masterUserID should not be equal to effective userID: " + masterUserID + " vs " + currentUserID))
				return
			}
			
		} else {
		
			if( authAccount != null ) {
			
				String userID = aimpMessage.get(aimp_userID)
			
				if(userID == null) {
					
					aimpMessage.put(aimp_userID, authAccount.get(vitalcore_username))

				} else {
				
					if(userID != authAccount.get(vitalcore_username).toString()) {
						callback(HaleyStatus.error('auth userID ' + authAccount.get(vitalcore_username) + ' does not match one set in message: ' + userID));
						return;
					}
				}
			
				String n = authAccount.get(vitalcore_name)
				aimpMessage.put(aimp_userName, n != null ? n : authAccount.get(vitalcore_username))
			
			}
		
		}
		
	
		String sid = aimpMessage.get(aimp_sessionID)
		
		if(sid == null) {
			aimpMessage.put(aimp_sessionID, vitalService.sessionID)
		} else {
			if(sid != sessionID) {
				callback(HaleyStatus.error('sessionID ' + sessionID + " does not match one set in message: " + sid));
				return
			}
		}
	
		ResultList rl = new ResultList()
		rl.addResult(aimpMessage);
	
		if(payload != null) {
			for(VitalObject g : payload) {
				rl.addResult(g);
			}
		}
	
		String method = ''
		if( haleySession.isAuthenticated() ) {
			method = 'haley-send-message'
		} else {
			method = 'haley-send-message-anonymous'
		}
		
		boolean updateTimestamp = true
		
		String t = aimpMessage.getType()
		
		if(t == aimp_UserLoggedIn || t == aimp_UserLoggedOut || t == aimp_UserLeftApp) {
			updateTimestamp = false
		} else if(t == aimp_HeartbeatMessage) {
			updateTimestamp = false
			if(this.lastActivityTimestamp != null) {
				aimpMessage.put(aimp_lastActivityTime, this.lastActivityTimestamp)
			}
		}
 		
		this.vitalService.callFunction(method, [message: (Object) rl.toJSON()]) { ResponseMessage sendRes ->
		
			if(sendRes.exceptionType) {
				
				if(retry == 0 && sendRes.exceptionType == "error_denied") {
					
					CachedCredentials cachedCredentials = cachedCredentials.get(haleySession.sessionID)
					
					if(cachedCredentials != null) {
						
						log.info("Session not found, re-authenticating...")
						
						haleySession.authAccount = null
						haleySession.authenticated = false
						haleySession.authSessionID = null
						vitalService.appSessionID = null
						
						authenticateSessionImpl(haleySession, cachedCredentials.username, cachedCredentials.password, false) { HaleyStatus status ->
							
							if(status.isOk()) {
								
								log.info("Successfully reauthenticated the session, sending the message")
								
								sendMessageImpl(haleySession, aimpMessage, payload, retry+1, callback)
								
							} else {
							
								log.error("Reauthentication attempt failed: ${status.errorMessage}")
								
								callback(HaleyStatus.error(sendRes.exceptionType + ' - ' + sendRes.exceptionMessage))
								return
							
							}
							
						}
						
						return
						
					}
					
					 
				}
				
				callback(HaleyStatus.error(sendRes.exceptionType + ' - ' + sendRes.exceptionMessage))
				return
			}
			
			ResultList sendRL = (ResultList) sendRes.response
		
//			send text message status: ERROR - error_denied - Session not found, session: Login_198a52b5-5e99-4626-ad17-f2ef923d7c1c
				
			if(sendRL.status.status != VitalStatus.status_ok) {
				
				callback(HaleyStatus.error(sendRL.status.message))
				return
				
			}
			
			log.info("message sent successfully", sendRL.status.message);
		
			if(updateTimestamp) {
				lastActivityTimestamp = System.currentTimeMillis()
			}
			
			callback(HaleyStatus.ok());
			
		
		}
		
	}
	
	
	public HaleyStatus sendMessage(HaleySession session, VitalObject message) {
		
		throw new Exception("Blocking version not implemented")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		return status
		
		
	}
	
	public void sendMessage(HaleySession session, VitalObject message, Closure callback) {

		this.sendMessage(session, message, [], callback)
		
		
	}
	
	
	
	public HaleyStatus sendMessage(HaleySession session, VitalObject message, List<VitalObject> payload) {
	
		throw new Exception("Blocking version not implemented")
//		HaleyStatus status = new HaleyStatus()
//		
//		
//		return status
	
	
	}

	
	
	// should callbacks be on a per-session basis?
	/*
	public HaleyStatus registerCallback(List<Class<? extends AIMPMessage>> messageTypes, boolean subclasses, Closure callback) {
	
		if(messageTypes == null || messageTypes.size() == 0) throw new Exception("Null or empty messageTypes list")
		
//
//	
//	var e = this._checkSession(haleySession);
//	if(e) {
//		throw e
//	}
	
		for( MessageHandler h : this.handlers) {
		
			if( h.callback == callback ) {
				log.warn("handler already registered ", callback);
				return HaleyStatus.error("handler already registered ");
			}
		}
	
		this.handlers.add(new MessageHandler(
			callback: callback,
			primaryClasses: messageTypes,
			classes: subclasses ? messageTypes : [] 
		))
			
		return HaleyStatus.ok();
	
	}
	*/
	
	/*
	 * Associates callbacks with different message types based on class defined in ontology
	 * 
	 * if subclasses = true, then the callback will apply to all subclasses of the message type(s)
	 * 
	 */
	/*
	public HaleyStatus registerCallback(Class<? extends AIMPMessage> messagetype, boolean subclasses, Closure callback) {
		
		return this.registerCallback([messagetype], subclasses, callback)
		
		
	}
	*/
	
	public HaleyStatus registerDefaultCallback(Closure callback) {
	
		//	var e = this._checkSession(haleySession);
		//	if(e) {
		//		throw e
		//	}
	
		if(callback == null) {
			if(this.defaultHandler == null) {
				return HaleyStatus.error("Default handler not set, cannot deregister");
			} else {
				this.defaultHandler = null;
				return HaleyStatus.ok();
			}
		}
	
		if(this.defaultHandler != null && this.defaultHandler == callback) {
			return HaleyStatus.error("Default handler already set and equal to new one");
		} else {
			this.defaultHandler = callback;
			return HaleyStatus.ok()
		}
		
	}
	
	public HaleyStatus registerRequestCallback(VitalObject aimpMessage, Closure callback) {
		
//		var e = this._checkSession(haleySession);
//		if(e) {
//			throw e
//		}
		
		if(aimpMessage == null) return HaleyStatus.error("null aimpMessage")
		if(aimpMessage.URI() == null) return HaleyStatus.error("null aimpMessage.URI")
		if(callback == null) return HaleyStatus.error("null callback")
		Closure currentCB = this.requestHandlers.get(aimpMessage.URI)
		
		if(currentCB == null || currentCB != callback) {
			this.requestHandlers.put(aimpMessage.URI(), callback);
			return HaleyStatus.ok();
		} else {
			return HaleyStatus.error("This callback already set for this message");
		}
		
	}
	
	public HaleyStatus deregisterCallback(Closure callback) {
		
//		var e = this._checkSession(haleySession);
//		if(e) {
//			throw e
//		}
		
		if(this.defaultHandler != null && this.defaultHandler == callback) {
			this.defaultHandler = null;
			return HaleyStatus.ok();
		}
		
		for( MessageHandler h : this.handlers ) {
			
			if(h.callback == callback) {
				
				this.handlers.remove(h)
				
				return HaleyStatus.ok();
			}
			
		}

		for(Iterator<Entry<String, Closure>> iterator = this.requestHandlers.entrySet().iterator(); iterator.hasNext();) {
			
			if( iterator.next().getValue() == callback ) {
				iterator.remove()
				return HaleyStatus.ok()
			}
			
		}
				
		return HaleyStatus.error("Callback not found");
		
	}
	
	public HaleyStatus uploadBinary(HaleySession session, Channel channel) {
		
		throw new Exception("not implemented yet")
//		HaleyStatus status = new HaleyStatus()
//		
//		
//		return status
		
	}
	
	public void uploadBinary(HaleySession session, Channel channel, Closure callback) {
		
		throw new Exception("not implemented yet")
		
	}
		
	public void uploadFile(HaleySession session, VitalObject questionMessage, VitalObject fileQuestion, File file, Closure callback) {
	
//		if(!scope == 'Public' || scope == 'Private') {
//			callback(HaleyStatus.error("Invalid scope: " + scope + " , expected Public/Private"))
//			return
//		}
		
		String error = this._checkSession(session);
		if(error) {
			callback(HaleyStatus.error(error));
			return;
		}

		def executor = new HaleyFileUploadImplementation()
		executor.callback = callback
		executor.haleyApi = this
		executor.haleySession = session
		executor.questionMsg = questionMessage
		executor.fileQuestion = fileQuestion
//		executor.scope = scope
		executor.file = file
		executor.doUpload()
				
	}
	
	private final static Pattern s3URLPattern = Pattern.compile('^s3\\:\\/\\/([^\\/]+)\\/(.+)$', Pattern.CASE_INSENSITIVE)
	
	public String getFileNodeDownloadURL(HaleySession haleySession, VitalObject fileNode) {
		
		String scope = fileNode.get(vital_fileScope)
		
		if(!scope) scope = 'public';
		
		if('PRIVATE' == scope.toUpperCase()) {
				
			return this.getFileNodeURIDownloadURL(haleySession, fileNode.URI());
			
		} else {
			
			//just convert s3 to public https link
			String fileURL = fileNode.get(vital_fileURL)
			Matcher matcher = s3URLPattern.matcher(fileURL);
			if(matcher.matches()) {
				
				String bucket = matcher.group(1)
				String key = matcher.group(2)
				
				//
				key = key.replace('%', '%25')
				key = key.replace('+', '%2B')
				
				String keyEscaped = key
				
				return 'https://' + bucket + '.s3.amazonaws.com/' + keyEscaped;
				
			}
			
			return fileURL;
			
		}
		
	}
	
	/**
	 * Returns the download URL for given file node URI
	 */
	public String getFileNodeURIDownloadURL(HaleySession haleySession, String fileNodeURI) {
	
		URL websocketURL = this.vitalService.url
		
		int port = websocketURL.getPort()
		boolean secure = websocketURL.getProtocol() == 'https'
		if(secure && port < 0) {
			port = 443
		} else if(port < 0){
			port = 80
		}
		
		String url = websocketURL.getProtocol() + "://" + websocketURL.getHost() 
		if(websocketURL.getPort() > 0) {
			url += ( ":" + websocketURL.getPort() )
		}
		
		url += ( '/filedownload?fileURI=' + URLEncoder.encode(fileNodeURI, 'UTF-8') )
		
		if(haleySession.isAuthenticated()) {
			url += ('&authSessionID=' + URLEncoder.encode(haleySession.getAuthSessionID(), 'UTF-8'))
		} else {
			url += '&sessionID=' + URLEncoder.encode(haleySession.getSessionID(), 'UTF-8');
		}

		return url;
		
		
	}
	
	public HaleyStatus downloadBinary(HaleySession session, String identifier, Channel channel) {
		
		throw new Exception("not implemented yet")
		
//		HaleyStatus status = new HaleyStatus()
//		
//		return status
	}
	
	/*
	 * Includes a test of using GPars for parallelization of calls.
	 * This should be used for all HaleyCallback cases
	 */
	
	public void downloadBinary(HaleySession session, String identifier, Channel channel, Closure callback) {
		
		throw new Exception("not implemented yet")
		
//		GParsPool.withExistingPool(mainPool) {  
//		
//		
//			{ ->
//			
//				HaleyStatus status = new HaleyStatus()
//			
//				for(n in 1..5) {
//			
//					sleep(1000)
//			
//					callback.downloadStatus(status)
//			
//				}
//			
//			}.async().call()
//			
//		}
	}
	
	
	public Collection<VitalObject> listChannels(HaleySession session) {

		throw new Exception("Blocking version not implemented")
		
//		List<AIMPChannel> channels = []
//		
//		return channels
		
	}

	//nodejs <error, list> type	 
	public void listChannels(HaleySession session, Closure callback) {
		
		String error = this._checkSession(session);
		if(error) {
			callback(error, null);
			return;
		}

		VitalObject msg = new VitalObject()
		msg.type = aimp_ListChannelsRequestMessage 
		msg.generateURI()

		Closure requestCallback = { ResultList message ->
		
			callback(null, message.iterator(aimp_Channel).toList());
		
			//remove it always!
			return false;
		
		}

		HaleyStatus status = this.registerRequestCallback(msg, requestCallback)
			
		if( ! status.ok ) {
			callback('couldn\'t register request callback: ' + status.errorMessage, null);
			return;
		}
	
//	this.sendMessageWithRequestCallback(haleySession, aimpMessage, graphObjectsList, callback, requestCallback)
	
		this.sendMessage(session, msg, []) { HaleyStatus sendStatus->
		
			if(!sendStatus.ok) {
				
				String m = "Error when sending list channel request message: " + sendStatus.errorMessage
				
				log.error(m);
				
				callback(m, null);
				
				deregisterCallback(requestCallback)
			}
			
		}
		
	} 
	
	/**
	 * async operation
	 * callback called with String error, List<DomainModel>
	 */
	/*
	public void listServerDomainModels(Closure callback) {

		//the endpoint must also provide simple rest methods to validate domains
		URL websocketURL = this.vitalService.url
		
		int port = websocketURL.getPort()
		boolean secure = websocketURL.getProtocol() == 'https'
		if(secure && port < 0) {
			port = 443
		} else if(port < 0){
			port = 80
		}
		
		def options = [
//			protocolVersion:"HTTP_2",
			ssl: secure,
//			useAlpn:true,
			trustAll:true
		  ]
		
		
		HttpClient client = null
		
		def onFinish = { String error, List<DomainModel> results ->
			
			try {
				if(client != null) client.close()
			} catch(Exception e) {
				log.error(e.localizedMessage, e)
			}
			
			callback(error, results)
			
		}
		
		try {
			
			client = this.vitalService.vertx.createHttpClient(options);
			
			HttpClientRequest request = client.get(port, websocketURL.host, "/domains") { HttpClientResponse response ->

				if( response.statusCode() != 200 ) {
					onFinish("HTTP Status: " + response.statusCode() + " - " + response.statusMessage(), null)
					return
				}

				response.bodyHandler { Buffer body ->


					try {
						List<GraphObject> models = JSONSerializer.fromJSONArrayString(body.toString())
						
						List<DomainModel> filtered = []
						for(GraphObject o : models) {
							if(!(o instanceof DomainModel)) throw new Exception("Expected DomainModels only: " + models.size())
							filtered.add(o) 
						}
						onFinish(null, filtered)
						
					} catch(Exception e) {
						log.error(e.localizedMessage, e)
						onFinish(e.localizedMessage, null)
					}

				}
				
				response.exceptionHandler { Throwable ex ->
					log.error(ex.localizedMessage, ex)
					onFinish(ex.localizedMessage, null)
				}

			}
			
			request.exceptionHandler { Throwable ex ->
				
				log.error(ex.localizedMessage, ex)
				
				onFinish(ex.localizedMessage, null)
				
			}
			
			request.end()
			
		} catch(Exception e) {
			log.error(e.localizedMessage, e)
			onFinish(e.localizedMessage, null)
		}
	
	}
	*/
	
}
