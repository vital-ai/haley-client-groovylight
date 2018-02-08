package ai.haley.api.impl

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import ai.haley.api.HaleyAPILite
import ai.haley.api.session.HaleySession
import ai.haley.api.session.HaleyStatus
import ai.vital.service.websocket.VitalObject;

//excutor caches the loginChannel
class HaleyFileUploadImplementation {

	private final static Logger log = LoggerFactory.getLogger(HaleyFileUploadImplementation.class)
	
	HaleyAPILite haleyApi
	
	HaleySession haleySession
	
//	String scope
	
	File file
	
	Closure callback
	
	VitalObject login
	 
//	Channel loginChannel
	
	VitalObject questionMsg
	
	VitalObject fileQuestion
	

	public void doUpload() {

		callback(HaleyStatus.error("NOT IMPLEMENTED!"))
		
//		onFileQuestionMsg()
		
	}
	
	/*
	public void onFileQuestionMsg() {
		
		String fileNodeClass = 'http://vital.ai/ontology/vital#FileNode'
	
		String parentNodeURI = null
	
		URL websocketURL = haleyApi.vitalService.url
	
	
		String url = '/fileupload/'
		//    	url += '?fileNodeClass=' + encodeURIComponent(fileNodeClass);
		url += '?temporary=true'
		//    url += '&scope=' + scope;
		url += '&authSessionID=' + URLEncoder.encode(haleySession.getAuthSessionID(), 'UTF-8');
		
		
		url += '&multipart=false'
		
		url += '&fileName=' + URLEncoder.encode(file.getName(), 'UTF-8')
			
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
	
	def onFinish = { String error, Map data ->
		
		
		try {
			if(client != null) client.close()
		} catch(Exception e) {
			log.error(e.localizedMessage, e)
		}
		
		if(error) {
			log.error("upload server error: " + error)
			callback(HaleyStatus.error(error))
			return
		}
		
		log.info('file data received: {}', data);

		AnswerMessage am = new AnswerMessage()
		am.generateURI((VitalApp) null)
		am.replyTo = questionMsg.URI
		am.channelURI = questionMsg.channelURI
		am.endpointURI = questionMsg.endpointURI
				
		FileAnswer fa = new FileAnswer()
		fa.generateURI((VitalApp) null)
		fa.fileNodeClassURI = fileNodeClass
		fa.parentObjectURI = parentNodeURI
		fa.url = data.url
		fa.fileName = data.fileName
		fa.fileType = data.fileType
		fa.fileLength = data.fileLength
		fa.deleteOnSuccess = true
		
		haleyApi.sendMessageWithRequestCallback(haleySession, am, [fa], { ResultList msgRL ->
			
			AIMPMessage msg = msgRL.first()
			
			if(!(msg instanceof MetaQLResultsMessage)) {
				log.warn("Not a results message, ignoring")
				return true
			}
			
			String status = msg.status
			
			if(!"ok".equalsIgnoreCase(status)) {
				
				String statusMessage = msg.statusMessage
				if(!statusMessage) statusMessage = 'unknow file upload error';
				callback(HaleyStatus.error(statusMessage))
				return false
				
			}
			
			List<FileNode> fileNodes = msgRL.iterator(FileNode.class).toList()
			
			FileNode fileNode = null
			
			if(fileNodes.size() > 0) {
				fileNode = fileNodes.get(0)
			}
			
			if(fileNode == null) {
				callback(HaleyStatus.error('no file node in the response message'))
				return false
			}
			
			callback(HaleyStatus.okWithResult(fileNode))
			
			return false;
			
		}, { HaleyStatus sendStatus ->
			
			if(!sendStatus.isOk()) {
				callback(HaleyStatus.error("Error when sending file answer: " +sendStatus.errorMessage, null))
				return
			}
			
			log.info("file answer message sent")
			
		})
		
	}
	
	
	try {
		
		client = haleyApi.vitalService.vertx.createHttpClient(options);
		
		HttpClientRequest request = client.post(port, websocketURL.host, url) { HttpClientResponse response ->

			if( response.statusCode() != 200 ) {
				onFinish("HTTP Status: " + response.statusCode() + " - " + response.statusMessage(), null)
				return
			}

			response.bodyHandler { Buffer body ->

				try {
					
					def resp = new JsonSlurper().parseText(body.toString())
					if(!(resp instanceof Map)) throw new Exception("Expected json object as a response")
					
					if(resp.error) {
						onFinish(resp.error, null)
						return 
					} 
					
					onFinish(null, resp)
					
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
		
		haleyApi.vitalService.vertx.fileSystem().props(file.getAbsolutePath()) { AsyncResult<FileProps> filePropsRes ->

			if(filePropsRes.failed()) {
				log.error(filePropsRes.cause().getLocalizedMessage(), filePropsRes.cause())
				onFinish(filePropsRes.cause().getLocalizedMessage(), null)
				return
			}
			
			FileProps fileProps = filePropsRes.result()
			
			if( !fileProps.isRegularFile() ) {
				onFinish("Path is not a regular file: " + file.getAbsolutePath(), null)
				return
			}
			
			haleyApi.vitalService.vertx.fileSystem().open(file.getAbsolutePath(), [
				read: true,
				write: false,
				create: false,
				createNew: false
			]) { AsyncResult<AsyncFile> asyncFileRes ->
	
				if(asyncFileRes.failed()) {
					log.error(asyncFileRes.cause().getLocalizedMessage(), asyncFileRes.cause())
					onFinish(asyncFileRes.cause().getLocalizedMessage(), null)
					return
				}
			
				AsyncFile asyncFile = asyncFileRes.result()
				
				
				String contentType = null
				
				try {
					Path source = Paths.get(file.getAbsolutePath());
					contentType = Files.probeContentType(source)
				} catch(Exception e) {
					log.warn("Error when detecting file content type: " + file.getAbsolutePath() + ": " + e.localizedMessage)
				}
						
				if(!contentType) {
					contentType = 'application/octet-stream'
				}
				
				request.headers().set(HttpHeaders.CONTENT_TYPE.toString(), contentType)
				request.headers().set(HttpHeaders.CONTENT_LENGTH.toString(), "" + fileProps.size())
				
				Pump pump = Pump.pump(asyncFile, request)
				
				asyncFile.endHandler { Void v ->
					request.end();
				}
				
				pump.start()
			}
	
		}
		
		
	} catch(Exception e) {
		log.error(e.localizedMessage, e)
		onFinish(e.localizedMessage, null)
	}

}
*/
}
