package ai.vital.service.websocket.binary

class ResponseMessage {

	String exceptionMessage
	
	String exceptionType
	
	Object response

	@Override
	public String toString() {
		return exceptionMessage != null ? "Exception ${exceptionType}: ${exceptionMessage}" : "OK ${response}"
	}


	public ResponseMessage(Object response) {
		super();
		this.response = response;
	}

	public ResponseMessage() {
		super();
	}

	public ResponseMessage(String exceptionMessage, String exceptionType) {
		super();
		this.exceptionMessage = exceptionMessage;
		this.exceptionType = exceptionType;
	}
	
	
}
