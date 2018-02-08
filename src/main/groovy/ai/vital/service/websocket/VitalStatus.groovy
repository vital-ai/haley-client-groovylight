package ai.vital.service.websocket

class VitalStatus {

	public final static String status_ok = 'ok'
	
	public final static String status_error = 'error'
	
	String status = 'ok'
	
	String message
	
	static VitalStatus withOKMessage(String message) {
		
		VitalStatus status = new VitalStatus()
		
		status.message = message
		
		return status
		
	}
	
	Map<String, Object> toJSON() {
		
		return [status: status, message: message]
		
	}
	
	static VitalStatus fromJSON(Map m) {
		VitalStatus s = new VitalStatus()
		s.message = m.message
		s.status = m.status.name
		return s
	}
	
}
