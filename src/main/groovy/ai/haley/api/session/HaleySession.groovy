package ai.haley.api.session

import ai.vital.service.websocket.VitalObject
import ai.vital.service.websocket.VitalServiceAsyncWebsocketLiteClient;;

class HaleySession {

	boolean authenticated = false
	
	String authSessionID

	String sessionID
	
	VitalObject authAccount
	
	public String toString() {
		String s = "HaleySession sessionID: ${sessionID} authenticated ? ${authenticated}"
		if(authenticated) {
			s += " authSessionID: ${authSessionID} authAccount: ${VitalServiceAsyncWebsocketLiteClient.toJson(authAccount)}"
		}
		return s
	}
}
