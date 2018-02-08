package ai.vital.service.websocket

import java.util.HashMap
import java.util.Map;

class VitalObject extends HashMap<String, Object> {

	public VitalObject() {
		super();
	}

	public VitalObject(Map<String, Object> m) {
		super(m);
	}

	public static String orgID = 'vital.ai'
	
	public static String appID = 'app'
	
	public static int counter = 0
	
	public void setType(String type) {
		this.put("type", type)
	}
	
	public String getType() {
		return this.get("type")
	} 
	
	public String URI() {
		return this.get("URI");
	}
	
	public void URI(String u) {
		if(u) {
			this.put("URI", u)
		} else {
			this.remove("URI")
		}
	}
	
	public String generateURI() {
	
		if(! type) throw new RuntimeException("type not set!");
		
		
		int i = type.lastIndexOf("#");
		
		if(i < 0 || i >= type.length() - 1) throw new RuntimeException("The type does not seem to be a full URI: " + type)
		
		String localName = type.substring(i)	
		
		String rp = System.currentTimeMillis() + "_" + counter++ /*:  r.nextInt(1000000))*/;
		
		String uri = "http://vital.ai/" + orgID + "/" + appID + "/" + localName + "/" /*+ (_transient ? "transient/" : "")*/ + rp; 
		
		this.put("URI", uri);
		 
		return uri
		
	}
	
}
