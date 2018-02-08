package ai.vital.service.websocket

import java.util.Map;

class ResultList {

	VitalStatus status = VitalStatus.withOKMessage()
	
	List results = []
	
	Map<String, Object> toJSON() {
		
		return [status: status.toJSON(), results: results]
		
	}
	
	void addResult(VitalObject object) {
		
		this.results.add([score: 1d, graphObject: object])
		
	}
	
	VitalObject first() {
		if(results.size() > 0) {
			return results[0].graphObject
		}
		return null
	}
	
	List<VitalObject> toList() {
		
		List out = []
		
		for(Object r : results) {
			VitalObject o = r.graphObject
			out.add(o)
		}

		return out
		
	}
	
	Iterable<VitalObject> iterator(String typeFilter) {
		
		List out = []
		
		for(Object r : results) {
			VitalObject o = r.graphObject
			if( o.type == typeFilter ) {
				out.add(o)
			}
		}

		return out		
		
	}
	
	static ResultList fromJSON(Map m) {
		
		ResultList r = new ResultList()
		
		r.status = VitalStatus.fromJSON(m.status)
		
		if(m.results != null) {
			
			for(Map res : m.results) {
			
				Map o = new HashMap(res)
				
				o.graphObject = new VitalObject(res.graphObject)
				
				r.results.add(o)	
				
			}
			
		}
		
		return r
		
	}
	
}
