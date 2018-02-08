package ai.vital.service.websocket

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Asynchronous version of vital service on vertx
 * All closures are called with {@link ai.vital.service.vertx.binary.ResponseMessage}
 * @author Derek
 *
 */
abstract class VitalServiceAsyncClientBase {

	private final static Logger log = LoggerFactory.getLogger(VitalServiceAsyncClientBase.class)
	
	String address = null
	
	/**
	 * subclasses should set the address
	 */
	protected VitalServiceAsyncClientBase() {
	}

	/**
	 * must call closure with response message
	 */
	abstract protected void impl(Closure closure, String method, List args)	
	
//	bulkExport(VitalSegment, OutputStream)
//	bulkImport(VitalSegment, InputStream)

	//ResultList
	void callFunction(String function, Map<String, Object> arguments, Closure closure) {
		impl(closure, 'callFunction', [function, arguments])
	}
	
	
	//VitalStatus
	void close(Closure closure) {
		impl(closure, 'close', [])
	}
	
	/*
	//VitalStatus
	void commitTransaction(VitalTransaction transaction, Closure closure) {
		impl(closure, 'commitTransaction', [transaction])
	}
	
	//VitalTransaction
	void createTransaction(Closure closure) {
		impl(closure, 'createTransaction', [])
	}

	
	//VitalStatus
	void delete(List<URIProperty> uris, Closure closure) {
		impl(closure, 'delete', [NO_TRANSACTION, uris]) 
	}
	
	//VitalStatus
	void delete(VitalTransaction transaction, List<URIProperty> uris, Closure closure) {
		impl(closure, 'delete', [transaction, uris]) 
	}
	
	
	//VitalStatus
	void delete(URIProperty uri, Closure closure) {
		impl(closure, 'delete', [NO_TRANSACTION, uri])
	}
	
	//VitalStatus
	void delete(VitalTransaction transaction, URIProperty uri, Closure closure) {
		impl(closure, 'delete', [transaction, uri])
	}
	

	//VitalStatus
	void deleteExpanded(List<URIProperty> uris, VitalPathQuery pathQuery, Closure closure) {
		impl(closure, 'deleteExpanded', [NO_TRANSACTION, uris, pathQuery])
	}
	
	//VitalStatus
	void deleteExpanded(VitalTransaction transaction, List<URIProperty> uris, VitalPathQuery pathQuery, Closure closure) {
		impl(closure, 'deleteExpanded', [transaction, uris, pathQuery])
	}
	

	//VitalStatus	
	void deleteExpanded(URIProperty uri, Closure closure) {
		impl(closure, 'deleteExpanded', [NO_TRANSACTION, uri])
	}
	
	//VitalStatus
	void deleteExpanded(VitalTransaction transaction, URIProperty uri, Closure closure) {
		impl(closure, 'deleteExpanded', [transaction, uri])
	}
	
	
	//VitalStatus
	void deleteExpanded(URIProperty uri, VitalPathQuery pathQuery, Closure closure) {
		impl(closure, 'deleteExpanded', [NO_TRANSACTION, uri, pathQuery])
	}
	
	//VitalStatus
	void deleteExpanded(VitalTransaction transaction, URIProperty uri, VitalPathQuery pathQuery, Closure closure) {
		impl(closure, 'deleteExpanded', [transaction, uri, pathQuery])
	}
	
	
	//VitalStatus
	void deleteExpandedObject(GraphObject graphObject, Closure closure) {
		impl(closure, 'deleteExpandedObject', [NO_TRANSACTION, graphObject])
	}
	
	//VitalStatus
	void deleteExpandedObjects(VitalTransaction transaction, List<GraphObject> graphObjects, VitalPathQuery pathQuery, Closure closure) {
		impl(closure, 'deleteExpandedObjects', [transaction, graphObjects, pathQuery])
	}
	
	
	//VitalStatus
	void deleteFile(URIProperty uri, String name, Closure closure) {
		impl(closure, 'deleteFile', [uri, name])
	}
	
	
	//VitalStatus
	void deleteObject(GraphObject graphObject, Closure closure) {
		impl(closure, 'deleteObject', [NO_TRANSACTION, graphObject])
	}
	
	//VitalStatus
	void deleteObject(VitalTransaction transaction, GraphObject graphObject, Closure closure) {
		impl(closure, 'deleteObject', [transaction, graphObject])
	}
	
	
	//VitalStatus
	void deleteObjects(List<GraphObject> graphObjects, Closure closure) {
		impl(closure, 'deleteObjects', [NO_TRANSACTION, graphObjects])
	}
	
	//VitalStatus
	void deleteObjects(VitalTransaction transaction, List<GraphObject> graphObjects, Closure closure) {
		impl(closure, 'deleteObjects', [transaction, graphObjects])
	}
	
	
	void doOperations(ServiceOperations serviceOps, Closure closure) {
		impl(closure, 'doOperations', [serviceOps])
	}
	
	//VitalStatus
	void downloadFile(URIProperty uri, String name, String localFilePath, boolean closeOutputStream, Closure closure) {
		impl(closure, "downloadFile", [uri, name, localFilePath, closeOutputStream])
	}
	
	//VitalStatus
	void fileExists(URIProperty uri, String name, Closure closure) {
		impl(closure, 'fileExists', [uri, name])
	}
	
	void generateURI(Class<? extends GraphObject> clazz, Closure closure) {
		impl(closure, 'generateURI', [clazz])
	}
	
	void get(GraphContext graphContext, List<URIProperty> uris, Closure closure) {
		impl(closure, 'get', [graphContext, uris])
	}
	
	void get(GraphContext graphContext, List<URIProperty> uris, boolean cache, Closure closure) {
		impl(closure, 'get', [graphContext, uris, cache])
	}
	
	
//	void get(GraphContext graphContext, List<URIProperty> uris, List<GraphObjectsIterable> containers, Closure closure) {
//		impl(closure, 'get', [graphContext, uris, containers])
//	}
	
	
	void get(GraphContext graphContext, URIProperty uri, Closure closure) {
		impl(closure, 'get', [graphContext, uri])
	} 
	
	void get(GraphContext graphContext, URIProperty uri, boolean cache, Closure closure) {
		impl(closure, 'get', [graphContext, uri, cache])
	}
	
//	void get(GraphContext graphContext, URIProperty uri, List<GraphObjectsIterable> containers, Closure closure) {
//		impl(closure, 'get', [graphContext, uri, containers])
//	}
	
	void getApp(Closure closure) {
		impl(closure, 'getApp', [])
	}
	
	void getDefaultSegmentName(Closure closure) {
		impl(closure, 'getDefaultSegmentName', [])
	}
	
	void getEndpointType(Closure closure) {
		impl(closure, 'getEndpointType', [])
	}
	
	void getExpanded(URIProperty uri, boolean cache, Closure closure) {
		impl(closure, 'getExpanded', [uri, cache])
	}
	
	void getExpanded(URIProperty uri, VitalPathQuery pathQuery, boolean cache, Closure closure) {
		impl(closure, 'getExpanded', [uri, pathQuery, cache])
	}
	
	//String
	void getName(Closure closure) {
		impl(closure, 'getName', [])
	}
	
	void getOrganization(Closure closure) {
		impl(closure, 'getOrganization', [])
	}
	
	
	//VitalSegment
	void getSegment(String segmentID, Closure closure) {
		impl(closure, 'getSegment', [segmentID])
	}
	
	void getTransactions(Closure closure) {
		impl(closure, 'getTransactions', [])
	}
	
	
	//ResultList
	void insert(VitalSegment segment, GraphObject graphObject, Closure closure) {
		impl(closure, 'insert', [NO_TRANSACTION, segment, graphObject])
	}
	
	//ResultList
	void insert(VitalTransaction transaction, VitalSegment segment, GraphObject graphObject, Closure closure) {
		impl(closure, 'insert', [transaction, segment, graphObject])
	}
	
	
	//ResultList
	void insert(VitalSegment segment, List<GraphObject> graphObjects, Closure closure) {
		impl(closure, 'insert', [NO_TRANSACTION, segment, graphObjects])
	}
	
	//ResultList
	void insert(VitalTransaction transaction, VitalSegment segment, List<GraphObject> graphObjects, Closure closure) {
		impl(closure, 'insert', [transaction, segment, graphObjects])
	}
	
	
	//ResultList
	void listDatabaseConnections(Closure closure) {
		impl(closure, 'listDatabaseConnections', [])
	}
	
	//ResultList
	void listFiles(String filePath, Closure closure) {
		impl(closure, 'listFiles', [filePath])
	}
	
	
	//List<VitalSegment>
	void listSegments(Closure closure) {
		impl(closure, 'listSegments', [])
	}
	
	//ResultList
	void listSegmentsWithConfig(Closure closure) {
		impl(closure, 'listSegmentsWithConfig', [])
	}
	
	*/
	
	//VitalStatus
	void ping(Closure closure) {
		impl(closure, 'ping', [])
	}

	/*
	//VitalPipeline ai.vital.vitalservice.VitalService.pipeline(Closure<?>)
		
	void query(VitalQuery query, Closure closure) {
		impl(closure, 'query', [query])
	}
	
//	queryContainers(VitalQuery, List<GraphObjectsIterable>, Closure closure)
	
	void queryLocal(VitalQuery query, Closure closure) {
		impl(closure, 'queryLocal', [query])
	}
	
	void rollbackTransaction(VitalTransaction transaction, Closure closure) {
		impl(closure, 'rollbackTransaction', [transaction])
	}

	
	//ResultList	
	void save(GraphObject graphObject, Closure closure) {
		impl(closure, 'save', [NO_TRANSACTION, graphObject])
	}
	
	//ResultList	
	void save(VitalTransaction transaction, GraphObject graphObject, Closure closure) {
		impl(closure, 'save', [transaction, graphObject])
	}
	
	
	//ResultList	
	void save(List<GraphObject> graphObjects, Closure closure) {
		impl(closure, 'save', [NO_TRANSACTION,graphObjects])
	}
	//ResultList	
	void save(VitalTransaction transaction, List<GraphObject> graphObjects, Closure closure) {
		impl(closure, 'save', [transaction, graphObjects])
	}
	
	
	//ResultList
	void save(VitalSegment segment, GraphObject graphObject, boolean create, Closure closure) {
		impl(closure, 'save', [NO_TRANSACTION, segment, graphObject, create])
	}
	
	//ResultList
	void save(VitalTransaction transaction, VitalSegment segment, GraphObject graphObject, boolean create, Closure closure) {
		impl(closure, 'save', [transaction, segment, graphObject, create])
	}
	
	
	//ResultList
	void save(VitalSegment segment, List<GraphObject> graphObjects, boolean create, Closure closure) {
		impl(closure, 'save', [NO_TRANSACTION, segment, graphObjects, create])
	}
	
	//ResultList
	void save(VitalTransaction transaction, VitalSegment segment, List<GraphObject> graphObjects, boolean create, Closure closure) {
		impl(closure, 'save', [transaction, segment, graphObjects, create])
	}
	
	
	void sendEvent(VITAL_Event event, boolean waitForDelivery, Closure closure) {
		impl(closure, 'sendEvent', [event, waitForDelivery])
	}
	
	
	void sendEvents(List<VITAL_Event> events, boolean waitForDelivery, Closure closure) {
		impl(closure, 'sendEvents', [events, waitForDelivery])
	}
	
	
	void setDefaultSegmentName(String defaultSegment, Closure closure) {
		impl(closure, 'setDefaultSegmentName', [defaultSegment])
	}
	
	//VitalStatus
	void uploadFile(URIProperty uri, String name, String localFilePath, boolean overwrite, Closure closure) {
		impl(closure, "uploadFile", [uri, name, localFilePath, overwrite])
	}
	
	void validate(Closure closure) {
		impl(closure, 'validate', [])
	}
	
	*/
	
}
