/* 

Copyright 2020 aholinch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package org.nograph.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.nograph.GraphQuery;
import org.nograph.NoGraphException;
import org.nograph.Node;
import org.nograph.Relationship;

import jodd.http.HttpBrowser;
import jodd.http.HttpRequest;
import jodd.http.HttpResponse;
import jodd.json.JsonArray;
import jodd.json.JsonObject;
import jodd.json.JsonParser;

public class NGWebGraphManager extends BaseGraphManager 
{
	private static final Logger logger = Logger.getLogger(NGWebGraphManager.class.getName());
	
	/**
	 * The JODD httpbrowser we use as a client.
	 */
    protected HttpBrowser httpClient;
    
    protected String baseURL;
    
    public NGWebGraphManager()
    {
    	init();
    }
    
    protected void init()
    {
    	// TODO Check the config for properties
    	baseURL = "http://localhost:8080/nograph/graph";
    	httpClient = new HttpBrowser();
    }
    
	@Override
	public void saveNode(Node n) throws NoGraphException
	{
		throw new NoGraphException("Read-only Implementation");
	}


	@Override
	public Node getNode(String id) throws NoGraphException {
		Node n = null;
		try
		{
			String url = baseURL+"/node/"+id;
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	n = jsonToNode(resp.body());
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting node",ex);
			throw new NoGraphException(ex);
		}
		return n;
	}

	@Override
	public void ingestNodes(List<Node> nodes) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public void saveNodes(List<Node> nodes) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public void deleteNodesByID(List<String> ids) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public void saveRelationship(Relationship r) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public Relationship getRelationship(String id, boolean fetchNodes) throws NoGraphException {
		Relationship r = null;
		try
		{
			String url = baseURL+"/rel/"+id;
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);
	    	String body = resp.body();
	    	r = jsonToRel(body,fetchNodes);
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting rel",ex);
			throw new NoGraphException(ex);
		}
		return r;
	}

	@Override
	public void ingestRelationships(List<Relationship> rels) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public void saveRelationships(List<Relationship> rels) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public void deleteRelationshipsByID(List<String> ids) throws NoGraphException {
		throw new NoGraphException("Read-only Implementation");
	}

	@Override
	public List<Node> findNodes(String type, String key, Object val, int maxResults) throws NoGraphException {
		boolean tnull = type==null;
		boolean vnull = val == null;
		boolean knull = key == null;
		
		if(tnull && knull) return null;
		
		if(!knull && vnull)
		{
			logger.warning("Unable to search for null values");
			return null;
		}
		
		List<Node> nodes = null;
		try
		{
			String url = baseURL+"/nodes";
			
			if(!tnull)
			{
				url += "/"+type;
			}
			
			if(maxResults > 0)
			{
				if(!url.contains("?"))url+="?";
				url+="max="+maxResults;
			}
			
			if(!knull)
			{
				if(maxResults > 0)
				{
					url+="&";
				}
				else
				{
					url+="?";
				}
				url+=key+"="+String.valueOf(val);
			}
			
	    	HttpRequest req = HttpRequest.get(url);
			
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	nodes = jsonToNodes(resp.body());
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting node",ex);
			throw new NoGraphException(ex);
		}

		return nodes;
	}

	@Override
	public List<Relationship> findRelationships(String type, String key, Object val, boolean fetchNodes, int maxResults)
			throws NoGraphException {
		boolean tnull = type==null;
		boolean vnull = val == null;
		boolean knull = key == null;
		
		if(tnull && knull) return null;
		
		if(!knull && vnull)
		{
			logger.warning("Unable to search for null values");
			return null;
		}
		
		List<Relationship> rels = null;
		try
		{
			String url = baseURL+"/rels";
			
			if(!tnull)
			{
				url += "/"+type;
			}
			
			if(maxResults > 0)
			{
				if(!url.contains("?"))url+="?";
				url+="max="+maxResults;
			}
			
			if(!knull)
			{
				if(maxResults > 0)
				{
					url+="&";
				}
				else
				{
					url+="?";
				}
				url+=key+"="+String.valueOf(val);
			}
			
	    	HttpRequest req = HttpRequest.get(url);
			
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	rels = jsonToRels(resp.body(),fetchNodes);
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting rels",ex);
			throw new NoGraphException(ex);
		}

		return rels;
	}

	@Override
	public List<Node> findNodes(GraphQuery query) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relationship> findRelationships(GraphQuery query) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relationship> findRelatedNodes(String id) throws NoGraphException {
		List<Relationship> rels = null;
		try
		{
			String url = baseURL+"/relsfornode/"+id;
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);
	    	String body = resp.body();
	    	
	    	JsonParser jp = new JsonParser();
	    	JsonObject bigObj = jp.parseAsJsonObject(body);
	    	
	    	JsonObject nodeMap = bigObj.getJsonObject("nodes");
	    	JsonArray arr = bigObj.getJsonArray("rels");
	    	
	    	int size = arr.size();
	    	rels = new ArrayList<Relationship>(size);
	    	Relationship r = null;
	    	Node n = null;
	    	String str = null;
	    	for(int i=0; i<size; i++)
	    	{
	    		str = arr.getJsonObject(i).toString();
	    		r = jsonToRel(str,false,jp);
	    		rels.add(r);
	    		n = r.getNode1();
	    		if(n != null)
	    		{
	    			id = n.getID();
	    			n = jsonToNode(nodeMap.getJsonObject(id).toString(),true,jp);
	    			r.setNode1(n);
	    		}
	    		n = r.getNode2();
	    		if(n != null)
	    		{
	    			id = n.getID();
	    			n = jsonToNode(nodeMap.getJsonObject(id).toString(),true,jp);
	    			r.setNode2(n);
	    		}
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting rels for node " + id,ex);
			throw new NoGraphException(ex);
		}
		return rels;
	}

	@Override
	public long countNodes(String type) throws NoGraphException {
		long count = -1;
		
		// this is lazy but gets the job done
		try
		{
			Map<String,Long> cnt = getNodeCountsByType();
			Long l = cnt.get(type);
			if(l != null)
			{
				count = l;
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting count for type " + type,ex);
		}
		
		return count;
	}

	@Override
	public long countRelationships(String type) throws NoGraphException {
		long count = -1;
		
		// this is lazy but gets the job done
		try
		{
			Map<String,Long> cnt = getRelationshipCountsByType();
			Long l = cnt.get(type);
			if(l != null)
			{
				count = l;
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting count for type " + type,ex);
		}
		
		return count;
	}

	@Override
	public List<String> getNodeTypes() throws NoGraphException {
		List<String> types = null;
		try
		{
			String url = baseURL+"/nodetypes";
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	JsonParser jp = new JsonParser();
	    	
	    	JsonArray arr = jp.parseAsJsonArray(resp.body());
	    	
	    	int size = arr.size();
	    	types = new ArrayList<String>(size);
	    	for(int i=0; i<size; i++)
	    	{
	    		types.add(arr.getString(i));
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting node types",ex);
			throw new NoGraphException(ex);
		}
		return types;
	}

	@Override
	public List<String> getRelationshipTypes() throws NoGraphException {
		List<String> types = null;
		try
		{
			String url = baseURL+"/reltypes";
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	JsonParser jp = new JsonParser();
	    	
	    	JsonArray arr = jp.parseAsJsonArray(resp.body());
	    	
	    	int size = arr.size();
	    	types = new ArrayList<String>(size);
	    	for(int i=0; i<size; i++)
	    	{
	    		types.add(arr.getString(i));
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting relationship types",ex);
			throw new NoGraphException(ex);
		}
		return types;
	}

	@Override
	public Map<String, Long> getNodeCountsByType() throws NoGraphException {
		Map<String,Long> counts = null;
		try
		{
			String url = baseURL+"/nodecounts";
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	JsonParser jp = new JsonParser();
	    	
	    	JsonObject obj = jp.parseAsJsonObject(resp.body());
	    	Map<String,Object> mo = obj.map();
	    	List<String> types = new ArrayList<String>(mo.keySet());
	    	int size = types.size();
	    	counts = new HashMap<String,Long>(size);
	    	
	    	String key = null;
	    	Long cnt = null;
	    	for(int i=0; i<size; i++)
	    	{
	    		key = types.get(i);
	    		cnt = obj.getLong(key);
	    		counts.put(key, cnt);
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting node counts",ex);
			throw new NoGraphException(ex);
		}
		return counts;
	}

	@Override
	public Map<String, Long> getRelationshipCountsByType() throws NoGraphException {
		Map<String,Long> counts = null;
		try
		{
			String url = baseURL+"/relcounts";
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	JsonParser jp = new JsonParser();
	    	
	    	JsonObject obj = jp.parseAsJsonObject(resp.body());
	    	Map<String,Object> mo = obj.map();
	    	List<String> types = new ArrayList<String>(mo.keySet());
	    	int size = types.size();
	    	counts = new HashMap<String,Long>(size);
	    	
	    	String key = null;
	    	Long cnt = null;
	    	for(int i=0; i<size; i++)
	    	{
	    		key = types.get(i);
	    		cnt = obj.getLong(key);
	    		counts.put(key, cnt);
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting rel counts",ex);
			throw new NoGraphException(ex);
		}
		return counts;
	}

	@Override
	public List<String> getPropertyNamesForNodeType(String type) throws NoGraphException {
		List<String> props = null;
		try
		{
			String url = baseURL+"/propsfornode/"+type;
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	JsonParser jp = new JsonParser();
	    	
	    	JsonArray arr = jp.parseAsJsonArray(resp.body());
	    	
	    	int size = arr.size();
	    	props = new ArrayList<String>(size);
	    	for(int i=0; i<size; i++)
	    	{
	    		props.add(arr.getString(i));
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting props for type: " + type,ex);
			throw new NoGraphException(ex);
		}
		return props;
	}

	@Override
	public List<String> getPropertyNamesForRelationshipType(String type) throws NoGraphException {
		List<String> props = null;
		try
		{
			String url = baseURL+"/propsforrel/"+type;
			
	    	HttpRequest req = HttpRequest.get(url);
	    	
	    	HttpResponse resp = httpClient.sendRequest(req);

	    	JsonParser jp = new JsonParser();
	    	
	    	JsonArray arr = jp.parseAsJsonArray(resp.body());
	    	
	    	int size = arr.size();
	    	props = new ArrayList<String>(size);
	    	for(int i=0; i<size; i++)
	    	{
	    		props.add(arr.getString(i));
	    	}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting props for type: " + type,ex);
			throw new NoGraphException(ex);
		}
		return props;	
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByNodeType() throws NoGraphException {
		Map<String,List<String>> out = new HashMap<String,List<String>>();
		try
		{
			List<String> types = getNodeTypes();
			List<String> props = null;
			String type = null;
			int size = types.size();
			for(int i=0; i<size; i++)
			{
				type = types.get(i);
				props = getPropertyNamesForNodeType(type);
				out.put(type, props);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting props for nodes",ex);
			throw new NoGraphException(ex);			
		}
		return out;
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByRelationshipType() throws NoGraphException {
		Map<String,List<String>> out = new HashMap<String,List<String>>();
		try
		{
			List<String> types = getRelationshipTypes();
			List<String> props = null;
			String type = null;
			int size = types.size();
			for(int i=0; i<size; i++)
			{
				type = types.get(i);
				props = getPropertyNamesForRelationshipType(type);
				out.put(type, props);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting props for rels",ex);
			throw new NoGraphException(ex);			
		}
		return out;
	}

	protected Node jsonToNode(String str)
	{
		return jsonToNode(str,true,null);
	}
	
	protected Node jsonToNode(String str, boolean doFull, JsonParser parser)
	{
		GenericNode n = new GenericNode();
		
		if(doFull)
		{
			n.fromJSONString(str);
		}
		else
		{
			JsonObject obj = parser.parseAsJsonObject(str);
			n.setID(obj.getString(GenericNode.ID_KEY));
			n.setType(obj.getString(GenericNode.TYPE_KEY));
		}
		
		return n;
    }

	protected Relationship jsonToRel(String str,boolean fetchNodes)
	{
		return jsonToRel(str,fetchNodes,new JsonParser());
	}
	
	protected Relationship jsonToRel(String str,boolean fetchNodes, JsonParser jp)
	{
		GenericRelationship r = new GenericRelationship();
		
		r.fromJSONString(str);
		
		JsonObject obj = jp.parseAsJsonObject(str);
		JsonObject nobj = null;
		String ns = null;
		Node n = null;
		nobj = obj.getJsonObject("node1");
		if(nobj != null)
		{
			ns = nobj.toString();
			n = jsonToNode(ns,fetchNodes,jp);
			r.setNode1(n);
		}
		nobj = obj.getJsonObject("node2");
		if(nobj != null)
		{
			ns = nobj.toString();
			n = jsonToNode(ns,fetchNodes,jp);
			r.setNode2(n);
		}
		return r;
    }
	
	protected List<Node> jsonToNodes(String str)
	{
		List<Node> nodes = null;
		JsonParser jp = new JsonParser();
		JsonArray arr = jp.parseAsJsonArray(str);
		int size = arr.size();
		nodes = new ArrayList<Node>(size);
		
		Node n = null;
		
		for(int i=0; i<size; i++)
		{
			str = arr.getJsonObject(i).toString();
			n = jsonToNode(str,true,jp);
			nodes.add(n);
		}
		
		return nodes;
	}
	
	protected List<Relationship> jsonToRels(String str, boolean fetchNodes)
	{
		List<Relationship> rels = null;
		JsonParser jp = new JsonParser();
		JsonArray arr = jp.parseAsJsonArray(str);
		int size = arr.size();
		rels = new ArrayList<Relationship>(size);
		
		Relationship r = null;
		
		for(int i=0; i<size; i++)
		{
			str = arr.getJsonObject(i).toString();
			r = jsonToRel(str,fetchNodes,jp);
			rels.add(r);
		}
		
		return rels;
	}

}
