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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.nograph.GraphQuery;
import org.nograph.ID;
import org.nograph.NoGraphException;
import org.nograph.Node;
import org.nograph.Relationship;
import org.nograph.impl.query.BasicStringQueryTranslator;
import org.nograph.impl.query.QueryTranslator;

import es.jodd.client.ElasticClient;
import es.jodd.client.SearchHit;
import es.jodd.client.SearchResults;
import jodd.json.JsonObject;

/**
 * A graphmanager that uses elasticsearch to store the data.
 * 
 * @author aholinch
 *
 */
public class ElasticGraphManager extends BaseGraphManager 
{
	private static final Logger logger = Logger.getLogger(ElasticGraphManager.class.getName());
	
	protected ElasticClient client = null;
	protected String nodeIndex = null;
	protected String relIndex = null;
	protected int defaultMaxHits = 10000;
	
	protected QueryTranslator queryTranslator = null;
	
    public ElasticGraphManager()
    {
    	client = new ElasticClient();
    	nodeIndex = "graphnodes";
    	relIndex = "graphrels";
    	
    	queryTranslator = new BasicStringQueryTranslator(":");
    }
    
	@Override
	public void saveNode(Node n) throws NoGraphException 
	{
		if(n == null) return;
		
		try
		{
			String id = n.getID();
			
			id = client.saveDoc(nodeIndex, n.toJSONString(), id);
			
			if(n.getID() == null)n.setID(id);
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving node",ex);
			throw new NoGraphException(ex);
		}
	}

	@Override
	public Node getNode(String id) throws NoGraphException 
	{
		if(id == null) return null;
		
		Node n = null;
		try
		{
			String json = client.getDoc(nodeIndex, id);

			if(json != null)
			{
				n = new GenericNode();
				n.setID(id);
				n.fromJSONString(json,"_source");
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node",ex);
			//throw new NoGraphException(ex);
		}
		return n;
	}

	@Override
	public void ingestNodes(List<Node> nodes) throws NoGraphException 
	{		
		if(nodes == null || nodes.size() == 0) return;
		
		try
		{
			decorateNodes(nodes);
			
			sampleNodeMeta(nodes);

			int size = nodes.size();
			List<String> jsons = new ArrayList<String>(size);
			for(int i=0; i<size; i++)
			{
				jsons.add(nodes.get(i).toJSONString());
			}
			
			List<String> ids = client.multiCreateDoc(nodeIndex, jsons);
			
			for(int i=0; i<size; i++)
			{
				nodes.get(i).setID(ids.get(i));
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error creating nodes",ex);
			throw new NoGraphException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void saveNodes(List<Node> nodes) throws NoGraphException 
	{
		if(nodes == null || nodes.size() == 0) return;
		
		try
		{
			List<String> ids = new ArrayList<String>(nodes.size());
			
			List<List<? extends ID>> lists = separateNewVsExisting(nodes,ids);
			List<Node> newl = (List<Node>) lists.get(0);
			List<Node> existl = (List<Node>) lists.get(1);
			
			ingestNodes(newl);
			
			if(existl == null || existl.size() == 0) return;
			
			decorateNodes(existl);
			
			sampleNodeMeta(existl);

			int size = nodes.size();
			List<String> jsons = new ArrayList<String>(size);
			
			Node n = null;
			for(int i=0; i<size; i++)
			{
				n = existl.get(i);
				jsons.add(n.toJSONString());
			}
			
			client.multiSaveDoc(nodeIndex, jsons, ids);
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving nodes",ex);
			throw new NoGraphException(ex);
		}
	}

	@Override
	public void deleteNodesByID(List<String> ids) throws NoGraphException {
		// TODO implement delete by query in ElasticClient
		
		if(ids == null || ids.size() == 0) return;
		
		int size = ids.size();
		String id = null;
		
		try
		{
			for(int i=0; i<size; i++)
			{
				id = ids.get(i);
				client.deleteDoc(nodeIndex, id);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error deleting nodes",ex);
			throw new NoGraphException(ex);
		}
	}

	protected String relToJSON(Relationship r)
	{
		return (new JsonObject(r.getMinPropertyMap())).toString();
	}
	
	@Override
	public void saveRelationship(Relationship r) throws NoGraphException 
	{
		if(r == null) return;
		try
		{
			String id = r.getID();
			
			id = client.saveDoc(relIndex, relToJSON(r), id);
			
			if(r.getID() == null)r.setID(id);
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving rel",ex);
			throw new NoGraphException(ex);
		}
	}

	@Override
	public Relationship getRelationship(String id, boolean fetchNodes) throws NoGraphException {
		if(id == null) return null;
		
		Relationship r = null;
		try
		{
			String json = client.getDoc(relIndex, id);

			if(json != null)
			{
				r = new GenericRelationship();
				r.setID(id);
				r.fromJSONString(json,"_source");
				
				if(fetchNodes)
				{
					Node n = null;
					n = getNode(r.getNode1ID());
					r.setNode1(n);
					n = getNode(r.getNode2ID());
					r.setNode2(n);
				}
			}
			
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rel",ex);
			throw new NoGraphException(ex);
		}
		return r;
	}

	@Override
	public void ingestRelationships(List<Relationship> rels) throws NoGraphException {
		if(rels == null || rels.size() == 0) return;
		
		try
		{
			decorateRels(rels);
			
			sampleRelMeta(rels);

			int size = rels.size();
			List<String> jsons = new ArrayList<String>(size);
			for(int i=0; i<size; i++)
			{
				jsons.add(relToJSON(rels.get(i)));
			}
			
			List<String> ids = client.multiCreateDoc(relIndex, jsons);
			
			for(int i=0; i<size; i++)
			{
				rels.get(i).setID(ids.get(i));
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error creating rels",ex);
			throw new NoGraphException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void saveRelationships(List<Relationship> rels) throws NoGraphException {
		if(rels == null || rels.size() == 0) return;
		
		try
		{
			List<String> ids = new ArrayList<String>(rels.size());
			
			List<List<? extends ID>> lists = separateNewVsExisting(rels,ids);
			List<Relationship> newl = (List<Relationship>) lists.get(0);
			List<Relationship> existl = (List<Relationship>) lists.get(1);
			
			ingestRelationships(newl);
			
			if(existl == null || existl.size() == 0) return;
			
			decorateRels(existl);
			
			sampleRelMeta(existl);

			int size = rels.size();
			List<String> jsons = new ArrayList<String>(size);
			
			Relationship r = null;
			for(int i=0; i<size; i++)
			{
				r = existl.get(i);
				jsons.add(relToJSON(r));
			}
			
			client.multiSaveDoc(relIndex, jsons, ids);
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving rels",ex);
			throw new NoGraphException(ex);
		}
	}

	@Override
	public void deleteRelationshipsByID(List<String> ids) throws NoGraphException {
		// TODO implement delete by query in ElasticClient
		
		if(ids == null || ids.size() == 0) return;
		
		int size = ids.size();
		String id = null;
		
		try
		{
			for(int i=0; i<size; i++)
			{
				id = ids.get(i);
				client.deleteDoc(relIndex, id);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error deleting rels",ex);
			throw new NoGraphException(ex);
		}
	}

	protected List<Node> resToNodes(SearchResults res)
	{
		SearchHit hits[] = res.getHits();
		
		int size = hits.length;
		
		List<Node> nodes = new ArrayList<Node>(size);
	
		SearchHit hit = null;
		GenericNode n = null;
		
		for(int i=0; i<size; i++)
		{
			hit = hits[i];
			n = new GenericNode();
			n.fromJSONString(hit.getSource());
			n.setID(hit.getID());
			nodes.add(n);
		}
		return nodes;
	}
	
	@Override
	public List<Node> findNodes(String type, String key, Object val, int maxResults) throws NoGraphException {
		if(key == null && type == null)
		{
			logger.warning("Query cannot be null");
			return null;
		}
		
		String query = "";
		
		List<Node> nodes = null;
		
		if(maxResults <= 0) maxResults = defaultMaxHits;
		
		try
		{
			if(type != null)
			{
				query="type:("+type+")";
			}
			
			if(key != null && val != null)
			{
				if(query.length()>0)query+= " AND ";
				query+=key+":("+val.toString()+")";
			}
			SearchResults res = client.runQueryStringQuery(nodeIndex, query, maxResults);
			
			nodes = resToNodes(res);	
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error running search",ex);
			throw new NoGraphException(ex);
		}
		
		return nodes;
	}

	@Override
	public List<Relationship> findRelationships(String type, String key, Object val, boolean fetchNodes, int maxResults)
			throws NoGraphException {
		if(key == null && type == null)
		{
			logger.warning("Query cannot be null");
			return null;
		}
		
		String query = "";
		
		List<Relationship> rels = null;
		
		if(maxResults <= 0) maxResults = defaultMaxHits;
		
		try
		{
			if(type != null)
			{
				query="type:("+type+")";
			}
			
			if(key != null && val != null)
			{
				if(query.length()>0)query+= " AND ";
				query+=key+":("+val.toString()+")";
			}
			SearchResults res = client.runQueryStringQuery(relIndex, query, maxResults);
			
			rels = resToRels(res,fetchNodes);	
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error running search",ex);
			throw new NoGraphException(ex);
		}
		
		return rels;
	}

	@Override
	public List<Node> findNodes(GraphQuery query) throws NoGraphException {		
		List<Node> nodes = null;
		
		try
		{
			String queryStr = queryTranslator.graphQueryToNativeNode("", query).toString();
			
			logger.info(queryStr);
		
			SearchResults res = client.runQueryStringQuery(nodeIndex, queryStr, query.getMaxResults());
			
			nodes = resToNodes(res);
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error running query",ex);
			throw new NoGraphException("Error running query",ex);
		}
		
		return nodes;
	}

	@Override
	public List<Relationship> findRelationships(GraphQuery query) throws NoGraphException {
		List<Relationship> rels = null;
		
		try
		{
			String queryStr = queryTranslator.graphQueryToNativeRel("", query).toString();
			
			logger.info(queryStr);
		
			SearchResults res = client.runQueryStringQuery(relIndex, queryStr, query.getMaxResults());
			
			rels = resToRels(res, query.getFetchNodesForRelationships());
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error running query",ex);
			throw new NoGraphException("Error running query",ex);
		}
		
		return rels;	
	}

	@Override
	public List<Relationship> findRelatedNodes(String id) throws NoGraphException {
		List<Relationship> rels = null;
		
		try
		{
			String query = "(node1.id="+id+") OR (node2.id="+id+")";
			
			SearchResults res = client.runQueryStringQuery(relIndex, query, defaultMaxHits);
			
			rels = resToRels(res,true);
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting related nodes",ex);
		}
		
		return rels;
	}
	
	protected List<Relationship> resToRels(SearchResults res, boolean fetchNodes) throws NoGraphException
	{
		SearchHit hits[] = res.getHits();
		
		int size = hits.length;
		
		List<Relationship> rels = new ArrayList<Relationship>(size);
	
		SearchHit hit = null;
		GenericRelationship r = null;
		String id = null;
		
		// Keep track of unique ids, sometimes rels comeback twice in these queries
		Map<String,String> idMap = new HashMap<String,String>();
		for(int i=0; i<size; i++)
		{
			hit = hits[i];
			id = hit.getID();
			id= idMap.get(id);
			if(id == null)
			{
				r = new GenericRelationship();
				r.fromJSONString(hit.getSource());
				id = hit.getID();
				r.setID(id);
				idMap.put(id, id);
				rels.add(r);
			}
		}
		
		if(fetchNodes)populateNodesForRels(rels);
		
		return rels;
	}

	@Override
	public long countNodes(String type) throws NoGraphException {
		long out = 0;
		
		try
		{
			SearchResults res = client.runMatchQuery(nodeIndex, "type", type, 0);
			if(res != null) out = res.getTotal();
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting counts",ex);
		}
		return out;
	}

	@Override
	public long countRelationships(String type) throws NoGraphException {
		long out = 0;
		
		try
		{
			SearchResults res = client.runMatchQuery(relIndex, "type", type, 0);
			if(res != null) out = res.getTotal();
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting counts",ex);
		}
		return out;
	}

	@Override
	public List<String> getNodeTypes() throws NoGraphException {
		Map<String,Long> m = getNodeCountsByType();
		
		List<String> types = null;
		if(m != null)
		{
			types = new ArrayList<String>(m.keySet());
			Collections.sort(types);
		}
		
		return types;
	}

	@Override
	public List<String> getRelationshipTypes() throws NoGraphException {
		Map<String,Long> m = getRelationshipCountsByType();
		
		List<String> types = null;
		if(m != null)
		{
			types = new ArrayList<String>(m.keySet());
			Collections.sort(types);
		}
		
		return types;
	}

	@Override
	public Map<String, Long> getNodeCountsByType() throws NoGraphException {
		Map<String,Long> m = null;
		
		try
		{
			m = client.getSimpleAggregate(nodeIndex, "type");
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node type counts",ex);
		}
		
		return m;
	}

	@Override
	public Map<String, Long> getRelationshipCountsByType() throws NoGraphException {
		Map<String,Long> m = null;
		
		try
		{
			m = client.getSimpleAggregate(relIndex, "type");
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rel type counts",ex);
		}
		
		return m;
	}


}
