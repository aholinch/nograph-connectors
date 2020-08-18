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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.TransactionConfig;
import org.nograph.GraphQuery;
import org.nograph.GraphQuery.SimpleCriterion;
import org.nograph.ID;
import org.nograph.NoGraphException;
import org.nograph.Node;
import org.nograph.Relationship;
import org.nograph.util.GraphUtil;

/**
 * Uses the neo4j bolt driver to interact with neo4j graphdb.
 * 
 * @author aholinch
 *
 */
public class Neo4jGraphManager extends BaseGraphManager 
{
	private static final Logger logger = Logger.getLogger(Neo4jGraphManager.class.getName());

	protected String url;
	protected String username;
	protected String password;
	protected Driver driver = null;
	
	private static final String driverSync = "mutex";
	
	public Neo4jGraphManager()
	{
		init();
	}
	
	protected void init()
	{
		// TODO get from config;
		url = "bolt://localhost:7687";
		username = null;
		password = null;
	}
	
	protected Driver getDriver()
	{
		if(driver != null) return driver;
		
		synchronized(driverSync)
		{
			if(driver == null) // yes, check again
			{
				try
				{
					driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));
				}
				catch(Exception ex)
				{
					logger.log(Level.WARNING,"Error getting neo4j driver",ex);
				}
			}
		}
		return driver;
	}
	
	protected void close(Driver driver)
	{
		if(driver != null)
		{
			try
			{
				driver.close();
			}
			catch(Exception ex)
			{
				logger.log(Level.WARNING,"Error closing neo4j driver",ex);
			}
		}
	}
	
	/**
	 * Closes quietly
	 * @param session
	 */
	protected void close(Session session)
	{
		if(session != null)try{session.close();}catch(Exception ex){};
	}
	
	/**
	 * Check the cypher for injection attack????
	 * @param str
	 * @return
	 */
	protected String scrubCypher(String str)
	{
		return str;
	}
	
	protected Relationship recToRel(Record r, int indr, int indn1, int indn2, boolean fetchNodes)
	{
		GenericRelationship rel = new GenericRelationship();
		
		org.neo4j.driver.v1.types.Relationship neoRel = r.get(indr).asRelationship();
		
		rel.setLongID(neoRel.id());
		rel.setType(neoRel.type());
		rel.setPropertyMap(neoRel.asMap());
		
		boolean n1OK = false;
		boolean n2OK = false;
		

		Node n = null;
		
		n = recToNode(r,indn1,fetchNodes);
		if(n != null)
		{
			n1OK = true;
			rel.setNode1(n);
		}
		
		n = recToNode(r,indn2,fetchNodes);
		if(n != null)
		{
			n2OK = true;
			rel.setNode2(n);
		}
		
		if(!n1OK)
		{
			// we can at least recover the id
			n = new GenericNode();
			n.setLongID(neoRel.startNodeId());
			rel.setNode1(n);
		}
	
		if(!n2OK)
		{
			// we can at least recover the id
			n = new GenericNode();
			n.setLongID(neoRel.endNodeId());
			rel.setNode2(n);
		}

		
		return rel;
	}
	
	protected Node recToNode(Record r, int ind, boolean doProps)
	{
		GenericNode n = new GenericNode();
		
		org.neo4j.driver.v1.types.Node node = r.get(ind).asNode();
		
		n.setLongID(node.id());
		
		if(node.labels() != null)
		{
			Iterator<String> iter = node.labels().iterator();
			n.setType(iter.next());
		}
		
		if(doProps)
		{
			n.setPropertyMap(node.asMap());
		}
		
		return n;
	}
	
	protected Map<String,Object> toNeo4jValues(Map<String,Object> m)
	{
		if(m != null)
		{
			List<String> keys = new ArrayList<String>(m.keySet());
			String key = null;
			Object val = null;
			int nk = keys.size();
			for(int i=0; i<nk; i++)
			{
				key = keys.get(i);
				val = m.get(key);
				if(val instanceof java.sql.Timestamp)
				{
					m.put(key, ((java.sql.Timestamp)val).getTime());
				}
			}
		}
		return m;
	}
	
	@Override
	public void saveNode(Node n) throws NoGraphException 
	{
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			
			Map<String,Object> map = toNeo4jValues(n.getPropertyMap());
			Map<String,Object> props = new HashMap<String,Object>();
			props.put("props",map);
			
			String type = n.getType();
			type = scrubCypher(type);

			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			String cypher = "CREATE (n:"+type+" $props) RETURN id(n)";

			String nid = n.getID();
			
			if(nid != null)
			{
				cypher = "MATCH (n:"+type+") where id(n)="+nid+" set n = $props return id(n)";
			}
			
			res = session.run(cypher,props,config);
			
			if(nid == null)
			{
				Record r = null;
				while(res.hasNext())
				{
					r = res.next();
					long id = r.get(0).asLong();
					n.setLongID(id);
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving node",ex);
		}
		finally
		{
			close(session);
		}

	}

	@Override
	public Node getNode(String id) throws NoGraphException 
	{
		//match (n) where id(n)=95 return n;
		Node n = null;
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			id = scrubCypher(id);

			//Map<String,Object> params = new HashMap<String,Object>();
			//params.put("idval", scrubCypher(id));
			//TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			//res = session.run("MATCH (n) where id(n)={idval} return n;",params,config);
			res = session.run("MATCH (n) where id(n)="+id+" return n;");
			
			Record r = null;
			while(res.hasNext())
			{
				r = res.next();
				n = recToNode(r,0,true);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node",ex);
		}
		finally
		{
			close(session);
		}
		
		return n;
	}
	
	@Override
	public void ingestNodes(List<Node> nodes) throws NoGraphException 
	{
		if(nodes == null || nodes.size() == 0) return;
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			
			Map<String,Object> map = null;
			
			Map<String,Object> props = new HashMap<String,Object>();
			
			Map<String,List<Node>> mm = GraphUtil.groupNodesByType(nodes);
			List<String> types = new ArrayList<String>(mm.keySet());
			
			int nt = types.size();
			String type = null;
			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			String cypher = null;
			List<Node> tmp = null;
			int nn = 0;
			Node n = null;
			
			
			for(int i=0; i<nt; i++)
			{
				type = types.get(i);
				
				tmp = mm.get(type);
				nn = tmp.size();
				
				type = scrubCypher(type);
					
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(nn);

				cypher = "UNWIND $props as map CREATE (n:"+type+") SET n = map RETURN id(n)";
	
				for(int j=0; j<nn; j++)
				{
					n = tmp.get(j);
					map = toNeo4jValues(n.getPropertyMap());
					list.add(map);
				}
				
				props.put("props", list);
				
				logger.info(cypher);
				res = session.run(cypher,props,config);
				
				Record r = null;
				int ind = 0;
				while(res.hasNext())
				{
					r = res.next();
					n = tmp.get(ind);
					long id = r.get(0).asLong();
					n.setLongID(id);
					ind++;
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error ingesting nodes",ex);
		}
		finally
		{
			close(session);
		}

	}
	
	/*
	@Override
	public void ingestNodes(List<Node> nodes) throws NoGraphException 
	{
		if(nodes == null || nodes.size() == 0) return;
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			
			Map<String,Object> map = null;
			
			Map<String,Object> props = new HashMap<String,Object>();
			
			Map<String,List<Node>> mm = GraphUtil.groupNodesByType(nodes);
			List<String> types = new ArrayList<String>(mm.keySet());
			
			int nt = types.size();
			String type = null;
			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			String cypher = null;
			List<Node> tmp = null;
			int nn = 0;
			Node n = null;
			
			for(int i=0; i<nt; i++)
			{
				type = types.get(i);
				
				tmp = mm.get(type);
				nn = tmp.size();
				
				type = scrubCypher(type);
					
				cypher = "CREATE (n:"+type+" $props) RETURN id(n)";
	
				for(int j=0; j<nn; j++)
				{
					n = tmp.get(j);
					map = toNeo4jValues(n.getPropertyMap());
					props.put("props",map);
					
					res = session.run(cypher,props,config);
					
					Record r = null;
					if(res.hasNext())
					{
						r = res.next();
						long id = r.get(0).asLong();
						n.setLongID(id);
					}
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error ingesting nodes",ex);
		}
		finally
		{
			close(session);
		}

	}
	*/

	@SuppressWarnings("unchecked")
	@Override
	public void saveNodes(List<Node> nodes) throws NoGraphException 
	{
		if(nodes == null || nodes.size() == 0) return;

		Driver driver = getDriver();
		Session session = null;
		
		try
		{
			List<List<? extends ID>> lists = separateNewVsExisting(nodes, null);
			List<Node> newl = (List<Node>) lists.get(0);
			List<Node> existl = (List<Node>) lists.get(1);
			
			ingestNodes(newl);
			
			if(existl == null || existl.size() == 0) return;
			
			// doesn't affect the source list
			
			nodes = existl;
			 
			session = driver.session();
			
			Map<String,Object> map = null;
			
			Map<String,Object> props = new HashMap<String,Object>();
			
			Map<String,List<Node>> mm = GraphUtil.groupNodesByType(nodes);
			List<String> types = new ArrayList<String>(mm.keySet());
			
			int nt = types.size();
			String type = null;
			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			String cypher = null;
			List<Node> tmp = null;
			int nn = 0;
			Node n = null;
			Map<String,Object> m = null;
			for(int i=0; i<nt; i++)
			{
				type = types.get(i);
				
				tmp = mm.get(type);
				nn = tmp.size();
				type = scrubCypher(type);
					
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(nn);
				
				cypher = "UNWIND $props as row MATCH (n:"+type+") where id(n) = row.nid SET n = row.map RETURN id(n)";
	
				for(int j=0; j<nn; j++)
				{
					n = tmp.get(j);
					map = toNeo4jValues(n.getPropertyMap());
					m = new HashMap<String,Object>();
					m.put("nid", n.getLongID());
					m.put("props", map);
					list.add(m);		
				}
				
				props.put("props", list);
				
				logger.info(cypher);
				session.run(cypher,props,config);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error ingesting nodes",ex);
		}
		finally
		{
			close(session);
		}
	}

	@Override
	public void deleteNodesByID(List<String> ids) throws NoGraphException 
	{
		if(ids == null || ids.size() == 0) return;
		
		String idList = ids.get(0);
		for(int i=1; i<ids.size(); i++)
		{
			idList +=", " + ids.get(i);
		}
		
		String cypher = "match (n) where id(n) in ["+idList+"] detach delete n";
		logger.info(cypher);
		Driver driver = getDriver();
		Session session = null;
		
		try
		{
			session = driver.session();
			
			session.run(cypher);			
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error deleting node",ex);
		}
		finally
		{
			close(session);
		}

	}

	@Override
	public void saveRelationship(Relationship rel) throws NoGraphException {
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			
			Map<String,Object> map = toNeo4jValues(rel.getPropertyMap());
			Map<String,Object> props = new HashMap<String,Object>();
			
			props.put("id1", rel.getNode1().getLongID());
			props.put("id2", rel.getNode2().getLongID());
			props.put("props",map);
			
			String type = rel.getType();
			type = scrubCypher(type);

			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			String cypher = "MATCH (a),(b) WHERE id(a)=$id1 AND id(b)=$id2 CREATE (a)-[r:"+type+"]->(b) set r=$props RETURN id(r)";

			String rid = rel.getID();
			
			if(rid != null)
			{
				cypher = "MATCH ()-[r:"+type+"]-() where id(r)="+rid+" set r = $props return id(r)";
			}
			
			res = session.run(cypher,props,config);
			
			if(rid == null)
			{
				Record r = null;
				while(res.hasNext())
				{
					r = res.next();
					long id = r.get(0).asLong();
					rel.setLongID(id);
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving rel",ex);
		}
		finally
		{
			close(session);
		}

	}

	@Override
	public Relationship getRelationship(String id, boolean fetchNodes) throws NoGraphException {
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		Relationship rel = null;
		try
		{
			session = driver.session();
			id = scrubCypher(id);

			res = session.run("MATCH (n)-[r]-(m) where id(r) = "+id+" return r,n,m;");
			
			Record r = null;
			while(res.hasNext())
			{
				r = res.next();
				rel = recToRel(r,0,1,2,fetchNodes);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rel",ex);
		}
		finally
		{
			close(session);
		}
		
		return rel;
	}

	@Override
	public void ingestRelationships(List<Relationship> rels) throws NoGraphException {
		if(rels == null || rels.size() == 0) return;
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			
			Map<String,Object> map = null;
			
			Map<String,Object> props = new HashMap<String,Object>();
			
			Map<String,List<Relationship>> mm = GraphUtil.groupRelationshipsByType(rels);
			List<String> types = new ArrayList<String>(mm.keySet());
			
			int nt = types.size();
			String type = null;
			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			String cypher = null;
			List<Relationship> tmp = null;
			int nn = 0;
			Relationship rel = null;
			Map<String,Object> m = null;
			for(int i=0; i<nt; i++)
			{
				type = types.get(i);
				
				tmp = mm.get(type);
				nn = tmp.size();
				
				type = scrubCypher(type);
					
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(nn);
				cypher = "UNWIND $props as row MATCH (a),(b) WHERE id(a)=row.id1 AND id(b)=row.id2 CREATE (a)-[r:"+type+"]->(b) set r=row.props RETURN id(r)";
				
				for(int j=0; j<nn; j++)
				{
					rel = tmp.get(j);
					m = new HashMap<String,Object>();
					m.put("id1",rel.getNode1().getLongID());
					m.put("id2",rel.getNode2().getLongID());
					map = toNeo4jValues(rel.getPropertyMap());
					m.put("props", map);
					list.add(m);
				}
				
				props.put("props", list);
				
				logger.info(cypher);
				res = session.run(cypher,props,config);
				
				Record r = null;
				int ind = 0;
				while(res.hasNext())
				{
					r = res.next();
					rel = tmp.get(ind);
					long id = r.get(0).asLong();
					rel.setLongID(id);
					ind++;
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error ingesting rels",ex);
		}
		finally
		{
			close(session);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void saveRelationships(List<Relationship> rels) throws NoGraphException {
		
		Driver driver = getDriver();
		Session session = null;
		
		try
		{
			List<List<? extends ID>> lists = separateNewVsExisting(rels, null);
			List<Relationship> newl = (List<Relationship>) lists.get(0);
			List<Relationship> existl = (List<Relationship>) lists.get(1);
			
			ingestRelationships(newl);
			
			if(existl == null || existl.size() == 0) return;
			
			// doesn't affect the source list
			
			rels = existl;

			session = driver.session();
			
			Map<String,Object> map = null;
			
			Map<String,Object> props = new HashMap<String,Object>();
			
			Map<String,List<Relationship>> mm = GraphUtil.groupRelationshipsByType(rels);
			List<String> types = new ArrayList<String>(mm.keySet());
			
			int nt = types.size();
			String type = null;
			TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			String cypher = null;
			List<Relationship> tmp = null;
			int nn = 0;
			Relationship rel = null;
			Map<String,Object> m = null;
			for(int i=0; i<nt; i++)
			{
				type = types.get(i);
				
				tmp = mm.get(type);
				nn = tmp.size();
				
				type = scrubCypher(type);
					
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(nn);
				cypher = "UNWIND $props as row MATCH (a)-[r:"+type+"]->(b) where id(r)=row.rid set r=row.props RETURN id(r)";
			
				for(int j=0; j<nn; j++)
				{
					rel = tmp.get(j);
					m = new HashMap<String,Object>();
					m.put("rid",rel.getLongID());
					map = toNeo4jValues(rel.getPropertyMap());
					m.put("props", map);
					list.add(m);
				}
				
				props.put("props", list);
				
				logger.info(cypher);
				
				session.run(cypher,props,config);
				
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving rels",ex);
		}
		finally
		{
			close(session);
		}


	}

	@Override
	public void deleteRelationshipsByID(List<String> ids) throws NoGraphException {
		if(ids == null || ids.size() == 0) return;
		
		String idList = ids.get(0);
		for(int i=1; i<ids.size(); i++)
		{
			idList +=", " + ids.get(i);
		}
		
		String cypher = "match ()-[r]-() where id(r) in ["+idList+"] detach delete r";
		logger.info(cypher);
		Driver driver = getDriver();
		Session session = null;
		
		try
		{
			session = driver.session();
			
			session.run(cypher);			
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error deleting rels",ex);
		}
		finally
		{
			close(session);
		}

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
		Node n = null;
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;

		try
		{
			String cypher = "match (n)";
			if(!tnull)
			{
				type = scrubCypher(type);
				cypher = "match (n:"+type+")";
			}
			
			String limit = " LIMIT 25";
			if(maxResults > 0)
			{
				limit = " LIMIT " + maxResults;
			}
			
			Map<String,Object> params = null;
			if(!knull)
			{
				params = new HashMap<String,Object>();
				params.put("param1val", val);
				cypher += " WHERE {param1val} in n."+key;
			}
			
			cypher += " return n"+limit;
			
			session = driver.session();
			
			logger.info(cypher);

			if(params != null)
			{
				TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
				res = session.run(cypher,params,config);
			}
			else
			{
				res = session.run(cypher);
			}
			
			Map<String,String> nids = new HashMap<String,String>();
			String nid = null;
			Record r = null;
			
			nodes = new ArrayList<Node>();
			
			while(res.hasNext())
			{
				r = res.next();
				n = recToNode(r,0,true);
				
				nid = n.getID();
				if(nids.get(nid)==null)
				{
					nodes.add(n);
					nids.put(nid, nid);
				}
			}

		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting nodes",ex);
			throw new NoGraphException(ex);
		}
		finally
		{
			close(session);
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

		Relationship rel = null;
		List<Relationship> rels = null;
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;

		try
		{
			String cypher = "match (n)-[r]-(m)";
			if(!tnull)
			{
				type = scrubCypher(type);
				cypher = "match (n)-[r:"+type+"]-(m)";
			}
			
			String limit = " LIMIT 25";
			if(maxResults > 0)
			{
				limit = " LIMIT " + maxResults;
			}
			
			Map<String,Object> params = null;
			if(!knull)
			{
				params = new HashMap<String,Object>();
				params.put("param1val", val);
				cypher += " WHERE {param1val} in r."+key;
			}
			
			cypher += " return r,n,m"+limit;
			
			session = driver.session();
			
			logger.info(cypher);
			
			if(params != null)
			{
				TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
				res = session.run(cypher,params,config);
			}
			else
			{
				res = session.run(cypher);
			}
			
			Map<String,String> rids = new HashMap<String,String>();
			String rid = null;
			Record r = null;
			rels = new ArrayList<Relationship>();
			while(res.hasNext())
			{
				r = res.next();
				rel = recToRel(r,0,1,2,fetchNodes);
				
				rid = rel.getID();
				if(rids.get(rid)==null)
				{
					rels.add(rel);
					rids.put(rid, rid);
				}
			}

		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting rels",ex);
			throw new NoGraphException(ex);
		}
		finally
		{
			close(session);
		}

		return rels;
	}
	
    protected String queryToCypher(String prefix, GraphQuery query, boolean ignoreType)
    {
    	return null;
    }
    
	@Override
	public List<Node> findNodes(GraphQuery query) throws NoGraphException {
		// TODO support complicated queries
		
		List<Node> nodes = null;
		Node n = null;
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;

		try
		{
			String cypher = "match (n)";
			
			if(query.getCriterion() instanceof SimpleCriterion)
			{
				SimpleCriterion crit = (SimpleCriterion)query.getCriterion();
				if(crit.getKey().equalsIgnoreCase("type"))
				{
					String type = scrubCypher(String.valueOf(crit.getValue()));
					cypher = "match (n:"+type+")";					
				}
			}
			
			String clause = queryToCypher("n",query,true);
			System.out.println(clause);
			
			String limit = " LIMIT 25";
			
			int maxResults = query.getMaxResults();
			if(maxResults > 0)
			{
				limit = " LIMIT " + maxResults;
			}
			
			
			cypher += " return n"+limit;
			
			session = driver.session();
			
			logger.info(cypher);


			res = session.run(cypher);
			
			Map<String,String> nids = new HashMap<String,String>();
			String nid = null;
			Record r = null;
			
			nodes = new ArrayList<Node>();
			
			while(res.hasNext())
			{
				r = res.next();
				n = recToNode(r,0,true);
				
				nid = n.getID();
				if(nids.get(nid)==null)
				{
					nodes.add(n);
					nids.put(nid, nid);
				}
			}

		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error getting nodes",ex);
			throw new NoGraphException(ex);
		}
		finally
		{
			close(session);
		}

		return nodes;
	}

	@Override
	public List<Relationship> findRelationships(GraphQuery query) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relationship> findRelatedNodes(String id) throws NoGraphException 
	{
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		Relationship rel = null;
		List<Relationship> rels = null;
		try
		{
			session = driver.session();
			id = scrubCypher(id);
			//Map<String,Object> params = new HashMap<String,Object>();
			//params.put("idval", scrubCypher(id));
			//TransactionConfig config = TransactionConfig.builder().withTimeout(Duration.ofSeconds(3)).build();
			
			//res = session.run("MATCH (n) where id(n)={idval} return n;",params,config);
			res = session.run("MATCH (n)-[r]-(m) where id(n)="+id+" or id(m) = "+id+" return distinct(r),n,m;");
			
			Map<String,String> rids = new HashMap<String,String>();
			String rid = null;
			Record r = null;
			rels = new ArrayList<Relationship>();
			while(res.hasNext())
			{
				r = res.next();
				rel = recToRel(r,0,1,2,true);
				
				rid = rel.getID();
				if(rids.get(rid)==null)
				{
					rels.add(rel);
					rids.put(rid, rid);
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rels",ex);
		}
		finally
		{
			close(session);
		}
		
		return rels;
	}

	@Override
	public long countNodes(String type) throws NoGraphException {
		if(type == null || type.trim().length() == 0)
		{
			logger.warning("Empty type");
			return 0;
		}
		
		long count = 0;
				
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			type = scrubCypher(type);

			session = driver.session();
			res = session.run("MATCH (n:"+type+") return count(n)");
			
			Record r = null;
			
			if(res.hasNext())
			{
				r = res.next();
				count = r.get(0).asNumber().longValue();
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node type count",ex);
		}
		finally
		{
			close(session);
		}

		return count;
	}

	@Override
	public long countRelationships(String type) throws NoGraphException {
		if(type == null || type.trim().length() == 0)
		{
			logger.warning("Empty type");
			return 0;
		}
		
		long count = 0;
				
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			type = scrubCypher(type);

			session = driver.session();
			res = session.run("MATCH ()-[r:"+type+"]-() return count(r)");
			
			Record r = null;
			
			if(res.hasNext())
			{
				r = res.next();
				count = r.get(0).asNumber().longValue();
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rel type count",ex);
		}
		finally
		{
			close(session);
		}

		return count;
	}

	@Override
	public List<String> getNodeTypes() throws NoGraphException 
	{
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		List<String> types = null;
		
		try
		{
			session = driver.session();
			res = session.run("MATCH (n) WITH DISTINCT labels(n) AS labels UNWIND labels AS label RETURN DISTINCT label ORDER BY label");
			
			Record r = null;
			types = new ArrayList<String>();
			while(res.hasNext())
			{
				r = res.next();
				types.add(r.get(0).asString());
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node types",ex);
		}
		finally
		{
			close(session);
		}
		return types;
	}

	
	@Override
	public List<String> getRelationshipTypes() throws NoGraphException {
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		List<String> types = null;
		
		try
		{
			
			session = driver.session();
			res = session.run("MATCH ()-[r]-() return distinct type(r)");
			
			Record r = null;
			types = new ArrayList<String>();
			while(res.hasNext())
			{
				r = res.next();
				types.add(r.get(0).asString());
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rel types",ex);
		}
		finally
		{
			close(session);
		}
		return types;	
	}

	@Override
	public Map<String, Long> getNodeCountsByType() throws NoGraphException {
		Map<String,Long> counts = new HashMap<String,Long>();
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			
			res = session.run("MATCH (n) WITH DISTINCT labels(n) AS labels UNWIND labels AS label RETURN DISTINCT label ORDER BY label");
			
			Record r = null;
			List<String> types = new ArrayList<String>();
			while(res.hasNext())
			{
				r = res.next();
				types.add(r.get(0).asString());
			}
			
			int nt = types.size();
			String type = null;
			long count = 0;
			
			for(int i=0; i<nt; i++)
			{
				type = types.get(i);
				res = session.run("MATCH (n:"+type+") return count(n)");
							
				if(res.hasNext())
				{
					r = res.next();
					count = r.get(0).asNumber().longValue();
					counts.put(type, count);
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node type count",ex);
		}
		finally
		{
			close(session);
		}	
		
		return counts;
	}

	@Override
	public Map<String, Long> getRelationshipCountsByType() throws NoGraphException 
	{
		Map<String,Long> counts = new HashMap<String,Long>();
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			res = session.run("MATCH ()-[r]-() return distinct type(r), count(r)");
			
			Record r = null;
			String type = null;
			long count = 0;
			while(res.hasNext())
			{
				r = res.next();
				type = r.get(0).asString();
				count = r.get(1).asNumber().longValue();
				counts.put(type, count);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting rel counts",ex);
		}
		finally
		{
			close(session);
		}
		
		return counts;
	}

	@Override
	public List<String> getPropertyNamesForNodeType(String type) throws NoGraphException {
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		List<String> types = null;
		
		try
		{
			type = scrubCypher(type);
			session = driver.session();
			res = session.run("MATCH(n:"+type+") WITH KEYS(n) AS keys UNWIND keys AS key RETURN DISTINCT key");
			
			Record r = null;
			types = new ArrayList<String>();
			while(res.hasNext())
			{
				r = res.next();
				types.add(r.get(0).asString());
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node props",ex);
		}
		finally
		{
			close(session);
		}
		return types;
	}

	@Override
	public List<String> getPropertyNamesForRelationshipType(String type) throws NoGraphException {
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		List<String> types = null;
		
		try
		{
			type = scrubCypher(type);
			session = driver.session();
			res = session.run("MATCH()-[r:"+type+"]-() WITH KEYS(r) AS keys UNWIND keys AS key RETURN DISTINCT key");
			
			Record r = null;
			types = new ArrayList<String>();
			while(res.hasNext())
			{
				r = res.next();
				types.add(r.get(0).asString());
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node props",ex);
		}
		finally
		{
			close(session);
		}
		return types;
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByNodeType() throws NoGraphException {
		/*
		 * MATCH(n) WITH LABELS(n) AS labels , KEYS(n) AS keys UNWIND labels AS label UNWIND keys AS key RETURN DISTINCT label, COLLECT(DISTINCT key) AS props ORDER BY label
		 */
		Map<String,List<String>> out = new HashMap<String,List<String>>();
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			res = session.run("MATCH(n) WITH LABELS(n) AS labels , KEYS(n) AS keys UNWIND labels AS label UNWIND keys AS key RETURN DISTINCT label, COLLECT(DISTINCT key) AS props ORDER BY label");
			
			Record r = null;
			List<String> props = null;
			List<Object> obj = null;
			String type = null;
			while(res.hasNext())
			{
				r = res.next();
				type = r.get(0).asString();
				props = new ArrayList<String>();
				obj = r.get(1).asList();
				if(obj != null)
				{
					int size = obj.size();
					for(int i=0; i<size; i++)props.add(String.valueOf(obj.get(i)));
				}
				out.put(type,props);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node props",ex);
		}
		finally
		{
			close(session);
		}
		
		return out;
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByRelationshipType() throws NoGraphException {
		//
		Map<String,List<String>> out = new HashMap<String,List<String>>();
		
		Driver driver = getDriver();
		Session session = null;
		StatementResult res = null;
		
		try
		{
			session = driver.session();
			res = session.run("MATCH ()-[r]-() WITH type(r) AS label, KEYS(r) AS keys UNWIND keys AS key RETURN DISTINCT label, COLLECT(DISTINCT key) AS props ORDER BY label");
			
			Record r = null;
			List<String> props = null;
			List<Object> obj = null;
			String type = null;
			while(res.hasNext())
			{
				r = res.next();
				type = r.get(0).asString();
				props = new ArrayList<String>();
				obj = r.get(1).asList();
				if(obj != null)
				{
					int size = obj.size();
					for(int i=0; i<size; i++)props.add(String.valueOf(obj.get(i)));
				}
				out.put(type,props);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting node props",ex);
		}
		finally
		{
			close(session);
		}
		
		return out;
	}

}
