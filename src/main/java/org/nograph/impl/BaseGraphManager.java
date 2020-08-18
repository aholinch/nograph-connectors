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

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.nograph.DataDecorator;
import org.nograph.GraphManager;
import org.nograph.ID;
import org.nograph.NoGraph;
import org.nograph.NoGraphConfig;
import org.nograph.NoGraphException;
import org.nograph.Node;
import org.nograph.Path;
import org.nograph.Relationship;
import org.nograph.GraphQuery.Criterion;
import org.nograph.util.FileUtil;

/**
 * The base implementation that handles most convenience operations leaving only core functions for the actual implementation.
 * 
 * @author aholinch
 *
 */
public abstract class BaseGraphManager implements GraphManager 
{
	private static final Logger logger = Logger.getLogger(BaseGraphManager.class.getName());

	public static final String PROP_MD = "meta.dir";

    protected String name = null;
    
	// decorator
	protected DataDecorator decorator = null;
	protected boolean decorateNodes = false;
	protected boolean decorateRels = false;

	// meta info
	protected GraphMeta graphMeta = null;
	protected String metaFile = null;
	protected static String metasync = "mutex";
	
	public BaseGraphManager()
	{
		
		graphMeta = new GraphMeta();
		NoGraphConfig config = NoGraph.getInstance().getConfig();
		String metaDir  = config.getProperty(PROP_MD);
		try
		{
			if(metaDir == null) metaDir = "";
			File md = new File(metaDir);
			md.mkdirs();
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error creating meta dir");
		}
		
		metaFile = metaDir+"graphmeta.json";
		loadGraphMeta();
	}
	
	@Override
	public void setName(String str) 
	{
		name = str;
	}

	@Override
	public String getName() 
	{
		return name;
	}
	
	@Override
	public void deleteNode(Node n) throws NoGraphException 
	{
		if(n == null) return;
		if(n.getID() == null) throw new NoGraphException("No node id");

		deleteNode(n.getID());
	}

	@Override
	public void deleteNode(String id) throws NoGraphException {
		if(id == null || id.trim().length() == 0) return;
		
		List<String> ids = new ArrayList<String>();
		ids.add(id);
		
		deleteNodesByID(ids);
	}

	@Override
	public void deleteNodes(List<Node> nodes) throws NoGraphException 
	{
		if(nodes == null || nodes.size() == 0) return;
		int size = nodes.size();
		List<String> ids = new ArrayList<String>(size);
		String id = null;
		for(int i=0; i<size; i++)
		{
			id = nodes.get(i).getID();
			if(id != null)
			{
				ids.add(id);
			}
		}
		
		deleteNodesByID(ids);
	}


	@Override
	public void deleteRelationship(Relationship r) throws NoGraphException 
	{
		if(r == null) return;
		if(r.getID() == null) throw new NoGraphException("No rel id");

		deleteRelationship(r.getID());
	}
	

	@Override
	public void deleteRelationship(String id) throws NoGraphException {
		if(id == null || id.trim().length() == 0) return;
		
		List<String> ids = new ArrayList<String>();
		ids.add(id);
		
		deleteRelationshipsByID(ids);
	}


	@Override
	public void deleteRelationships(List<Relationship> rels) throws NoGraphException 
	{
		if(rels == null || rels.size() == 0) return;
		int size = rels.size();
		List<String> ids = new ArrayList<String>(size);
		String id = null;
		for(int i=0; i<size; i++)
		{
			id = rels.get(i).getID();
			if(id != null)
			{
				ids.add(id);
			}
		}
		
		deleteRelationshipsByID(ids);
	}

	@Override
	public List<Node> findNodes(String key, Object val) throws NoGraphException {
		return findNodes(null,key,val,0);
	}

	@Override
	public List<Node> findNodes(String type, String key, Object val) throws NoGraphException {
		return findNodes(type,key,val,0);
	}

	@Override
	public List<Relationship> findRelationships(String key, Object val, boolean fetchNodes) throws NoGraphException {
		return findRelationships(null,key,val,fetchNodes,0);
	}

	@Override
	public List<Relationship> findRelationships(String type, String key, Object val, boolean fetchNodes)
			throws NoGraphException {
		return findRelationships(type,key,val,fetchNodes,0);
	}

	@Override
	public List<Relationship> findRelatedNodes(Node n) throws NoGraphException 
	{
		return findRelatedNodes(n.getID());
	}
	
	/**
	 * Implement this method to do a bulk query by node ids
	 * 
	 * @param ids
	 * @return
	 */
	protected Map<String,Node> buildNodeMap(List<String> ids)
	{
		return null;
	}
	
	/**
	 * The relationships have node ids, now let's turn them into full nodes.
	 * 
	 * @param rels
	 * @throws NoGraphException
	 */
	protected void populateNodesForRels(List<Relationship> rels) throws NoGraphException
	{
		if(rels == null || rels.size() == 0) return;
		
		Map<String,Node> nodeMap = new HashMap<String,Node>();
		
		int size = rels.size();
		Node n = null;
		String id = null;
		Relationship r = null;
		
		Map<String,String> mids = new HashMap<String,String>();
		
		String hold = "";
		
		// first pass is to build ids
        for(int i=0; i<size; i++)
        {
        	r = rels.get(i);
        	mids.put(r.getNode1ID(),hold);
        	mids.put(r.getNode2ID(),hold);
        }
        
        List<String> ids = new ArrayList<String>(mids.keySet());
        nodeMap = buildNodeMap(ids);
        if(nodeMap == null)
        {
        	nodeMap = new HashMap<String,Node>();
        }
        
        for(int i=0; i<size; i++)
        {
        	r = rels.get(i);

        	// we should do a bulk query, which is what buildNodeMap does, but slow and steady will win the race for now
        	id = r.getNode1ID();
        	n = nodeMap.get(id);
        	if(n == null)
        	{
        		n = getNode(id);
        		nodeMap.put(id, n);
        	}
        	r.setNode1(n);
        	
        	id = r.getNode2ID();
        	n = nodeMap.get(id);
        	if(n == null)
        	{
        		n = getNode(id);
        		nodeMap.put(id, n);
        	}
        	r.setNode2(n);
        }
	}


	@Override
	public void setDataDecorator(DataDecorator decorator) 
	{
		this.decorator = decorator;
		if(decorator != null)
		{
			this.decorateNodes = decorator.decoratesNodes();
			this.decorateRels = decorator.decoratesRelationships();
		}
	}

	@Override
	public DataDecorator getDataDecorator() 
	{
		return decorator;
	}

	@Override
	public void clearDataDecorator() 
	{
		decorator = null;
	}

	protected void decorateNode(Node n)
	{
		if(!decorateNodes) return;
		decorator.decorateNode(n);
	}

	protected void decorateRel(Relationship r)
	{
		if(!decorateRels) return;
		decorator.decorateRelationship(r);
	}

	protected void decorateNodes(List<Node> nodes)
	{
		if(!decorateNodes) return;
		Node n = null;
		int size = nodes.size();
		for(int i=0; i<size; i++)
		{
			n = nodes.get(i);
			decorator.decorateNode(n);
		}
	}

	protected void decorateRels(List<Relationship> rels)
	{
		if(!decorateRels) return;
		Relationship r = null;
		int size = rels.size();
		for(int i=0; i<size; i++)
		{
			r = rels.get(i);
			decorator.decorateRelationship(r);
		}
	}
	
	/**
	 * Separate a list of ID objects into new (ID=null) and existing (ID!=null).
	 * 
	 * @param objs
	 * @param idList
	 * @return
	 */
	protected List<List<? extends ID>> separateNewVsExisting(List<? extends ID> objs, List<String> idList)
	{
		List<List<? extends ID>> out = new ArrayList<List<? extends ID>>();
		
		int size = objs.size();
		List<ID> newList = new ArrayList<ID>(size);
		List<ID> existingList = new ArrayList<ID>(size);
		
		out.add(newList);
		out.add(existingList);
		
		if(idList == null) idList = new ArrayList<String>(size);
		
		ID id = null;
		String idStr = null;
		for(int i=0; i<size; i++)
		{
			id = objs.get(i);
			idStr = id.getID();
			if(idStr == null)
			{
				newList.add(id);
			}
			else
			{
				existingList.add(id);
				idList.add(idStr);
			}
		}
		
		return out;
	}
	
    /**
     * Search for paths that link one node to other nodes.
     * 
     * @param startCriterion
     * @param relationshipCriterion
     * @param endCriterion
     * @param maxLength
     * @param maxHits
     * @throws NoGraphException
     * @return
     */
    public List<Path> findPaths(Criterion startCriterion, Criterion relationshipCriterion, Criterion endCriterion,
    		                    int maxLength, int maxHits) throws NoGraphException
    {
    	throw new NoGraphException("Path query not implemented");
    }
    

	protected void loadGraphMeta()
	{
		synchronized(metasync)
		{
			graphMeta = new GraphMeta();
			File f = new File(metaFile);
			if(f.exists())
			{
				String json = FileUtil.getStringFromFile(metaFile);
				graphMeta.fromJSONString(json);
			}	
		}		
	}
	
	/**
	 * Most bulk inserts are going to be for identical property types, so we don't need to see each one.
	 * 
	 * @param nodes
	 */
	protected void sampleNodeMeta(List<Node> nodes) 
	{
		if(nodes == null || nodes.size() == 0) return;
		
		int size = nodes.size();
		if(size < 10)
		{
			for(int i=0; i<size; i++)
			{
				graphMeta.updateNodeMeta(nodes.get(i));
			}
		}
		else
		{
			graphMeta.updateNodeMeta(nodes.get(0));
			graphMeta.updateNodeMeta(nodes.get(size-1));
			graphMeta.updateNodeMeta(nodes.get(size/2));
			graphMeta.updateNodeMeta(nodes.get(size/4));
			graphMeta.updateNodeMeta(nodes.get(3*size/4));
		}
		writeGraphMeta();
	}

	protected void sampleRelMeta(List<Relationship> rels) 
	{
		if(rels == null || rels.size() == 0) return;
		// sample first middle and last
		int size = rels.size();
		if(size < 5)
		{
			for(int i=0; i<size; i++)
			{
				graphMeta.updateRelationshipMeta(rels.get(i));
			}
		}
		else
		{
			graphMeta.updateRelationshipMeta(rels.get(0));
			graphMeta.updateRelationshipMeta(rels.get(size-1));
			graphMeta.updateRelationshipMeta(rels.get(size/2));
		}
		writeGraphMeta();
	}

	
	protected void writeGraphMeta()
	{
		synchronized(metasync)
		{
			FileWriter fw = null;
			try
			{
				fw = new FileWriter(metaFile);
				fw.write(graphMeta.toJSONString());
				fw.flush();
				fw.close();
				fw = null;
			}
			catch(Exception ex)
			{
				logger.log(Level.SEVERE, "Error commiting meta to disk", ex);
			}
			finally
			{
				if(fw != null)try{fw.close();}catch(Exception ex){};
			}
			
		} // end sync
	}
	
	@Override
	public List<String> getPropertyNamesForNodeType(String type) throws NoGraphException 
	{
		if(graphMeta == null) return null;
		
		return graphMeta.getPropertiesForNodeType(type);
	}

	@Override
	public List<String> getPropertyNamesForRelationshipType(String type) throws NoGraphException 
	{
		if(graphMeta == null) return null;
		
		return graphMeta.getPropertiesForRelationshipType(type);
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByNodeType() throws NoGraphException 
	{
		Map<String,List<String>> out = new HashMap<String,List<String>>();
		
		List<String> types = this.getNodeTypes();
		if(types != null && graphMeta != null)
		{
			List<String> props = null;
			String type = null;
			int size = types.size();
			for(int i=0; i<size; i++)
			{
				type = types.get(i);
				props = graphMeta.getPropertiesForNodeType(type);
				out.put(type, props);
			}
		}
		return out;
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByRelationshipType() throws NoGraphException 	
	{
		Map<String,List<String>> out = new HashMap<String,List<String>>();
		
		List<String> types = this.getRelationshipTypes();
		if(types != null && graphMeta != null)
		{
			List<String> props = null;
			String type = null;
			int size = types.size();
			for(int i=0; i<size; i++)
			{
				type = types.get(i);
				props = graphMeta.getPropertiesForRelationshipType(type);
				out.put(type, props);
			}
		}
		return out;
	}
	
	
	/**
	 * Replace this managers graph meta data with the provided one.
	 * 
	 * @param gm
	 */
	public void replaceGraphMeta(GraphMeta gm)
	{
		try
		{
			synchronized(metasync)
			{
				graphMeta = gm;
				writeGraphMeta();
				loadGraphMeta();
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE,"Error updating meta",ex);
		}
	}
}
