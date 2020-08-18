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

import java.util.List;
import java.util.Map;

import org.nograph.GraphQuery;
import org.nograph.NoGraphException;
import org.nograph.Node;
import org.nograph.Relationship;

public class OrientDBGraphManager extends BaseGraphManager {

	@Override
	public void saveNode(Node n) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public Node getNode(String id) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void ingestNodes(List<Node> nodes) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public void saveNodes(List<Node> nodes) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteNodesByID(List<String> ids) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public void saveRelationship(Relationship r) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public Relationship getRelationship(String id, boolean fetchNodes) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void ingestRelationships(List<Relationship> rels) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public void saveRelationships(List<Relationship> rels) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRelationshipsByID(List<String> ids) throws NoGraphException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Node> findNodes(String type, String key, Object val, int maxResults) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relationship> findRelationships(String type, String key, Object val, boolean fetchNodes, int maxResults)
			throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long countNodes(String type) throws NoGraphException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long countRelationships(String type) throws NoGraphException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<String> getNodeTypes() throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getRelationshipTypes() throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Long> getNodeCountsByType() throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Long> getRelationshipCountsByType() throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getPropertyNamesForNodeType(String type) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getPropertyNamesForRelationshipType(String type) throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByNodeType() throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<String>> getPropertyNamesByRelationshipType() throws NoGraphException {
		// TODO Auto-generated method stub
		return null;
	}

}
