package com.sjsu.cmpe273.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sjsu.cmpe273.hash.Hasher;
import com.sjsu.cmpe273.resources.Node;
import com.sjsu.cmpe273.resources.RedisData;

import redis.clients.jedis.Jedis;

public class RedisServer {
	private SortedMap<Long, Node> nodeMap;
	private List<Long> nodeHashSet;

	public RedisServer() {
		nodeMap = new TreeMap<Long, Node>();
		nodeHashSet = new ArrayList<Long>();
	}

	public List<Long> getDataSet() {
		return nodeHashSet;
	}

	public void setDataSet(List<Long> dataSet) {
		this.nodeHashSet = dataSet;
	}

	public SortedMap<Long, Node> getNodeMap() {
		return nodeMap;
	}

	public void setNodeMap(SortedMap<Long, Node> nodeMap) {
		this.nodeMap = nodeMap;
	}

	public void addNodeToCluster(String ipAddress, int port) throws Exception{
		Node node = new Node(ipAddress, port);
		Node affectedNode = null;
		Long lastNodeHash = null;
		//nodeMap.put(Hasher.getHash(node), node);
		if(checkIfNodeIsUp(node)){
			nodeHashSet.add(Hasher.getHash(node));
			Collections.sort(nodeHashSet);

			//If it is the first node, add it immediately and exit the method
			if(nodeMap.size() == 0){
				nodeMap.put(Hasher.getHash(node), node);
				return;
			}
			//Size of cluster is greater than 1
			//TODO Migrate the adjacent nodes data
			//Find the index at which the node is placed
			int nodeIndex = nodeHashSet.indexOf(Hasher.getHash(node));
			nodeMap.put(Hasher.getHash(node), node);
			//Get the node immediately after it
			if(nodeIndex == nodeHashSet.size()-1){
				//This is the last node in the map. The affected node is the one at 0th position.
				affectedNode = nodeMap.get(nodeHashSet.get(0));
				//Checking for all data pairs whose hash values are greater than last node's hash and are assigned to node 0
				lastNodeHash = nodeHashSet.get(nodeHashSet.size()-1);
			}else{
				affectedNode = nodeMap.get(nodeHashSet.get(nodeIndex+1));
			}

			Jedis oldJedis = affectedNode.getJedis();
			Long newNodeHash = Hasher.getHash(node);
			//Get all the keys from this(old) node
			oldJedis.connect();
			Set<String> oldKeySet = oldJedis.keys("*");
			if (!oldKeySet.isEmpty()){
				RedisData currentMovingData = null;
				for (String currentKey : oldKeySet) {
					if((Hasher.getHash(currentKey) < newNodeHash) || 
							//Checking if data is set in node 0 and has a hash greater than the last node in the node map
							(lastNodeHash!= null && Hasher.getHash(currentKey) > lastNodeHash)){
						//This key is to be moved
						currentMovingData = new RedisData(currentKey, oldJedis.get(currentKey));
						//Deleting from the old node
						oldJedis.del(currentKey);
						System.out.println("Moving key :"+currentKey+" from "+affectedNode.getIpAddress()+":"+affectedNode.getPort()+
								" to :"+node.getIpAddress()+":"+node.getPort());
						//Inserting data into the cluster
						insertData(currentMovingData);
					}
				}
			}
			oldJedis.disconnect();
			oldJedis.close();
		}else{
			throw new Exception("The node at IP : "+ipAddress+":"+port+" is not up.");
		}	
	}

	public void removeNode(Node node){
		//TODO Migrate the data from this node into the adjacent nodes and then Remove the node from the map


		nodeMap.remove(Hasher.getHash(node));
	}

	public List<RedisData> getAllData(){
		//TODO Return data from all the Redis nodes
		List<RedisData> outputList = null;

		for(Long hash : nodeHashSet){
			if(outputList == null){
				outputList = new ArrayList<RedisData>();
			}
			outputList.addAll(getDataFromNode(nodeMap.get(hash)));
		}

		if (outputList != null) {
			return outputList;
		} else {
			throw new NullPointerException(" There are no nodes available in the cluster. ");
		}
	}

	public List<RedisData> getDataFromNode(Node node){
		//TODO Return data from a single Redis node
		List<RedisData> outputList = null;
		Jedis currJedis = node.getJedis();
		currJedis.connect();
		if (currJedis.isConnected()){
			outputList = new ArrayList<RedisData>();
			Set<String> keys = currJedis.keys("*");
			for (String key: keys) {
				outputList.add(new RedisData(key, currJedis.get(key)));
			}
			currJedis.disconnect();
			currJedis.close();
		}else{
			throw new NullPointerException("Unable to connect to instance :"+node.getIpAddress()+" -- Port : "+node.getPort());
		}		
		return outputList;
	}

	public boolean checkIfNodeIsUp(Node node){
		boolean isConnected = false;
		if(node.getJedis().isConnected()){
			return true;
		}else{
			try {
				node.getJedis().connect();
				isConnected = node.getJedis().isConnected();
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				node.getJedis().disconnect();
				node.getJedis().close();
			}		
		}
		return isConnected;		
	}

	public Node findNodeForData(RedisData data){
		//TODO Find the node to which this key is to be assigned to
		Long keyHash = Hasher.getHash(data);
		for (Long hash : nodeHashSet) {
			if(keyHash < hash){
				//This is the node in which data will be inserted
				Node assignedNode = (Node) nodeMap.get(hash);
				return assignedNode;
			}
		}		
		return nodeMap.get(nodeHashSet.get(0));
	}

	public Node insertData(RedisData data) throws NullPointerException{
		//TODO insert data into the assigned node and return the node to which it is assigned to		
		Node assignedNode = null;
		if(nodeMap!= null && !nodeMap.isEmpty()){
			assignedNode = findNodeForData(data);
			Jedis currJedis = assignedNode.getJedis();
			//Connect to the node and perform insertion
			currJedis.connect();
			System.out.println("Key : "+data.getKey()+" sent to node : "+assignedNode.getIpAddress()+":"+assignedNode.getPort());
			currJedis.set(data.getKey(), data.getValue().toString());
			currJedis.disconnect();
			currJedis.close();
		}		

		if (assignedNode != null) {
			return assignedNode;
		} else {
			throw new NullPointerException("Node is not available for data : Key : "+data.getKey()+" -- "+ data.getValue());
		}
	}

	public void removeFromCluster(String key){

	}



}
