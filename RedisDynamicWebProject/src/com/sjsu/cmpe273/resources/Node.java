package com.sjsu.cmpe273.resources;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;

public class Node extends SuperNode{
	private List<ReplicaNode> replicas;
	
	public Node(String ipAddress, Integer port) {
		
		super(ipAddress, port);
	}

	@Override
	public String getIpAddress() {
		
		return this.ipAddress;
	}

	@Override
	public void setIpAddress(String ipAddress) {
		
		this.ipAddress = ipAddress;
	}

	@Override
	public Integer getPort() {
		
		return this.port;
	}

	@Override
	public void setPort(Integer port) {
		
		this.port = port;
	}
	
	public void addReplica(ReplicaNode replica){
		if(replicas == null){
			replicas = new ArrayList<ReplicaNode>();
		}
		replicas.add(replica);
	}
	
	public void addReplica(String ipAddress, Integer port){
		if(replicas == null){
			replicas = new ArrayList<ReplicaNode>();
		}
		replicas.add(new ReplicaNode(ipAddress, port));
	}

	public List<ReplicaNode> getReplicas() {
		return replicas;
	}

	public void setReplicas(List<ReplicaNode> replicas) {
		this.replicas = replicas;
	}

	@Override
	public Jedis getJedis() {
		return this.jedis;
	}

	@Override
	public void setJedis(Jedis jedis) {
		this.jedis = jedis;
		
	}
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof Node){
			Node node = (Node)obj;
			return this.ipAddress.equals(node.ipAddress) && this.port == node.port;
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString().replace("com.sjsu.cmpe273.redis.hash.Node@", "");
	}
	

}
