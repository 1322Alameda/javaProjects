package com.sjsu.cmpe273.resources;

import redis.clients.jedis.Jedis;

public class ReplicaNode extends SuperNode{

	public ReplicaNode(String ipAddress, Integer port) {
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
		if (obj instanceof ReplicaNode){
			ReplicaNode node = (ReplicaNode)obj;
			return this.ipAddress.equals(node.ipAddress) && this.port == node.port;
		}
		
		return false;
	}

	

}
