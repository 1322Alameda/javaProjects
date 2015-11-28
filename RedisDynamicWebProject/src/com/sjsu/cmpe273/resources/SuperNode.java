package com.sjsu.cmpe273.resources;

import redis.clients.jedis.Jedis;

public abstract class SuperNode implements IHashable{

	protected String ipAddress;
	
	protected Integer port;
	
	protected Jedis jedis;
	
	public SuperNode(String ipAddress, Integer port) {
		
		super();
		this.ipAddress = ipAddress;
		this.port = port;
		this.jedis = new Jedis(ipAddress, port);
		
	}	

	public abstract Jedis getJedis();

	public abstract void setJedis(Jedis jedis);

	public abstract String getIpAddress();

	public abstract void setIpAddress(String ipAddress);

	public abstract Integer getPort();

	public abstract void setPort(Integer port);
	
	/*@Override
	public String toString() {
		// TODO Auto-generated method stub
		return port+ipAddress;
	}
	*/
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString().replace("com.sjsu.cmpe273.redis.hash.SuperNode@", "");
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof SuperNode){
			SuperNode node = (SuperNode)obj;
			return this.ipAddress.equals(node.ipAddress) && this.port == node.port;
		}
		
		return false;
	}
	
}
