package com.sjsu.cmpe273.resources;

public class RedisData implements IHashable {

	private String key;
	
	private Object value;

	public RedisData(String key, Object value) {
		super();
		this.key = key;
		
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString().replace("com.sjsu.cmpe273.redis.hash.RedisData@", "");
	}
	
}
