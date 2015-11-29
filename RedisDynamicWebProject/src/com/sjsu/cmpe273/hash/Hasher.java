package com.sjsu.cmpe273.hash;

import com.sjsu.cmpe273.resources.RedisData;
import com.sjsu.cmpe273.resources.SuperNode;

import redis.clients.util.MurmurHash;

public class Hasher {
	
	private static MurmurHash murmurHash = new MurmurHash();

	public static Long getHash(Object hashable){
		
		if( hashable instanceof SuperNode){
			SuperNode superNode = (SuperNode)hashable;			
			String hashString = "http://"+superNode.getIpAddress()+":"+superNode.getPort()+"/";			
			return murmurHash.hash(hashString);		
				
		}else if( hashable instanceof RedisData){			
			return murmurHash.hash(((RedisData)hashable).getKey());
		}else{
			return murmurHash.hash(hashable.toString());
		} 
		
	
	}
	
	public static Long getReplicaHash(SuperNode superNode, int replicaID){
		String hashString = "http://"+superNode.getIpAddress()+":"+superNode.getPort()+"/ -- "+replicaID;
		return murmurHash.hash(hashString);
	}
	
	public static Integer[] getNumArray(Integer input){
		int len = Integer.toString(input).length();
		Integer[] iarray = new Integer[len];
		for (int index = 0; index < len; index++) {
		    iarray[index] = input % 10;
		    input /= 10;
		}
		return iarray;
	}
}
