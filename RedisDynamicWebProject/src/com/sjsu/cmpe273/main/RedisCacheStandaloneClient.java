package com.sjsu.cmpe273.main;

import com.sjsu.cmpe273.server.RedisServer;

public class RedisCacheStandaloneClient {
	public static void main(String[] args) {
		RedisServer redisServer = new RedisServer();
		System.out.println("Welcome to Redis consistent hash cluster");
		System.out.println("Select an option \n1. Add a node. \n2. Insert data. \n3. View data from node. \n4. View data in all nodes");
		
	}

}
