package com.sjsu.cmpe273.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.sjsu.cmpe273.hash.Hasher;
import com.sjsu.cmpe273.resources.Node;
import com.sjsu.cmpe273.resources.RedisData;
import com.sjsu.cmpe273.server.RedisServer;

public class RedisCacheStandaloneClient {
	public static void main(String[] args) {

		Scanner scanner = null;
		try {

			RedisServer redisServer = new RedisServer();
			scanner = new Scanner(System.in);
			System.out.println("Welcome to Redis consistent hash cluster");
			while (true) {

				System.out.println("Select an option \n1. Add a node. \n2. Insert data. \n3. View data from node. \n4. View data in all nodes");
				int choice = scanner.nextInt();
				Node currentNode = null; 
				switch (choice) {
				case 1:
					//Add a node
					System.out.println("----------------------------------------");
					System.out.println("Enter the IP address");
					String newIP = scanner.next();
					System.out.println("Enter the port no");
					int newPort = scanner.nextInt();
					System.out.println("Enter the no of replicas");
					int replicas = scanner.nextInt();
					try {
						redisServer.addNodeToCluster(newIP, newPort, replicas);
						System.out.print("Added node successfully. New cluster is :");
						for(Long hash : redisServer.getDataSet()){
							currentNode = redisServer.getNodeMap().get(hash);
							System.out.println(currentNode.getIpAddress()+":"+currentNode.getPort()+"\t -- "+hash);
						}
						System.out.println("----------------------------------------");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					break;

				case 2:
					//Insert data
					System.out.println("----------------------------------------");
					System.out.println("Enter the key :");
					String key = scanner.next();
					System.out.println("Hash : "+Hasher.getHash(key));
					System.out.println("Enter the value : ");
					String value = scanner.next();
					System.out.println("Adding key "+key+" to the cluster ");
					redisServer.insertData(key,value);
					System.out.println("----------------------------------------");

					break;

				case 3:
					//Selective nodes data
					System.out.println("----------------------------------------");
					System.out.println("Select the node whose data you want to view ");
					List<Node> nodeList = new ArrayList<Node>();
					for(int i=0; i<redisServer.getDataSet().size();i++){
						currentNode= redisServer.getNodeMap().get(redisServer.getDataSet().get(i));
						System.out.println(i+". "+currentNode.getIpAddress()+":"+currentNode.getPort());
						nodeList.add(currentNode);
					}
					System.out.println();
					int nodeID = scanner.nextInt();
					int dataCounter =0 ;
					List<RedisData> data = redisServer.getDataFromNode(nodeList.get(nodeID));
					for (RedisData redisData : data) {
						System.out.println((dataCounter++)+redisData.getKey()+" -- "+redisData.getValue());
					}
					System.out.println("----------------------------------------");
					break;

				case 4:
					//View data in all nodes
					Map<Node, List<RedisData>> dataMap = redisServer.getAllData();
					System.out.println("----------------------------------------");
					for(Map.Entry<Node,List<RedisData>> entry : dataMap.entrySet()) {
						currentNode = entry.getKey();
						List<RedisData> currentData = entry.getValue();
						System.out.println("--------------------------");
						System.out.println(currentNode.getIpAddress()+":"+currentNode.getPort());
						System.out.println("--------------------------");
						for (RedisData redisData : currentData) {
							System.out.println(redisData.getKey()+" --- "+redisData.getValue());
						}
					}
					System.out.println("----------------------------------------");

					break;

				default:
					break;
				}

			}

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			scanner.close();
		}

	}

}
