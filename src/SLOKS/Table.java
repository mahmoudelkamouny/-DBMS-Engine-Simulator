package SLOKS;

import java.sql.Timestamp;
import java.util.*;
import java.io.*;
import java.util.Date;
public class Table implements java.io.Serializable  {

Timestamp Touchdate;
Vector <String> files;
String ClusteringKey;
String name;
 
public Table(String name,String clusteringKey) {
	Touchdate = new Timestamp(System.currentTimeMillis());
	files = new Vector<String>();
	ClusteringKey = clusteringKey;
	this.name=name;

}
	
public static void main (String [] args){
 }
	
	
}
	 

