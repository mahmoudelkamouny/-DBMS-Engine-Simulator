package SLOKS;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Random;
import java.util.Vector;

public class DB_Test {

	public static void main(String[] args) throws DBAppException, IOException {
		DBApp dbApp = new DBApp();
		
		    int x[] = {0,10}; 
	        int y[] = {0,10};
	        int x2[] = {3,10}; 
	        int y2[] = {3,10};
	        int numberofpoints = 2;
	        polygon a=new polygon(x,y,2);
		
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("gpa", "java.lang.double");                                    
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("pol", "java.awt.Polygon");
		
		
//		                
		 dbApp.init();                 // run it one time only when you start the program to initialize important vectors to run the program 
		 dbApp.createTable("Student","name", htblColNameType);
		
		 dbApp.createBTreeIndex("Student","name");
      //dbApp.createRTreeIndex("Student","pol");
		 
		Random rand = new Random();
		 for(int i=0;i<20;i++){
			 int rand_int1 = rand.nextInt(1000);
			 int rand_int2 = rand.nextInt(1000);
			 int rand_int3 = rand.nextInt(1000);
			 int rand_int4 = rand.nextInt(1000);
			 int rand_int5 = rand.nextInt(1000);
			 int rand_int6 = rand.nextInt(1000);
			 int rand_int7 = rand.nextInt(1000);
			 int rand_int8 = rand.nextInt(1000);
			 int rand_int9 = rand.nextInt(1000);
			 int rand_int10 = rand.nextInt(1000);
			 int rand_int11 = rand.nextInt(1000);
			 int rand_int12 = rand.nextInt(1000);
			 int rand_int13 = rand.nextInt(1000);
			 int rand_int14 = rand.nextInt(1000);
			 int rand_int15 = rand.nextInt(1000);
			 
			    double rand_double = rand.nextDouble();
			 Hashtable htblColNameValue0 = new Hashtable();
				htblColNameValue0.put("id", new Integer(rand_int1));
				htblColNameValue0.put("name", new String("ahmed"));
				htblColNameValue0.put("gpa", new Double(0.95));
				htblColNameValue0.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue0);

				Hashtable htblColNameValue1 = new Hashtable();
				htblColNameValue1.put("id", new Integer(rand_int2));
				htblColNameValue1.put("name", new String("sayad"));
				htblColNameValue1.put("gpa", new Double(0.95));
				htblColNameValue1.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue1);

				Hashtable htblColNameValue2 = new Hashtable();
				htblColNameValue2.put("id", new Integer(rand_int3));
				htblColNameValue2.put("name", new String("medhat"));
				htblColNameValue2.put("gpa", new Double(1.25));
				htblColNameValue2.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue2);

				Hashtable htblColNameValue3 = new Hashtable();
				htblColNameValue3.put("id", new Integer(rand_int4));
				htblColNameValue3.put("name", new String("abbas"));
				htblColNameValue3.put("gpa", new Double(1.5));
				htblColNameValue3.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue3);

				Hashtable htblColNameValue4 = new Hashtable();
				htblColNameValue4.put("id", new Integer(rand_int5));
				htblColNameValue4.put("name", new String("farouk"));
				htblColNameValue4.put("gpa", new Double(0.88));
				htblColNameValue4.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue4);
				
				Hashtable htblColNameValue5 = new Hashtable();
				htblColNameValue5.put("id", new Integer(rand_int6));
				htblColNameValue5.put("name", new String("zaky"));
				htblColNameValue5.put("gpa", new Double(1.00));
				htblColNameValue5.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue5);

				Hashtable htblColNameValue6 = new Hashtable();
				htblColNameValue6.put("id", new Integer(rand_int7));
				htblColNameValue6.put("name", new String("ibrahim"));
				htblColNameValue6.put("gpa", new Double(2.00));
				htblColNameValue6.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue6);

				Hashtable htblColNameValue7 = new Hashtable();
				htblColNameValue7.put("id", new Integer(rand_int8));
				htblColNameValue7.put("name", new String("labib"));
				htblColNameValue7.put("gpa", new Double(2.55));
				htblColNameValue7.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue7);

				Hashtable htblColNameValue8 = new Hashtable();
				htblColNameValue8.put("id", new Integer(rand_int9));
				htblColNameValue8.put("name", new String("omer"));
				htblColNameValue8.put("gpa", new Double(0.77));
				htblColNameValue8.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue8);

				Hashtable htblColNameValue9 = new Hashtable();
				htblColNameValue9.put("id", new Integer(rand_int10));
				htblColNameValue9.put("name", new String("rehab"));
				htblColNameValue9.put("gpa", new Double(3.22));
				htblColNameValue9.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue9);

				Hashtable htblColNameValue10 = new Hashtable();
				htblColNameValue10.put("gpa", new Double(1.2));
				htblColNameValue10.put("name", new String("mahmoud"));
				htblColNameValue10.put("id", new Integer(rand_int11));
				htblColNameValue10.put("pol",new polygon(x,y,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue10);
				
				Hashtable htblColNameValue11 = new Hashtable();
				htblColNameValue11.put("id", new Integer(rand_int12));
				htblColNameValue11.put("name", new String("karim"));
				htblColNameValue11.put("gpa", new Double(2.01));
				htblColNameValue11.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue11);

				Hashtable htblColNameValue12 = new Hashtable();
				htblColNameValue12.put("id", new Integer(rand_int13));
		        htblColNameValue12.put("name", new String("morad"));
				htblColNameValue12.put("gpa", new Double(1.99));
				htblColNameValue12.put("pol",new polygon(x2,y2,2));
				dbApp.insertIntoTable( "Student" , htblColNameValue12);
				
		        Hashtable htblColNameValue13 = new Hashtable();
		        htblColNameValue13.put("name", new String("belal" ) );
		        htblColNameValue13.put("id", new Integer(rand_int14));
		        htblColNameValue13.put("gpa", new Double( rand_double ) );
		        htblColNameValue13.put("pol",new polygon(x,y,2));
		        dbApp.insertIntoTable( "Student" , htblColNameValue13);
		
		        Hashtable htblColNameValue14 = new Hashtable();
		        htblColNameValue14.put("name", new String("samy" ) );
		        htblColNameValue14.put("id", new Integer(rand_int15));
		        htblColNameValue14.put("gpa", new Double(2) );
		        htblColNameValue14.put("pol",new polygon(x,y,2));
		        dbApp.insertIntoTable( "Student" , htblColNameValue14);	 
		 }
			dbApp.update_trees_after_insert("Student"); // after finishing insertion call this method to updates trees other than clustering one if exists
////------------------------------------------------------------------------------------------------------------------------------------------------------		
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
						               	//////////////////For testing delete and update//////////////
		 
//			 Hashtable htblColNameValue15 = new Hashtable();
//	          // htblColNameValue15.put("name", new String("rehab") );
//	           htblColNameValue15.put("id", new Integer(4));
//	         //htblColNameValue15.put("gpa", new Double(3.00) );
//	           //htblColNameValue15.put("pol",new polygon(x,y,2));
//		 
//	         //dbApp.deleteFromTable("Student",htblColNameValue15);
//	          dbApp.updateTable("Student", "samy", htblColNameValue15);
//------------------------------------------------------------------------------------------------------------------------------------------------------		
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
	       	                              ////////////////////////for creating index after insertion///////////////////                                      
	           
//		dbApp.createRTreeIndex("Student","pol");
//		RTree<Double> b = deserialize_RTree("Student", "pol");
//		System.out.println(b.toString());
//		 Ref r=b.search(49.0);
//		 System.out.println(r.getPage()+" "+r.getIndexInPage());
//		Vector<Ref> v=dbApp.deserialize_classTree("data/Overflow of "+"49.0"+".ser");
//	 for(int i=0;i<v.size();i++){
//		 System.out.println("("+v.get(i).getPage()+","+v.get(i).getIndexInPage()+")");
//	 }
//		
//		dbApp.createBTreeIndex("Student","id");
//		BPTree<Integer> b = deserialize_BTree("Student", "id");
//		System.out.println(b.toString());
//		 Ref r=b.search(10);
//		 System.out.println(r.getPage()+" "+r.getIndexInPage());
////
//			Vector<Ref> v=dbApp.deserialize_classTree("data/Overflow of "+"10"+".ser");
//		 for(int i=0;i<v.size();i++){
//			 System.out.println("("+v.get(i).getPage()+","+v.get(i).getIndexInPage()+")");
//		 }
//		 
		
		 
		 //dbApp.createBTreeIndex("Student","name");
//		 BPTree<String> b1 = dbApp.deserialize_BTree("Student", "name");
//			System.out.println(b1.toString());
//			 Ref r1=b1.search("morad");
//			 System.out.println(r1.getPage()+" "+r1.getIndexInPage());
////
//				Vector<Ref> v1=dbApp.deserialize_classTree("data/Overflow of "+"morad"+".ser");
//			 for(int i=0;i<v1.size();i++){
//				 System.out.println("("+v1.get(i).getPage()+","+v1.get(i).getIndexInPage()+")");
//			 }
//			 System.out.println(v1.size());
////		 
			// dbApp.createBTreeIndex("Student","gpa");
//			 BPTree<Double> b2 = deserialize_BTree("Student", "gpa");
//				System.out.println(b2.toString());
//				 Ref r2=b2.search(2.00);
//				 System.out.println(r2.getPage()+" "+r2.getIndexInPage());
//
//					Vector<Ref> v2=dbApp.deserialize_classTree("data/Overflow of "+"2.0"+".ser");
//				 for(int i=0;i<v2.size();i++){
//					 System.out.println("("+v2.get(i).getPage()+","+v2.get(i).getIndexInPage()+")");
//				 }
		
//------------------------------------------------------------------------------------------------------------------------------------------------------		
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
//------------------------------------------------------------------------------------------------------------------------------------------------	 
	         						 	//////////////////For testing select//////////////
	           
//		SQLTerm s1 = new SQLTerm("Student", "name", ">","ahmed");
//		SQLTerm s2 = new SQLTerm("Student", "id", "<",100);
//		SQLTerm s3 = new SQLTerm("Student", "gpa", ">",3.00);
//		SQLTerm[] arrSQLTerms = new SQLTerm[3];
//		arrSQLTerms[0] = s1;
//		arrSQLTerms[1] = s2;
//		arrSQLTerms[2] = s3;
//		String[] strarrOperators = new String[2];
//		strarrOperators[0] = "OR";
//		strarrOperators[1] = "XOR";
//		
//    java.util.Iterator<Tuple> res1=(java.util.Iterator<Tuple>) dbApp.select(arrSQLTerms, strarrOperators);
//    	while(res1.hasNext()){
//    		System.out.println(res1.next().toString());
//    	}
    	       
//------------------------------------------------------------------------------------------------------------------------------------------------------		
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
//------------------------------------------------------------------------------------------------------------------------------------------------------		 
	         			 			 		 /////////////////For Printing ///////////////////////
	         			
	         			Table t =dbApp.deserializet("data/" + "Student"+ ".ser");
	         			for (int i = 0; i < t.files.size(); i++) {
	         				System.out
	         						.println("New------------------------"+i+"--------------------Page");
	         				Page p = dbApp.deserialize(t.files.get(i));
	         				for (int j = 0; j < p.Tuple.size(); j++) {
                                System.out.println("");
	         					System.out.println(j+" "+p.Tuple.get(j).toString());
//	         							System.out.println(j+" "
//	         									+ "id = "
//	         									+ p.Tuple.get(j).Hash.get("id")
//	         									+ " name = "
//	         									+ p.Tuple.get(j).Hash
//	         											.get("name")
//	         									+ " gpa = "
//	         									+ p.Tuple.get(j).Hash
//	         											.get("gpa"));
	         						}
	         				}
	           
	           
	           
	           
	           
	}
	
	
	
	
	
	
	
}
