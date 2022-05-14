package SLOKS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements java.io.Serializable {
	
	 Vector <Tuple> Tuple;	
	 int size;
	public Page() {
		
	
		 FileReader fileReader = null;
		 try {
			 try {
				fileReader = new FileReader("config/DBApp.properties");
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			 
			 Properties p= new Properties();
			 try {
				p.load(fileReader);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 
			 String MaximumRowsCountinPage = p.getProperty("MaximumRowsCountinPage");
			 int i=Integer.parseInt(MaximumRowsCountinPage);
			 
			 
			 Tuple=new Vector<Tuple>(i);
			 size= i;
		 }
		 finally{
			 if(fileReader != null)
			 {
				 try {
					fileReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
		 }
		 
		 
		 
		
	 }
	 
	

	 
	 
}