package SLOKS;

import java.io.Serializable;
import java.util.*;
public class Tuple implements Comparable<Tuple>, Serializable  {
	String key;
	Hashtable<String, Object> Hash;

	public Tuple(String key, Hashtable<String, Object> hash) {
		this.key = key;
		Hash = hash;
	}

	@Override
	public int compareTo(Tuple t) {
		String cn = t.Hash.get(key).getClass() + "";
		if (cn.contains("java.lang.Integer")) {
		
			if ((int) (this.Hash.get(key)) == (int) (t.Hash.get(t.getKey()))) {
				return 0;
			} else if ((int) (this.Hash.get(key)) > (int) (t.Hash.get(t
					.getKey()))) {
				return 1;
			} else {
				return -1;
			}
		}
		 else if(cn.contains("java.lang.String")){
			 String first=(String)(this.Hash.get(key));
			 String second=((String)(t.Hash.get(t.getKey())));
			 if (first.compareTo(second)==0) {
					return 0;
				} else if (first.compareTo(second)>0){
					return 1;
				} else if (first.compareTo(second)<0) {
					return -1;
				}
		 }
		 else if(cn.contains("java.lang.Double")) {
	            if ((double) (this.Hash.get(key)) == (double) (t.Hash.get(t.getKey()))) {
	                return 0;
	            } else if ((double) (this.Hash.get(key)) > (double) (t.Hash.get(t.getKey()))) {
	                return 1;
	            } else {
	                return -1;
	            }

		 }
		 else if(cn.contains(" java.util.Date")){
			 Date date1=(Date)(this.Hash.get(key));
			 Date date2=(Date)(t.Hash.get(t.getKey()));
			 if (date1.compareTo(date2) > 0) {
		            return 1;
		        } else if (date1.compareTo(date2) < 0) {
		          return -1;
		        } else if (date1.compareTo(date2) == 0) {
		            return 0;
		 }
			 
		 }
		
		 else if(cn.contains("class Sloks.polygon")){
			 polygon p1=(polygon)(this.Hash.get(key));
			 polygon p2=(polygon)(t.Hash.get(t.getKey()));
			 if(p1.compareTo(p2)==0){
				 return 0;
				 }
				 	 else if(p1.compareTo(p2)>0){
					 return 1;
					 }
					 else if(p1.compareTo(p2)<0){
						 return -1;		 
		 }
			
		 }
		return 6;

	}

	public Object getKey() {
		return this.key;
	}
	
	public String toString(){
		return Hash.toString();
	}

}