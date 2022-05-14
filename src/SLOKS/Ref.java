package SLOKS;

import java.io.Serializable;

public class Ref implements Serializable,Comparable<Ref>{
	
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the B+ tree 
	 */
	private static final long serialVersionUID = 1L;
	private int pageNo, indexInPage;
	
	public Ref(int pageNo, int indexInPage)
	{
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
	}
	
	/**
	 * @return the page at which the record is saved on the hard disk
	 */
	public int getPage()
	{
		return pageNo;
	}
	
	/**
	 * @return the index at which the record is saved in the page
	 */
	public int getIndexInPage()
	{
		return indexInPage;
	}

	@Override
	public int compareTo(Ref arg0) {
		if(this.pageNo>arg0.pageNo){
			return 1;}
		else if(this.pageNo<arg0.pageNo){
			return -1;}
		else if(this.pageNo==arg0.pageNo){
			if(this.indexInPage>arg0.indexInPage){
				return 1;}
			else if(this.indexInPage<arg0.indexInPage){
				return -1;}
			else
				return 0;
			
		}
		
		return 0;
	}
}
