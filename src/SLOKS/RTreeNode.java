package SLOKS;



import java.io.Serializable;
import java.util.Hashtable;

public abstract class RTreeNode<T extends Comparable<T>> implements Serializable{
	
	/**
	 * Abstract class that collects the common functionalities of the inner and leaf nodes
	 */
	private static final long serialVersionUID = 1L;
	protected Comparable<T>[] keys;
	protected int numberOfKeys;
	protected int order;
	protected int index;		//for printing the tree
	private boolean isRoot;
	private static int nextIdx = 0;

	public RTreeNode(int order) 
	{
		index = nextIdx++;
		numberOfKeys = 0;
		this.order = order;
	}
	
	/**
	 * @return a boolean indicating whether this node is the root of the B+ tree
	 */
	public boolean isRoot()
	{
		return isRoot;
	}
	
	/**
	 * set this node to be a root or unset it if it is a root
	 * @param isRoot the setting of the node
	 */
	public void setRoot(boolean isRoot)
	{
		this.isRoot = isRoot;
	}
	
	/**
	 * find the key at the specified index
	 * @param index the index at which the key is located
	 * @return the key which is located at the specified index
	 */
	public Comparable<T> getKey(int index) 
	{
		return keys[index];
	}

	/**
	 * sets the value of the key at the specified index
	 * @param index the index of the key to be set
	 * @param key the new value for the key
	 */
	public void setKey(int index, Comparable<T> key) 
	{
		keys[index] = key;
	}
	
	/**
	 * @return a boolean whether this node is full or not
	 */
	public boolean isFull() 
	{
		return numberOfKeys == order;
	}
	
	/**
	 * @return the last key in this node
	 */
	public Comparable<T> getLastKey()
	{
		return keys[numberOfKeys-1];
	}
	
	/**
	 * @return the first key in this node
	 */
	public Comparable<T> getFirstKey()
	{
		return keys[0];
	}
	
	/**
	 * @return the minimum number of keys this node can hold
	 */
	public abstract int minKeys();

	/**
	 * insert a key with the associated record reference in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference a pointer to the record on the hard disk
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node
	 * @return a key and a new node in case of a node splitting and null otherwise
	 */
	public abstract RPushUp<T> insert(T key, Ref recordReference, RTreeInnerNode<T> parent, int ptr,String col_name,String table_name);
	
	public abstract Ref search(T key,Page Page,int page_index);
	public abstract Ref search(T key);
	public abstract Ref get(T key,String table_name,String col,Hashtable<String, Object> htblColNameValue);
	public abstract int findIndex(T key,Page page,int page_index);

	/**
	 * delete a key from the B+ tree recursively
	 * @param key the key to be deleted from the B+ tree
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if this node was successfully deleted and false otherwise
	 */
	public abstract boolean delete(T key, RTreeInnerNode<T> parent, int ptr);
	
	/**
	 * A string represetation for the node
	 */
	public String toString()
	{		
		String s = "(" + index + ")";

		s += "[";
		for (int i = 0; i < order; i++)
		{
			String key = " ";
			if(i < numberOfKeys)
				key = keys[i].toString();
			
			s+= key;
			if(i < order - 1)
				s += "|";
		}
		s += "]";
		return s;
	}
	public abstract boolean update(T key,Ref old_reference,Ref new_reference,int ptr,RTreeInnerNode<T> parent,boolean flag);
	public abstract boolean update_for_insert(T key, Ref old_refrence,Ref new_refrence,int ptr, RTreeInnerNode<T> parent);
	public abstract boolean delete_using_Ref(T key, Ref r, RTreeInnerNode<T> parent, int ptr);
}
