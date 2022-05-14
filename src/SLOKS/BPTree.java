package SLOKS;



import java.io.Serializable;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;



public class BPTree<T extends Comparable<T>> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private BPTreeNode<T> root;
	
	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public BPTree(int order) 
	{
		this.order = order;
		root = new BPTreeLeafNode<T>(this.order);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param Key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	public void insert(T Key, Ref recordReference,String col_name,String table_name)
	{
		PushUp<T> pushUp = root.insert(Key, recordReference, null, -1, col_name,table_name);
		if(pushUp != null)
		{
			BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
			newRoot.insertLeftAt(0, pushUp.key, root);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root = newRoot;
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 */
	public Ref search(T key,Page page,int page_index)
	{
		return root.search(key,page,page_index);
	}
	
	public Ref search(T key) {
		return root.search(key);
	}
	
	public Ref get(T key,String table_name,String col_name,Hashtable<String, Object> htblColNameValue) {
		return root.get(key,table_name,col_name,htblColNameValue);
	}
	
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 */
	public boolean delete(T key)
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<BPTreeNode<T>>();
			while(!cur.isEmpty())
			{
				BPTreeNode<T> curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof BPTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						System.out.print(parent.getChild(i).index+",");
						next.add(parent.getChild(i));
					}
					System.out.print("} ");
				}
				
			}
			System.out.println();
			cur = next;
		}	
		//	</For Testing>
		return s;
	}
	
	public void update(T key,Ref old_reference,Ref new_reference){
		 root.update(key, old_reference,new_reference,-1,null,false);
	}
	
	public void update_for_insert(T key,Ref old_reference,Ref new_reference){
		 root.update_for_insert(key, old_reference,new_reference,-1,null);
	}
	
	public boolean delete_using_Ref(T key, Ref r) {
		boolean done = root.delete_using_Ref(key, r, null, -1);
		while (root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
}
