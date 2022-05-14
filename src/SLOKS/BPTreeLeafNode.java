package SLOKS;








import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;


public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Ref[] records;
	private BPTreeLeafNode<T> next;
	
	@SuppressWarnings("unchecked")
	public BPTreeLeafNode(int n) 
	{
		super(n);
		keys = new Comparable[n];
		records = new Ref[n];

	}
	
	/**
	 * @return the next leaf node
	 */
	public BPTreeLeafNode<T> getNext()
	{
		return this.next;
	}
	
	/**
	 * sets the next leaf node
	 * @param node the next leaf node
	 */
	public void setNext(BPTreeLeafNode<T> node)
	{
		this.next = node;
	}
	
	/**
	 * @param index the index to find its record
	 * @return the reference of the queried index
	 */
	public Ref getRecord(int index) 
	{
		return records[index];
	}
	
	/**
	 * sets the record at the given index with the passed reference
	 * @param index the index to set the value at
	 * @param recordReference the reference to the record
	 */
	public void setRecord(int index, Ref recordReference) 
	{
		records[index] = recordReference;
	}

	/**
	 * @return the reference of the last record
	 */
	public Ref getFirstRecord()
	{
		return records[0];
	}

	/**
	 * @return the reference of the last record
	 */
	public Ref getLastRecord()
	{
		return records[numberOfKeys-1];
	}
	
	/**
	 * finds the minimum number of keys the current node must hold
	 */
	public int minKeys()
	{
		if(this.isRoot())
			return 1;
		return (order + 1) / 2;
	}
	
	/**
	 * insert the specified key associated with a given record refernce in the B+ tree
	 */
	public PushUp<T> insert(T key, Ref recordReference,
			BPTreeInnerNode<T> parent, int ptr,String col_name,String table_name) {
		DBApp db = new DBApp();
		// System.out.println(this.keys[0]);
		for (int i = 0; i < this.keys.length; i++) {
			if (this.keys[i] == null) {

				break;
			}
			try {
				if(!col_name.equals("not cluster")){
				if(db.getcluster(table_name).equals(col_name)){
					if (this.keys[i].compareTo(key) == 0) {
						String path = "data/Overflow of "+key+".ser";
						Vector<String> big_vector = db.deserialize_Overflow_page();
						for (int f = 0; f < big_vector.size(); f++) {
							if (big_vector.get(f).equals(path)) {
								Vector<Ref> overflow_page = db.deserialize_classTree(path);
								overflow_page.add(this.records[i]);
								db.serialize_classtree(overflow_page, path);
								db.serialize_Overflow_page(big_vector);
								return null;
							}
						}
						Vector<Ref> new_page = new Vector<Ref>();
						new_page.add(this.records[i]);
						db.serialize_classtree(new_page, path);
						big_vector.add(path);
						Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
						overflow_numbers.add((Object) key);
						db.serialize_Overflow_numbers(overflow_numbers);
						db.serialize_Overflow_page(big_vector);
						db.serialize_classtree(new_page, path);

						return null;
					}
				}
				
				else{
				if (this.keys[i].compareTo(key) == 0) {
					String path = "data/Overflow of "+key+".ser";
					Vector<String> big_vector = db.deserialize_Overflow_page();
					for (int f = 0; f < big_vector.size(); f++) {
						if (big_vector.get(f).equals(path)) {
							Vector<Ref> overflow_page = db.deserialize_classTree(path);
							overflow_page.add(recordReference);
							db.serialize_classtree(overflow_page, path);
							db.serialize_Overflow_page(big_vector);
							return null;
						}
					}
					Vector<Ref> new_page = new Vector<Ref>();
					new_page.add(recordReference);
					db.serialize_classtree(new_page, path);
					big_vector.add(path);
					Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
					overflow_numbers.add((Object) key);
					db.serialize_Overflow_numbers(overflow_numbers);
					db.serialize_Overflow_page(big_vector);
					db.serialize_classtree(new_page, path);

					return null;
				}}
			}} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (this.isFull()) {
			BPTreeNode<T> newNode = this.split(key, recordReference);
			Comparable<T> newKey = newNode.getFirstKey();
			return new PushUp<T>(newNode, newKey);
		} else {
			int index = 0;
			while (index < numberOfKeys && getKey(index).compareTo(key) <= 0)
				++index;
			this.insertAt(index, key, recordReference);
			return null;
		}
	}
	
	/**
	 * inserts the passed key associated with its record reference in the specified index
	 * @param index the index at which the key will be inserted
	 * @param key the key to be inserted
	 * @param recordReference the pointer to the record associated with the key
	 */
	private void insertAt(int index, Comparable<T> key, Ref recordReference) 
	{
		for (int i = numberOfKeys - 1; i >= index; --i) 
		{
			this.setKey(i + 1, getKey(i));
			this.setRecord(i + 1, getRecord(i));
		}

		this.setKey(index, key);
		this.setRecord(index, recordReference);
		++numberOfKeys;
	}
	
	/**
	 * splits the current node
	 * @param key the new key that caused the split
	 * @param recordReference the reference of the new key
	 * @return the new node that results from the split
	 */
	public BPTreeNode<T> split(T key, Ref recordReference) 
	{
		int keyIndex = this.findIndex(key);
		int midIndex = numberOfKeys / 2;
		if((numberOfKeys & 1) == 1 && keyIndex > midIndex)	//split nodes evenly
			++midIndex;		

		
		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order);
		for (int i = midIndex; i < totalKeys - 1; ++i) 
		{
			newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
			numberOfKeys--;
		}
		
		//insert the new key
		if(keyIndex < totalKeys / 2)
			this.insertAt(keyIndex, key, recordReference);
		else
			newNode.insertAt(keyIndex - midIndex, key, recordReference);
		
		//set next pointers
		newNode.setNext(this.getNext());
		this.setNext(newNode);
		
		return newNode;
	}
	
	/**
	 * finds the index at which the passed key must be located 
	 * @param key the key to be checked for its location
	 * @return the expected index of the key
	 */
	public int findIndex(T key) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			
			int cmp = getKey(i).compareTo(key);
			if (cmp > 0) {
				return i;}
		}
		return numberOfKeys;
	}
	
	public int findIndex(T key,Page page,int page_index) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			
			int cmp = getKey(i).compareTo(key);
			if (cmp > 0) {
				return i;}
		}
		return numberOfKeys;
	}

	/**
	 * returns the record reference with the passed key and null if does not exist
	 */
	@Override
	public Ref search(T key,Page page,int page_index) 
	{
		DBApp db = new DBApp();
		Ref new_ref = new Ref(-1, -1);
		int i = 0;
		for (i = 0; i < numberOfKeys; ++i) {

			if (this.getKey(i).compareTo(key) == 0) {

				return this.getRecord(i);
			}

			else if (this.getKey(i).compareTo(key) > 0) {

				String path = "data/Overflow of " + this.getKey(i) + ".ser";
				Vector<String> big_vector = db.deserialize_Overflow_page();
				for (int f = 0; f < big_vector.size(); f++) {
					if (big_vector.get(f).equals(path)) {
						Vector<Ref> overflow_page = db.deserialize_classTree(path);

						return overflow_page.get(0);
					}
				}

				return this.getRecord(i);
			} else if (this.getKey(i).compareTo(key) < 0) {

				Ref new_ref3 = new Ref(this.getRecord(i).getPage(),
						(this.getRecord(i).getIndexInPage()) + 1);
				new_ref = new_ref3;
			}

		}

		return new_ref;
	}
	
	/**
	 * delete the passed key from the B+ tree
	 */
	public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) 
	{
		DBApp db=new DBApp();
		for(int i = 0; i < numberOfKeys; ++i)
			if(keys[i].compareTo(key) == 0)
			{
				this.deleteAt(i);
				Vector<String> big_vector = db.deserialize_Overflow_page();
				for(int j=0;j<big_vector.size();j++){
					if(big_vector.get(j).equals("data/Overflow of "+key+".ser")){
						File f=new File("data/Overflow of "+key+".ser");
			    		f.delete();
			    		Vector<Object> overflow_numbers=db.deserialize_Overflow_numbers();
		    	        overflow_numbers.remove(key);
		    	        db.serialize_Overflow_numbers(overflow_numbers);
						big_vector.remove(j);
					}
				}
				db.serialize_Overflow_page(big_vector);
				
				if(i == 0 && ptr > 0)
				{
					//update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				//check that node has enough keys
				if(!this.isRoot() && numberOfKeys < this.minKeys())
				{
					//1.try to borrow
					if(borrow(parent, ptr))
						return true;
					//2.merge
					merge(parent, ptr);
				}
				return true;
			}
		return false;
	}
	
	/**
	 * delete a key at the specified index of the node
	 * @param index the index of the key to be deleted
	 */
	public void deleteAt(int index)
	{
		for(int i = index; i < numberOfKeys - 1; ++i)
		{
			keys[i] = keys[i+1];
			records[i] = records[i+1];
		}
		numberOfKeys--;
	}
	
	/**
	 * tries to borrow a key from the left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if borrow is done successfully and false otherwise
	 */
	public boolean borrow(BPTreeInnerNode<T> parent, int ptr)
	{
		//check left sibling
		if(ptr > 0)
		{
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());		
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				return true;
			}
		}
		
		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
				return true;
			}
		}
		return false;
	}
	
	/**
	 * merges the current node with its left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 */
	public void merge(BPTreeInnerNode<T> parent, int ptr)
	{
		if(ptr > 0)
		{
			//merge with left
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			leftSibling.merge(this);
			parent.deleteAt(ptr-1);			
		}
		else
		{
			//merge with right
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}
	
	/**
	 * merge the current node with the specified node. The foreign node will be deleted
	 * @param foreignNode the node to be merged with the current node
	 */
	public void merge(BPTreeLeafNode<T> foreignNode)
	{
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));
		
		this.setNext(foreignNode.getNext());
	}
	
	
	public boolean update(T key, Ref old_refrence, Ref new_refrence, int ptr,
			BPTreeInnerNode<T> parent, boolean flag) {
		DBApp db = new DBApp();
		for (int i = 0; i < numberOfKeys; ++i) {
			if (keys[i].compareTo(key) == 0&& records[i].getPage() == old_refrence.getPage()&& records[i].getIndexInPage() == old_refrence.getIndexInPage()) {
				records[i] = new_refrence;
				return true;
			}

			else if (keys[i].compareTo(key) == 0) {
				Vector<String> big_vector = db.deserialize_Overflow_page();
				for (int z = 0; z < big_vector.size(); z++) {
					String text = "data/Overflow of " + key + ".ser";
					if (big_vector.get(z).equals(text)) {
						Vector<Ref> overflow_page = db.deserialize_classTree("data/Overflow of "+ key + ".ser");
						for (int j = 0; j < overflow_page.size(); j++) {
							Ref r = overflow_page.get(j);
							if (r.getPage() == old_refrence.getPage()
									&& r.getIndexInPage() == old_refrence
											.getIndexInPage()) {
								overflow_page.remove(j);
								overflow_page.add(new_refrence);
								Collections.sort(overflow_page);
								db.serialize_classtree(overflow_page,
										"data/Overflow of " + key + ".ser");
								db.serialize_Overflow_page(big_vector);
								return true;
							}
						}
					}
				}
			}
		}

		return false;
	}
	
	@Override
	public Ref search(T key) {
		for (int i = 0; i < numberOfKeys; ++i)
			if (this.getKey(i).compareTo(key) == 0)
				return this.getRecord(i);
		return null;
	}
	
	public Ref get(T key,String table_name,String col_name,Hashtable<String, Object> htblColNameValue) {
		DBApp db = new DBApp();
		Table t = db.deserializet("data/" + table_name + "" + ".ser");
		for (int i = 0; i < numberOfKeys; ++i) {
			if (this.getKey(i).compareTo(key) == 0) {
				Ref r = this.getRecord(i);
				Page p = db.deserialize(t.files.get(r.getPage()));
				
				if (p.Tuple.get(r.getIndexInPage()).Hash.get(col_name).equals(htblColNameValue.get(col_name))) {
					return this.getRecord(i);
				} else {
					Vector<String> big_vector = db.deserialize_Overflow_page();
					for (int z = 0; z < big_vector.size(); z++) {
						String text = "data/Overflow of " + key + ".ser";
						if (big_vector.get(z).equals(text)) {
							Vector<Ref> overflow_page = db.deserialize_classTree("data/Overflow of "+ key + ".ser");
							for (int j = 0; j < overflow_page.size(); j++) {
								Ref ref = overflow_page.get(j);
								 p = db.deserialize(t.files.get(ref.getPage()));
								if (p.Tuple.get(ref.getIndexInPage()).Hash.get(col_name).equals(htblColNameValue.get(col_name))) {
									return ref;}
							}
						}
					}
				}
			}
		}

		return null;
	}
	
	public boolean delete_using_Ref(T key, Ref recordReference,BPTreeInnerNode<T> parent, int ptr) {
		DBApp db = new DBApp();
		for (int i = 0; i < numberOfKeys; ++i) {
			if (keys[i].compareTo(key) == 0
					&& records[i].getPage()==recordReference.getPage()&&records[i].getIndexInPage()==recordReference.getIndexInPage()) {
				this.deleteAt(i);
				Vector<String> big_vector = db.deserialize_Overflow_page();
				for (int z = 0; z < big_vector.size(); z++) {
					String text = "data/Overflow of " + key + ".ser";
					if (big_vector.get(z).equals(text)) {
						Vector<Ref> overflow_page = db.deserialize_classTree("data/Overflow of "+ key + ".ser");
						if(overflow_page.size()>0){
				      this.insert(key,overflow_page.get(0), parent, ptr,"not cluster","no table");
				      overflow_page.remove(0);
				      db.serialize_classtree(overflow_page,"data/Overflow of " + key + ".ser");}}}
				
				
				if (i == 0 && ptr > 0) {
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				if (!this.isRoot() && numberOfKeys < this.minKeys()) {

					if (borrow(parent, ptr))
						return true;

					merge(parent, ptr);
				}
				return true;
			} else if (keys[i].compareTo(key) == 0) {
				Vector<String> big_vector = db.deserialize_Overflow_page();
				for (int z = 0; z < big_vector.size(); z++) {
					String text = "data/Overflow of " + key + ".ser";
					if (big_vector.get(z).equals(text)) {
						Vector<Ref> overflow_page = db
								.deserialize_classTree("data/Overflow of "
										+ key + ".ser");
						for (int j = 0; j < overflow_page.size(); j++) {
							Ref r = overflow_page.get(j);
							if (r.getPage() == recordReference.getPage()&& r.getIndexInPage() == recordReference.getIndexInPage()) {
								overflow_page.remove(j);
								db.serialize_classtree(overflow_page,"data/Overflow of " + key + ".ser");
							}
							
						}
						
						for (int j = 0; j < overflow_page.size()-1; j++) {
							if(overflow_page.get(j).getPage()==overflow_page.get(j+1).getPage()&&overflow_page.get(j).getIndexInPage()==overflow_page.get(j+1).getIndexInPage()){
								overflow_page.remove(j);
							}
						}
						
						if (overflow_page.size() == 0) {
							File f = new File("data/Overflow of " + key+ ".ser");
							f.delete();
							big_vector.remove(z);
							db.serialize_Overflow_page(big_vector);
							return true;
						}
					}
				}
			}
		}
		return false;
	
						}
	
	public boolean update_for_insert(T key, Ref oldR, Ref newR, int ptr,
			BPTreeInnerNode<T> parent) {
		for (int i = 0; i < numberOfKeys; ++i) {
			if (keys[i].compareTo(key) == 0) {
				
				records[i]=newR;
				return true;}
				
			}
		
		return false;
	}
	
	
}
