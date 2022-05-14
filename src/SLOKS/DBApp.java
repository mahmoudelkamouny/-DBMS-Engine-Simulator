package SLOKS;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

import javax.crypto.spec.IvParameterSpec;
import javax.swing.text.html.HTMLDocument.Iterator;


public class DBApp implements Serializable {
	static Vector<String> TableFiles = new Vector<String>();

	

	public void init() {// call this only the first time you create new dbapp

		serlizetf(TableFiles);
		Vector<String> big_vector = new Vector<String>();
		serialize_Overflow_page(big_vector);

		Vector<Object> overflow_numbers = new Vector<Object>();
		serialize_Overflow_numbers(overflow_numbers);
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		TableFiles = deserlizetf();
		boolean flag = false;
		int i;

		for (i = 0; i < TableFiles.size(); i++) {
			if (TableFiles.get(i).equals("data/" + strTableName + "" + ".ser")) {
				flag = true;
			}
		}

		if (flag == false) {
			String text = "";
			Set<String> keys = htblColNameType.keySet();
			for (String key : keys) {

				if (strClusteringKeyColumn.equals(key)) {
					text = text + strTableName + "," + "" + key + ","
							+ htblColNameType.get(key) + "," + "True" + ","
							+ "False" + "\n";
				} else {

					text = text + strTableName + "," + "" + key + ","
							+ htblColNameType.get(key) + "" + "," + "False"
							+ "," + "False" + "\n";
				}

			}
			writeToFile("data/metadata.class", text);

			Table t = new Table(strTableName, strClusteringKeyColumn);

			String s = "data/" + strTableName + "" + ".ser";
			serializet(t, s);
			TableFiles.add(i, s);
			serlizetf(TableFiles);
		}

	}

	public void createBTreeIndex(String strTableName, String strColName)
			throws DBAppException, IOException {
		// 1-update metadata file
		
		int size = read_config_file();

		// 2-create the index
		// 3- we want to read properties file
		if(!check_Index(strTableName, strColName)){
			update_metadata_forINDEX(strTableName, strColName);	
		Boolean flag=false;
		Table t=deserializet("data/" + strTableName + "" + ".ser");
		if(t.files.size()!=0&&(getcluster(strTableName,"data/metadata.class")).equals(strColName)){
			flag=true;
		}
		
		if (getcoltypeforindex(strTableName, strColName).equals("java.lang.Integer")) {
			BPTree<Integer> b = new BPTree<Integer>(size);
			 t = deserializet("data/" + strTableName + "" + ".ser");
			for (int i = 0; i < t.files.size(); i++) {
				Page p = deserialize(t.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int col_data = (int) p.Tuple.get(j).Hash.get(strColName);
					Ref refrence = new Ref(i, j);
					b.insert(col_data, refrence,strColName,strTableName);
				}
			}
			serialize_BTree(strTableName, strColName, b);
		} else if (getcoltypeforindex(strTableName, strColName).equals("java.lang.String")) {
			BPTree<String> b = new BPTree<String>(size);
			 t = deserializet("data/" + strTableName + "" + ".ser");
			for (int i = 0; i < t.files.size(); i++) {
				Page p = deserialize(t.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String col_data = (String) p.Tuple.get(j).Hash.get(strColName);
					Ref refrence = new Ref(i, j);
					b.insert(col_data, refrence,strColName,strTableName);
				}
			}
			serialize_BTree(strTableName, strColName, b);
		} else if (getcoltypeforindex(strTableName, strColName).equals("java.lang.double")) {
			BPTree<Double> b = new BPTree<Double>(size);
			 t = deserializet("data/" + strTableName + "" + ".ser");
			for (int i = 0; i < t.files.size(); i++) {
				Page p = deserialize(t.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double col_data = (double) p.Tuple.get(j).Hash
							.get(strColName);
					Ref refrence = new Ref(i, j);
					b.insert(col_data, refrence,strColName,strTableName);
				}
			}
			serialize_BTree(strTableName, strColName, b);
		} else if (getcoltypeforindex(strTableName, strColName).equals(
				"java.lang.Boolean")) {
			BPTree<Boolean> b = new BPTree<Boolean>(size);
			  t = deserializet("data/" + strTableName + "" + ".ser");
			for (int i = 0; i < t.files.size(); i++) {
				Page p = deserialize(t.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean col_data = (Boolean) p.Tuple.get(j).Hash
							.get(strColName);
					Ref refrence = new Ref(i, j);
					b.insert(col_data, refrence,strColName,strTableName);
				}
			}
			
			serialize_BTree(strTableName, strColName, b);
		} else if (getcoltypeforindex(strTableName, strColName).equals(
				"java.util.Date")) {
			BPTree<Date> b = new BPTree<Date>(size);
			 t = deserializet("data/" + strTableName + "" + ".ser");
			for (int i = 0; i < t.files.size(); i++) {
				Page p = deserialize(t.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date col_data = (Date) p.Tuple.get(j).Hash.get(strColName);
					Ref refrence = new Ref(i, j);
					b.insert(col_data, refrence,strColName,strTableName);
				}
			}
			
			serialize_BTree(strTableName, strColName, b);
		}
		if (flag==true){
	
			update_ref(strTableName,strColName);
		}}
		else{
			throw new DBAppException("column already has an index");
		}
	}

	public void createRTreeIndex(String strTableName, String strColName)
			throws DBAppException {
		int size = read_config_file();
		if (!check_Index(strTableName, strColName)) {
			update_metadata_forINDEX(strTableName, strColName);
			Boolean flag = false;
			Table t = deserializet("data/" + strTableName + "" + ".ser");
			try {
				if (t.files.size() != 0
						&& (getcluster(strTableName, "data/metadata.class"))
								.equals(strColName)) {
					flag = true;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (getcoltypeforindex(strTableName, strColName).equals("java.awt.Polygon")) {
				RTree<Double> b = new RTree<Double>(size);
				t = deserializet("data/" + strTableName + "" + ".ser");
				for (int i = 0; i < t.files.size(); i++) {
					Page p = deserialize(t.files.get(i));
					for (int j = 0; j < p.Tuple.size(); j++) {
						polygon a = (polygon) p.Tuple.get(j).Hash
								.get(strColName);
						double col_data = a.area;
						Ref refrence = new Ref(i, j);
						b.insert(col_data, refrence, strColName,strTableName);
					}
				}
				serialize_RTree(strTableName, strColName, b);
			}

			if (flag == true) {

				update_ref(strTableName, strColName);
			}
		} else {
			throw new DBAppException("column already has an index");
		}

		
	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException,
			IOException {
		Ref reference = null;
		DBApp db = new DBApp();
		if (check_if_cluster_indexed(strTableName)) {
			String strColName = getcluster(strTableName, "data/metadata.class");
			if (getcoltypeforindex(strTableName, strColName).equals(
					"java.lang.Integer")) {
				int key = (int) htblColNameValue.get(strColName);
				BPTree<Integer> b = deserialize_BTree(strTableName, strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				if (t.files.size() != 0) {
					Page Last_page = deserialize(t.files
							.get(t.files.size() - 1));
					reference = b.search(
							(int) (htblColNameValue.get(strColName)),
							Last_page, t.files.size() - 1);
					if (reference.getIndexInPage() == Last_page.Tuple
							.capacity()) {
						Ref r10 = new Ref(reference.getPage() + 1, 0);
						reference = r10;
					}

				} else {
					reference = b.search(
							(int) (htblColNameValue.get(strColName)), null, -1);
				}
				Tuple tuple = new Tuple(t.ClusteringKey, htblColNameValue);
				if (reference.getPage() != -1) {
					if (reference.getPage() == t.files.size()) {
						Page p2 = createpage(t, strTableName);
						p2.Tuple.add(tuple);
						int g = t.files.size() - 1;
						Ref ref = new Ref(g, 0);
						b.insert((Integer) htblColNameValue.get(strColName),
								ref,strColName,strTableName);
						serialize(p2, "data/" + strTableName + "" + g + ""
								+ ".ser");
					} else {
						Page page = deserialize(t.files
								.get(reference.getPage()));
						Tuple Last_tuple = page.Tuple
								.get(page.Tuple.size() - 1);
						Ref ref = new Ref(reference.getPage(),
								reference.getIndexInPage() + 1);

						b.insert((Integer) htblColNameValue.get(strColName),
								ref,strColName,strTableName);
						if (reference.getPage() != t.files.size() - 1) {
							page.Tuple.remove(page.Tuple.size() - 1);
							page.Tuple.add(page.Tuple.size() - 1, tuple);
							Collections.sort(page.Tuple); // sort the page
							for (int j = 0; j < page.Tuple.size(); j++) {
								Ref ref_new = new Ref(reference.getPage(), j);
								Ref ref_old = b.search((Integer) page.Tuple
										.get(j).Hash.get(strColName));
								b.update_for_insert(
										(Integer) page.Tuple.get(j).Hash
												.get(strColName), ref_old,
										ref_new);
							}
							serialize(page, t.files.get(reference.getPage()));

							for (int i = reference.getPage() + 1; i < t.files
									.size(); i++) {
								Page p = deserialize(t.files.get(i));
								if (p.Tuple.size() == p.Tuple.capacity()
										&& i != t.files.size() - 1) {
									Tuple temp = p.Tuple
											.get(p.Tuple.size() - 1);
									p.Tuple.remove(p.Tuple.size() - 1);
									p.Tuple.add(p.Tuple.size() - 1, Last_tuple);
									Last_tuple = temp;
									Collections.sort(p.Tuple); // sort the page
									for (int j = 0; j < p.Tuple.size(); j++) {
										Ref ref_new = new Ref(i, j);
										Ref ref_old = b
												.search((Integer) p.Tuple
														.get(j).Hash
														.get(strColName));

										b.update_for_insert((Integer) p.Tuple
												.get(j).Hash.get(strColName),
												ref_old, ref_new);
									}
									serialize(p, t.files.get(i));
								}
								if (i == t.files.size() - 1) {
									if (p.Tuple.size() == p.Tuple.capacity()) {
										Tuple temp = p.Tuple
												.get(p.Tuple.size() - 1);
										p.Tuple.remove(p.Tuple.size() - 1);
										p.Tuple.add(p.Tuple.size() - 1,
												Last_tuple);
										Last_tuple = temp;
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Integer) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Integer) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i)); // serialize
										Page p2 = createpage(t, strTableName); // create
										p2.Tuple.add(0, Last_tuple); // insert
										p2.Tuple.remove(0); // remove the tuple
										Ref r = new Ref(0, 0);
										b.insert((Integer) tuple.Hash
												.get(strColName), r,strColName,strTableName);
										serialize(p2, t.files.get(i + 1));
									} else { // last page not full
										p.Tuple.add(Last_tuple);
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Integer) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Integer) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i));
									}
								}
							}
						}

						else if (reference.getPage() == t.files.size() - 1) {
							if (page.Tuple.size() == page.Tuple.capacity()) {
								Tuple temp = page.Tuple
										.get(page.Tuple.size() - 1);
								page.Tuple.remove(page.Tuple.size() - 1);
								page.Tuple.add(page.Tuple.size() - 1, tuple);
								Last_tuple = temp;
								Collections.sort(page.Tuple); // sort the page
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Integer) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Integer) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
								Page p2 = createpage(t, strTableName);
								p2.Tuple.add(0, Last_tuple);
								for (int j = 0; j < p2.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Integer) p2.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Integer) p2.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(p2,
										t.files.get(reference.getPage() + 1));

							} else { // last page not full
								page.Tuple.add(tuple);
								Collections.sort(page.Tuple);
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Integer) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Integer) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
							}
						}
					}
				} else {
					Page p2 = createpage(t, strTableName); // create new page
					p2.Tuple.add(0, tuple); // insert in it
					Ref ref = new Ref(0, 0);
					b.insert((Integer) htblColNameValue.get(strColName), ref,strColName,strTableName);
					serialize(p2, t.files.get(reference.getPage() + 1));
				}
				Page p = new Page();
				db.update_ref(key, p);
				serializet(t, strTableName + ".ser");
				serialize_BTree(strTableName, strColName, b);
			}

			else if (getcoltypeforindex(strTableName, strColName).equals(
					"java.lang.String")) {

				String key = (String) htblColNameValue.get(strColName);
				BPTree<String> b = deserialize_BTree(strTableName, strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				if (t.files.size() != 0) {
					Page Last_page = deserialize(t.files
							.get(t.files.size() - 1));
					reference = b.search(
							(String) (htblColNameValue.get(strColName)),
							Last_page, t.files.size() - 1);
					if (reference.getIndexInPage() == Last_page.Tuple
							.capacity()) {
						Ref r10 = new Ref(reference.getPage() + 1, 0);
						reference = r10;
					}

				} else {
					reference = b.search(
							(String) (htblColNameValue.get(strColName)), null,
							-1);
				}
				Tuple tuple = new Tuple(t.ClusteringKey, htblColNameValue);
				if (reference.getPage() != -1) {
					if (reference.getPage() == t.files.size()) {
						Page p2 = createpage(t, strTableName);
						p2.Tuple.add(tuple);
						int g = t.files.size() - 1;
						Ref ref = new Ref(g, 0);
						b.insert((String) htblColNameValue.get(strColName), ref,strColName,strTableName);
						serialize(p2, "data/" + strTableName + "" + g + ""
								+ ".ser");
					} else {
						Page page = deserialize(t.files
								.get(reference.getPage()));
						Tuple Last_tuple = page.Tuple
								.get(page.Tuple.size() - 1);
						Ref ref = new Ref(reference.getPage(),
								reference.getIndexInPage() + 1);
						b.insert((String) htblColNameValue.get(strColName), ref,strColName,strTableName);
						if (reference.getPage() != t.files.size() - 1) {
							page.Tuple.remove(page.Tuple.size() - 1);
							page.Tuple.add(page.Tuple.size() - 1, tuple);
							Collections.sort(page.Tuple); // sort the page
							for (int j = 0; j < page.Tuple.size(); j++) {
								Ref ref_new = new Ref(reference.getPage(), j);
								Ref ref_old = b.search((String) page.Tuple
										.get(j).Hash.get(strColName));
								b.update_for_insert(
										(String) page.Tuple.get(j).Hash
												.get(strColName), ref_old,
										ref_new);
							}
							serialize(page, t.files.get(reference.getPage()));

							for (int i = reference.getPage() + 1; i < t.files
									.size(); i++) {
								Page p = deserialize(t.files.get(i));
								if (p.Tuple.size() == p.Tuple.capacity()
										&& i != t.files.size() - 1) {
									Tuple temp = p.Tuple
											.get(p.Tuple.size() - 1);
									p.Tuple.remove(p.Tuple.size() - 1);
									p.Tuple.add(p.Tuple.size() - 1, Last_tuple);
									Last_tuple = temp;
									Collections.sort(p.Tuple); // sort the page
									for (int j = 0; j < p.Tuple.size(); j++) {
										Ref ref_new = new Ref(i, j);
										Ref ref_old = b.search((String) p.Tuple
												.get(j).Hash.get(strColName));

										b.update_for_insert((String) p.Tuple
												.get(j).Hash.get(strColName),
												ref_old, ref_new);
									}
									serialize(p, t.files.get(i));
								}
								if (i == t.files.size() - 1) {
									if (p.Tuple.size() == p.Tuple.capacity()) {
										Tuple temp = p.Tuple
												.get(p.Tuple.size() - 1);
										p.Tuple.remove(p.Tuple.size() - 1);
										p.Tuple.add(p.Tuple.size() - 1,
												Last_tuple);
										Last_tuple = temp;
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((String) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(String) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i)); // serialize
										Page p2 = createpage(t, strTableName); // create
										p2.Tuple.add(0, Last_tuple); // insert
										p2.Tuple.remove(0); // remove the tuple
										Ref r = new Ref(0, 0);
										b.insert((String) tuple.Hash
												.get(strColName), r,strColName,strTableName);
										serialize(p2, t.files.get(i + 1));
									} else { // last page not full
										p.Tuple.add(Last_tuple);
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((String) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(String) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i));
									}
								}
							}
						}

						else if (reference.getPage() == t.files.size() - 1) {
							if (page.Tuple.size() == page.Tuple.capacity()) {
								Tuple temp = page.Tuple
										.get(page.Tuple.size() - 1);
								page.Tuple.remove(page.Tuple.size() - 1);
								page.Tuple.add(page.Tuple.size() - 1, tuple);
								Last_tuple = temp;
								Collections.sort(page.Tuple); // sort the page
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((String) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((String) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
								Page p2 = createpage(t, strTableName);
								p2.Tuple.add(0, Last_tuple);
								for (int j = 0; j < p2.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((String) p2.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((String) p2.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(p2,
										t.files.get(reference.getPage() + 1));

							} else { // last page not full
								page.Tuple.add(tuple);
								Collections.sort(page.Tuple);
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((String) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((String) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
							}
						}
					}
				} else {
					Page p2 = createpage(t, strTableName); // create new page
					p2.Tuple.add(0, tuple); // insert in it
					Ref ref = new Ref(0, 0);
					b.insert((String) htblColNameValue.get(strColName), ref,strColName,strTableName);
					serialize(p2, t.files.get(reference.getPage() + 1));
				}
				Page p = new Page();
				db.update_ref_String(key, p);
				serializet(t, strTableName + ".ser");
				serialize_BTree(strTableName, strColName, b);
				
			} else if (getcoltypeforindex(strTableName, strColName).equals(
					"java.lang.double")) {
				BPTree<Double> b = deserialize_BTree(strTableName, strColName);
				Double key = (Double) htblColNameValue.get(strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				if (t.files.size() != 0) {
					Page Last_page = deserialize(t.files
							.get(t.files.size() - 1));
					reference = b.search(
							(Double) (htblColNameValue.get(strColName)),
							Last_page, t.files.size() - 1);
					if (reference.getIndexInPage() == Last_page.Tuple
							.capacity()) {
						Ref r10 = new Ref(reference.getPage() + 1, 0);
						reference = r10;
					}

				} else {
					reference = b.search(
							(Double) (htblColNameValue.get(strColName)), null,
							-1);
				}
				Tuple tuple = new Tuple(t.ClusteringKey, htblColNameValue);
				if (reference.getPage() != -1) {
					if (reference.getPage() == t.files.size()) {
						Page p2 = createpage(t, strTableName);
						p2.Tuple.add(tuple);
						int g = t.files.size() - 1;
						Ref ref = new Ref(g, 0);
						b.insert((Double) htblColNameValue.get(strColName), ref,strColName,strTableName);
						serialize(p2, "data/" + strTableName + "" + g + ""
								+ ".ser");
					} else {
						Page page = deserialize(t.files
								.get(reference.getPage()));
						Tuple Last_tuple = page.Tuple
								.get(page.Tuple.size() - 1);
						Ref ref = new Ref(reference.getPage(),
								reference.getIndexInPage() + 1);
						b.insert((Double) htblColNameValue.get(strColName), ref,strColName,strTableName);
						if (reference.getPage() != t.files.size() - 1) {
							page.Tuple.remove(page.Tuple.size() - 1);
							page.Tuple.add(page.Tuple.size() - 1, tuple);
							Collections.sort(page.Tuple); // sort the page
							for (int j = 0; j < page.Tuple.size(); j++) {
								Ref ref_new = new Ref(reference.getPage(), j);
								Ref ref_old = b.search((Double) page.Tuple
										.get(j).Hash.get(strColName));
								b.update_for_insert(
										(Double) page.Tuple.get(j).Hash
												.get(strColName), ref_old,
										ref_new);
							}
							serialize(page, t.files.get(reference.getPage()));

							for (int i = reference.getPage() + 1; i < t.files
									.size(); i++) {
								Page p = deserialize(t.files.get(i));
								if (p.Tuple.size() == p.Tuple.capacity()
										&& i != t.files.size() - 1) {
									Tuple temp = p.Tuple
											.get(p.Tuple.size() - 1);
									p.Tuple.remove(p.Tuple.size() - 1);
									p.Tuple.add(p.Tuple.size() - 1, Last_tuple);
									Last_tuple = temp;
									Collections.sort(p.Tuple); // sort the page
									for (int j = 0; j < p.Tuple.size(); j++) {
										Ref ref_new = new Ref(i, j);
										Ref ref_old = b.search((Double) p.Tuple
												.get(j).Hash.get(strColName));

										b.update_for_insert((Double) p.Tuple
												.get(j).Hash.get(strColName),
												ref_old, ref_new);
									}
									serialize(p, t.files.get(i));
								}
								if (i == t.files.size() - 1) {
									if (p.Tuple.size() == p.Tuple.capacity()) {
										Tuple temp = p.Tuple
												.get(p.Tuple.size() - 1);
										p.Tuple.remove(p.Tuple.size() - 1);
										p.Tuple.add(p.Tuple.size() - 1,
												Last_tuple);
										Last_tuple = temp;
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Double) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Double) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i)); // serialize
										Page p2 = createpage(t, strTableName); // create
										p2.Tuple.add(0, Last_tuple); // insert
										p2.Tuple.remove(0); // remove the tuple
										Ref r = new Ref(0, 0);
										b.insert((Double) tuple.Hash
												.get(strColName), r,strColName,strTableName);
										serialize(p2, t.files.get(i + 1));
									} else { // last page not full
										p.Tuple.add(Last_tuple);
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Double) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Double) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i));
									}
								}
							}
						}

						else if (reference.getPage() == t.files.size() - 1) {
							if (page.Tuple.size() == page.Tuple.capacity()) {
								Tuple temp = page.Tuple
										.get(page.Tuple.size() - 1);
								page.Tuple.remove(page.Tuple.size() - 1);
								page.Tuple.add(page.Tuple.size() - 1, tuple);
								Last_tuple = temp;
								Collections.sort(page.Tuple); // sort the page
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Double) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Double) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
								Page p2 = createpage(t, strTableName);
								p2.Tuple.add(0, Last_tuple);
								for (int j = 0; j < p2.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Double) p2.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Double) p2.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(p2,
										t.files.get(reference.getPage() + 1));

							} else { // last page not full
								page.Tuple.add(tuple);

								

								Collections.sort(page.Tuple);
								

								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Double) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Double) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
							}
						}
					}
				} else {
					Page p2 = createpage(t, strTableName); // create new page
					p2.Tuple.add(0, tuple); // insert in it
					Ref ref = new Ref(0, 0);
					b.insert((Double) htblColNameValue.get(strColName), ref,strColName,strTableName);
					serialize(p2, t.files.get(reference.getPage() + 1));
				}
				Page p = new Page();
				db.update_ref_double(key, p);
				serializet(t, strTableName + ".ser");
				serialize_BTree(strTableName, strColName, b);

			}

			else if (getcoltypeforindex(strTableName, strColName).equals(
					"java.lang.Boolean")) {
				BPTree<Boolean> b = deserialize_BTree(strTableName, strColName);
				Boolean key = (Boolean) htblColNameValue.get(strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				if (t.files.size() != 0) {
					Page Last_page = deserialize(t.files
							.get(t.files.size() - 1));
					reference = b.search(
							(Boolean) (htblColNameValue.get(strColName)),
							Last_page, t.files.size() - 1);
					if (reference.getIndexInPage() == Last_page.Tuple
							.capacity()) {
						Ref r10 = new Ref(reference.getPage() + 1, 0);
						reference = r10;
					}

				} else {
					reference = b.search(
							(Boolean) (htblColNameValue.get(strColName)), null,
							-1);
				}
				Tuple tuple = new Tuple(t.ClusteringKey, htblColNameValue);
				if (reference.getPage() != -1) {
					if (reference.getPage() == t.files.size()) {
						Page p2 = createpage(t, strTableName);
						p2.Tuple.add(tuple);
						int g = t.files.size() - 1;
						Ref ref = new Ref(g, 0);
						b.insert((Boolean) htblColNameValue.get(strColName), ref,strColName,strTableName);
						serialize(p2, "data/" + strTableName + "" + g + ""
								+ ".ser");
					} else {
						Page page = deserialize(t.files
								.get(reference.getPage()));
						Tuple Last_tuple = page.Tuple
								.get(page.Tuple.size() - 1);
						Ref ref = new Ref(reference.getPage(),
								reference.getIndexInPage() + 1);
						b.insert((Boolean) htblColNameValue.get(strColName), ref,strColName,strTableName);
						if (reference.getPage() != t.files.size() - 1) {
							page.Tuple.remove(page.Tuple.size() - 1);
							page.Tuple.add(page.Tuple.size() - 1, tuple);
							Collections.sort(page.Tuple); // sort the page
							for (int j = 0; j < page.Tuple.size(); j++) {
								Ref ref_new = new Ref(reference.getPage(), j);
								Ref ref_old = b.search((Boolean) page.Tuple
										.get(j).Hash.get(strColName));
								b.update_for_insert(
										(Boolean) page.Tuple.get(j).Hash
												.get(strColName), ref_old,
										ref_new);
							}
							serialize(page, t.files.get(reference.getPage()));

							for (int i = reference.getPage() + 1; i < t.files
									.size(); i++) {
								Page p = deserialize(t.files.get(i));
								if (p.Tuple.size() == p.Tuple.capacity()
										&& i != t.files.size() - 1) {
									Tuple temp = p.Tuple
											.get(p.Tuple.size() - 1);
									p.Tuple.remove(p.Tuple.size() - 1);
									p.Tuple.add(p.Tuple.size() - 1, Last_tuple);
									Last_tuple = temp;
									Collections.sort(p.Tuple); // sort the page
									for (int j = 0; j < p.Tuple.size(); j++) {
										Ref ref_new = new Ref(i, j);
										Ref ref_old = b.search((Boolean) p.Tuple
												.get(j).Hash.get(strColName));

										b.update_for_insert((Boolean) p.Tuple
												.get(j).Hash.get(strColName),
												ref_old, ref_new);
									}
									serialize(p, t.files.get(i));
								}
								if (i == t.files.size() - 1) {
									if (p.Tuple.size() == p.Tuple.capacity()) {
										Tuple temp = p.Tuple
												.get(p.Tuple.size() - 1);
										p.Tuple.remove(p.Tuple.size() - 1);
										p.Tuple.add(p.Tuple.size() - 1,
												Last_tuple);
										Last_tuple = temp;
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Boolean) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Boolean) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i)); // serialize
										Page p2 = createpage(t, strTableName); // create
										p2.Tuple.add(0, Last_tuple); // insert
										p2.Tuple.remove(0); // remove the tuple
										Ref r = new Ref(0, 0);
										b.insert((Boolean) tuple.Hash
												.get(strColName), r,strColName,strTableName);
										serialize(p2, t.files.get(i + 1));
									} else { // last page not full
										p.Tuple.add(Last_tuple);
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Boolean) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Boolean) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i));
									}
								}
							}
						}

						else if (reference.getPage() == t.files.size() - 1) {
							if (page.Tuple.size() == page.Tuple.capacity()) {
								Tuple temp = page.Tuple
										.get(page.Tuple.size() - 1);
								page.Tuple.remove(page.Tuple.size() - 1);
								page.Tuple.add(page.Tuple.size() - 1, tuple);
								Last_tuple = temp;
								Collections.sort(page.Tuple); // sort the page
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Boolean) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Boolean) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
								Page p2 = createpage(t, strTableName);
								p2.Tuple.add(0, Last_tuple);
								for (int j = 0; j < p2.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Boolean) p2.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Boolean) p2.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(p2,
										t.files.get(reference.getPage() + 1));

							} else { // last page not full
								page.Tuple.add(tuple);

							

								Collections.sort(page.Tuple);
								

								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Boolean) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Boolean) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
							}
						}
					}
				} else {
					Page p2 = createpage(t, strTableName); // create new page
					p2.Tuple.add(0, tuple); // insert in it
					Ref ref = new Ref(0, 0);
					b.insert((Boolean) htblColNameValue.get(strColName), ref,strColName,strTableName);
					serialize(p2, t.files.get(reference.getPage() + 1));
				}
				Page p = new Page();
				db.update_ref_Boolean(key, p);
				serializet(t, strTableName + ".ser");
				serialize_BTree(strTableName, strColName, b);
			}

			else if (getcoltypeforindex(strTableName, strColName).equals(
					"java.util.Date")) {
				BPTree<Date> b = deserialize_BTree(strTableName, strColName);
				Date key = (Date) htblColNameValue.get(strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				if (t.files.size() != 0) {
					Page Last_page = deserialize(t.files
							.get(t.files.size() - 1));
					reference = b.search(
							(Date) (htblColNameValue.get(strColName)),
							Last_page, t.files.size() - 1);
					if (reference.getIndexInPage() == Last_page.Tuple
							.capacity()) {
						Ref r10 = new Ref(reference.getPage() + 1, 0);
						reference = r10;
					}

				} else {
					reference = b.search(
							(Date) (htblColNameValue.get(strColName)), null,
							-1);
				}
				Tuple tuple = new Tuple(t.ClusteringKey, htblColNameValue);
				if (reference.getPage() != -1) {
					if (reference.getPage() == t.files.size()) {
						Page p2 = createpage(t, strTableName);
						p2.Tuple.add(tuple);
						int g = t.files.size() - 1;
						Ref ref = new Ref(g, 0);
						b.insert((Date) htblColNameValue.get(strColName), ref,strColName,strTableName);
						serialize(p2, "data/" + strTableName + "" + g + ""
								+ ".ser");
					} else {
						Page page = deserialize(t.files
								.get(reference.getPage()));
						Tuple Last_tuple = page.Tuple
								.get(page.Tuple.size() - 1);
						Ref ref = new Ref(reference.getPage(),
								reference.getIndexInPage() + 1);
						b.insert((Date) htblColNameValue.get(strColName), ref,strColName,strTableName);
						if (reference.getPage() != t.files.size() - 1) {
							page.Tuple.remove(page.Tuple.size() - 1);
							page.Tuple.add(page.Tuple.size() - 1, tuple);
							Collections.sort(page.Tuple); // sort the page
							for (int j = 0; j < page.Tuple.size(); j++) {
								Ref ref_new = new Ref(reference.getPage(), j);
								Ref ref_old = b.search((Date) page.Tuple
										.get(j).Hash.get(strColName));
								b.update_for_insert(
										(Date) page.Tuple.get(j).Hash
												.get(strColName), ref_old,
										ref_new);
							}
							serialize(page, t.files.get(reference.getPage()));

							for (int i = reference.getPage() + 1; i < t.files
									.size(); i++) {
								Page p = deserialize(t.files.get(i));
								if (p.Tuple.size() == p.Tuple.capacity()
										&& i != t.files.size() - 1) {
									Tuple temp = p.Tuple
											.get(p.Tuple.size() - 1);
									p.Tuple.remove(p.Tuple.size() - 1);
									p.Tuple.add(p.Tuple.size() - 1, Last_tuple);
									Last_tuple = temp;
									Collections.sort(p.Tuple); // sort the page
									for (int j = 0; j < p.Tuple.size(); j++) {
										Ref ref_new = new Ref(i, j);
										Ref ref_old = b.search((Date) p.Tuple
												.get(j).Hash.get(strColName));

										b.update_for_insert((Date) p.Tuple
												.get(j).Hash.get(strColName),
												ref_old, ref_new);
									}
									serialize(p, t.files.get(i));
								}
								if (i == t.files.size() - 1) {
									if (p.Tuple.size() == p.Tuple.capacity()) {
										Tuple temp = p.Tuple
												.get(p.Tuple.size() - 1);
										p.Tuple.remove(p.Tuple.size() - 1);
										p.Tuple.add(p.Tuple.size() - 1,
												Last_tuple);
										Last_tuple = temp;
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Date) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Date) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i)); // serialize
										Page p2 = createpage(t, strTableName); // create
										p2.Tuple.add(0, Last_tuple); // insert
										p2.Tuple.remove(0); // remove the tuple
										Ref r = new Ref(0, 0);
										b.insert((Date) tuple.Hash
												.get(strColName), r,strColName,strTableName);
										serialize(p2, t.files.get(i + 1));
									} else { // last page not full
										p.Tuple.add(Last_tuple);
										Collections.sort(p.Tuple);
										for (int j = 0; j < p.Tuple.size(); j++) {
											Ref ref_new = new Ref(i, j);
											Ref ref_old = b
													.search((Date) p.Tuple
															.get(j).Hash
															.get(strColName));
											b.update_for_insert(
													(Date) p.Tuple.get(j).Hash
															.get(strColName),
													ref_old, ref_new);
										}
										serialize(p, t.files.get(i));
									}
								}
							}
						}

						else if (reference.getPage() == t.files.size() - 1) {
							if (page.Tuple.size() == page.Tuple.capacity()) {
								Tuple temp = page.Tuple
										.get(page.Tuple.size() - 1);
								page.Tuple.remove(page.Tuple.size() - 1);
								page.Tuple.add(page.Tuple.size() - 1, tuple);
								Last_tuple = temp;
								Collections.sort(page.Tuple); // sort the page
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Date) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Date) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
								Page p2 = createpage(t, strTableName);
								p2.Tuple.add(0, Last_tuple);
								for (int j = 0; j < p2.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Date) p2.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Date) p2.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(p2,
										t.files.get(reference.getPage() + 1));

							} else { // last page not full
								page.Tuple.add(tuple);

								

								Collections.sort(page.Tuple);
								

								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search((Date) page.Tuple
											.get(j).Hash.get(strColName));
									b.update_for_insert((Date) page.Tuple
											.get(j).Hash.get(strColName),
											ref_old, ref_new);
								}
								serialize(page,
										t.files.get(reference.getPage()));
							}
						}
					}
				} else {
					Page p2 = createpage(t, strTableName); // create new page
					p2.Tuple.add(0, tuple); // insert in it
					Ref ref = new Ref(0, 0);
					b.insert((Date) htblColNameValue.get(strColName), ref,strColName,strTableName);
					serialize(p2, t.files.get(reference.getPage() + 1));
				}
				Page p = new Page();
				db.update_ref_Date(key, p);
				serializet(t, strTableName + ".ser");
				serialize_BTree(strTableName, strColName, b);
			}
			else if (getcoltypeforindex(strTableName, strColName).equals("java.awt.Polygon")) {
				RTree<Double> b = deserialize_RTree(strTableName, strColName);
				polygon a = (polygon) htblColNameValue.get(strColName);
				Double key=a.area;
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				if (t.files.size() != 0) {
					Page Last_page = deserialize(t.files.get(t.files.size() - 1));
					polygon tem=(polygon) (htblColNameValue.get(strColName));
					reference = b.search(tem.area,Last_page, t.files.size() - 1);
					if (reference.getIndexInPage() == Last_page.Tuple.capacity()) {
						Ref r10 = new Ref(reference.getPage() + 1, 0);
						reference = r10;
					}

				} else {
					reference = b.search(((polygon)(htblColNameValue.get(strColName))).area,null,-1);
				}
				Tuple tuple = new Tuple(t.ClusteringKey, htblColNameValue);
				if (reference.getPage() != -1) {
					if (reference.getPage() == t.files.size()) {
						Page p2 = createpage(t, strTableName);
						p2.Tuple.add(tuple);
						int g = t.files.size() - 1;
						Ref ref = new Ref(g, 0);
						b.insert(((polygon) htblColNameValue.get(strColName)).area, ref,strColName,strTableName);
						serialize(p2, "data/" + strTableName + "" + g + ""
								+ ".ser");
					} else {
						Page page = deserialize(t.files
								.get(reference.getPage()));
						Tuple Last_tuple = page.Tuple.get(page.Tuple.size() - 1);
						Ref ref = new Ref(reference.getPage(),reference.getIndexInPage() + 1);
						b.insert((Double) key, ref,strColName,strTableName);
						if (reference.getPage() != t.files.size() - 1) {
							page.Tuple.remove(page.Tuple.size() - 1);
							page.Tuple.add(page.Tuple.size() - 1, tuple);
							Collections.sort(page.Tuple); // sort the page
							for (int j = 0; j < page.Tuple.size(); j++) {
								Ref ref_new = new Ref(reference.getPage(), j);
								Ref ref_old = b.search(((polygon) page.Tuple.get(j).Hash.get(strColName)).area);
								b.update_for_insert(((polygon) page.Tuple.get(j).Hash.get(strColName)).area, ref_old,ref_new);
							}
							serialize(page, t.files.get(reference.getPage()));

							for (int i = reference.getPage() + 1; i < t.files
									.size(); i++) {
								Page p = deserialize(t.files.get(i));
								if (p.Tuple.size() == p.Tuple.capacity()
										&& i != t.files.size() - 1) {
									Tuple temp = p.Tuple
											.get(p.Tuple.size() - 1);
									p.Tuple.remove(p.Tuple.size() - 1);
									p.Tuple.add(p.Tuple.size() - 1, Last_tuple);
									Last_tuple = temp;
									Collections.sort(p.Tuple); // sort the page
									for (int j = 0; j < page.Tuple.size(); j++) {
										Ref ref_new = new Ref(reference.getPage(), j);
										Ref ref_old = b.search(((polygon) page.Tuple.get(j).Hash.get(strColName)).area);
										b.update_for_insert(((polygon) page.Tuple.get(j).Hash.get(strColName)).area, ref_old,ref_new);
									}
									serialize(p, t.files.get(i));
								}
								if (i == t.files.size() - 1) {
									if (p.Tuple.size() == p.Tuple.capacity()) {
										Tuple temp = p.Tuple
												.get(p.Tuple.size() - 1);
										p.Tuple.remove(p.Tuple.size() - 1);
										p.Tuple.add(p.Tuple.size() - 1,
												Last_tuple);
										Last_tuple = temp;
										Collections.sort(p.Tuple);
										for (int j = 0; j < page.Tuple.size(); j++) {
											Ref ref_new = new Ref(reference.getPage(), j);
											Ref ref_old = b.search(((polygon) page.Tuple.get(j).Hash.get(strColName)).area);
											b.update_for_insert(((polygon) page.Tuple.get(j).Hash.get(strColName)).area, ref_old,ref_new);
										}
										serialize(p, t.files.get(i)); // serialize
										Page p2 = createpage(t, strTableName); // create
										p2.Tuple.add(0, Last_tuple); // insert
										p2.Tuple.remove(0); // remove the tuple
										Ref r = new Ref(0, 0);
										b.insert(((polygon) tuple.Hash.get(strColName)).area, r,strColName,strTableName);
										serialize(p2, t.files.get(i + 1));
									} else { // last page not full
										p.Tuple.add(Last_tuple);
										Collections.sort(p.Tuple);
										for (int j = 0; j < page.Tuple.size(); j++) {
											Ref ref_new = new Ref(reference.getPage(), j);
											Ref ref_old = b.search(((polygon) page.Tuple.get(j).Hash.get(strColName)).area);
											b.update_for_insert(((polygon) page.Tuple.get(j).Hash.get(strColName)).area, ref_old,ref_new);
										}
										serialize(p, t.files.get(i));
									}
								}
							}
						}

						else if (reference.getPage() == t.files.size() - 1) {
							if (page.Tuple.size() == page.Tuple.capacity()) {
								Tuple temp = page.Tuple.get(page.Tuple.size() - 1);
								page.Tuple.remove(page.Tuple.size() - 1);
								page.Tuple.add(page.Tuple.size() - 1, tuple);
								Last_tuple = temp;
								Collections.sort(page.Tuple); // sort the page
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(reference.getPage(), j);
									Ref ref_old = b.search(((polygon) page.Tuple.get(j).Hash.get(strColName)).area);
									b.update_for_insert(((polygon) page.Tuple.get(j).Hash.get(strColName)).area, ref_old,ref_new);
								}
								serialize(page,t.files.get(reference.getPage()));
								Page p2 = createpage(t, strTableName);
								p2.Tuple.add(0, Last_tuple);
								for (int j = 0; j < p2.Tuple.size(); j++) {
									Ref ref_new = new Ref(t.files.size() - 1, j);
									Ref ref_old = b.search(((polygon) p2.Tuple.get(j).Hash.get(strColName)).area);
									b.update_for_insert(((polygon) p2.Tuple.get(j).Hash.get(strColName)).area,ref_old, ref_new);
								}
								serialize(p2,t.files.get(reference.getPage() + 1));

							} else { // last page not full
								page.Tuple.add(tuple);
								Collections.sort(page.Tuple);
								for (int j = 0; j < page.Tuple.size(); j++) {
									Ref ref_new = new Ref(reference.getPage(), j);
									Ref ref_old = b.search(((polygon) page.Tuple.get(j).Hash.get(strColName)).area);
									b.update_for_insert(((polygon) page.Tuple.get(j).Hash.get(strColName)).area, ref_old,ref_new);
								}
								serialize(page,t.files.get(reference.getPage()));
							}
						}
					}
				} else {
					Page p2 = createpage(t, strTableName); // create new page
					p2.Tuple.add(0, tuple); // insert in it
					Ref ref = new Ref(0, 0);
					b.insert(((polygon) htblColNameValue.get(strColName)).area, ref,strColName,strTableName);
					serialize(p2, t.files.get(reference.getPage() + 1));
				
				}
				Page p = new Page();
				db.update_ref_double(key, p);
				serializet(t, strTableName + ".ser");
				serialize_RTree(strTableName, strColName, b);

			}
			

		} else {
			Table table;
			Vector<String> TableFiles = deserlizetf();
			int tableCounter;


			for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) {
				if (TableFiles.get(tableCounter).equals("data/" + strTableName + "" + ".ser")) {
					break;
				}
			}

			table = deserializet(TableFiles.get(tableCounter));
			Tuple tuple = new Tuple(table.ClusteringKey, htblColNameValue); // tuple to be inserted

			int pageIndex;
			if (table.files.size() == 0) { // CASE 1
				Page page = null;
				page = createpage(table, strTableName);
				page.Tuple.add(tuple);
				serialize(page, table.files.get(0)); // first page created
				serializet(table, TableFiles.get(tableCounter));
				serlizetf(TableFiles);
				return;
			} // END OF CASE 1

			else {

				for (pageIndex = 0; pageIndex < table.files.size(); pageIndex++) {

					Page A = deserialize(table.files.get(pageIndex));

					if (A.Tuple.size() == A.Tuple.capacity()) { // means en el page full
						Tuple last = A.Tuple.get(A.Tuple.size() - 1);

						if (last.compareTo(tuple) > 0) {
							if (pageIndex + 1 == table.files.size()) {
								Page pageNew;
								pageNew = createpage(table, strTableName);
								A.Tuple.remove(last);
								pageNew.Tuple.add(last);
								int rightIndex = Collections.binarySearch(A.Tuple, tuple);
								if (rightIndex < 0) {
									rightIndex = rightIndex + 1;
									rightIndex = Math.abs(rightIndex);
								}
								A.Tuple.add(rightIndex, tuple);
								serialize(A, table.files.get(pageIndex));
								serialize(pageNew, table.files.get(pageIndex + 1));
								serializet(table, TableFiles.get(tableCounter));
								serlizetf(TableFiles);
								return;

							} else { // not the last page in the table
								A.Tuple.remove(last);
								int rightIndex = Collections.binarySearch(A.Tuple, tuple);
								if (rightIndex < 0) {
									rightIndex = rightIndex + 1;
									rightIndex = Math.abs(rightIndex);
								}
								A.Tuple.add(rightIndex, tuple);

								Hashtable<String, Object> extraTuple = new Hashtable<String, Object>();
								extraTuple.putAll(last.Hash);

								serialize(A, table.files.get(pageIndex));
								serializet(table, TableFiles.get(tableCounter));
								serlizetf(TableFiles);

								insertIntoTable(strTableName, extraTuple);
								return;
							}

						} else {
							if (pageIndex + 1 == table.files.size()) {

								Page pageNew;
								pageNew = createpage(table, strTableName);
								pageNew.Tuple.add(tuple);

								Table t = deserializet(TableFiles.get(0));

								serialize(A, table.files.get(pageIndex));
								serialize(pageNew, table.files.get(pageIndex + 1));
								serializet(table, TableFiles.get(tableCounter));
								serlizetf(TableFiles);

								return;
							}
						}

					} else {

						int rightIndex = Collections.binarySearch(A.Tuple, tuple);
						if (rightIndex < 0) {
							rightIndex = rightIndex + 1;
							rightIndex = Math.abs(rightIndex);
						}

						A.Tuple.add(rightIndex, tuple);
						serialize(A, table.files.get(pageIndex));
						serializet(table, TableFiles.get(tableCounter));
						serlizetf(TableFiles);
					}
				} // for loop bta3t el pages
			} // else el kbera khales
//			} else {
//				System.out.println("Enter correct Hashtable");
//			}
		}// method
	}

	public void updateTable(String strTableName, String strClusteringKey,
			Hashtable<String, Object> htblColNameValue) throws DBAppException,
			IOException {
		Table ta = deserializet("data/" + strTableName + "" + ".ser");
		if (check_if_cluster_indexed(strTableName)) {
			String col_name=getcluster( strTableName,"data/metadata.class");
			String type = getcluster_type(strTableName);
			if (type.equals("java.lang.Integer")) {
				int key_value=Integer.parseInt(strClusteringKey);
				BPTree<Integer> b = deserialize_BTree(strTableName,col_name);
				Ref ref1 = b.search(key_value);
				Page page = deserialize(ta.files.get(ref1.getPage()));
				Vector<String> col_names = get_all_col(strTableName);
				Hashtable<String, Object> new_hash=page.Tuple.get(ref1.getIndexInPage()).Hash;
				update_trees_after_update(strTableName, ref1, htblColNameValue, page,new_hash);
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						page.Tuple.get(ref1.getIndexInPage()).Hash.replace(
								col_names.get(f), page.Tuple.get(ref1
										.getIndexInPage()).Hash.get(col_names
										.get(f)), htblColNameValue
										.get(col_names.get(f)));
					}
					Collections.sort(page.Tuple);
				}
				serialize(page, ta.files.get(ref1.getPage()));
				if (check_overflow_page_int(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "
							+ key_value + ".ser");
					for (int i = overflow_page.size() - 1; i > -1; i--) {
						Ref ref = overflow_page.get(i);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						new_hash=page1.Tuple.get(ref.getIndexInPage()).Hash;
						update_trees_after_update(strTableName, ref, htblColNameValue, page1,new_hash);
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								page1.Tuple.get(ref.getIndexInPage()).Hash
										.replace(col_names.get(f), page1.Tuple
												.get(ref.getIndexInPage()).Hash
												.get(col_names.get(f)),
												htblColNameValue.get(col_names
														.get(f)));
							}
							Collections.sort(page1.Tuple);
						}
						serialize(page1, ta.files.get(ref.getPage()));
					}
				}
			} else if (type.equals("java.lang.String")) {
				BPTree<String> b = deserialize_BTree(strTableName,col_name);
				String key_value = strClusteringKey;
				Ref ref1 = b.search(key_value);
				Page page = deserialize(ta.files.get(ref1.getPage()));
				Vector<String> col_names = get_all_col(strTableName);
				Hashtable<String, Object> new_hash=page.Tuple.get(ref1.getIndexInPage()).Hash;
				update_trees_after_update(strTableName, ref1, htblColNameValue, page,new_hash);
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						page.Tuple.get(ref1.getIndexInPage()).Hash.replace(
								col_names.get(f), page.Tuple.get(ref1
										.getIndexInPage()).Hash.get(col_names
										.get(f)), htblColNameValue
										.get(col_names.get(f)));
					}
					Collections.sort(page.Tuple);
				}
				serialize(page, ta.files.get(ref1.getPage()));
				if (check_overflow_page_String(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "
							+ key_value + ".ser");
					for (int i = overflow_page.size() - 1; i > -1; i--) {
						Ref ref = overflow_page.get(i);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								page1.Tuple.get(ref.getIndexInPage()).Hash
										.replace(col_names.get(f), page1.Tuple
												.get(ref.getIndexInPage()).Hash
												.get(col_names.get(f)),
												htblColNameValue.get(col_names
														.get(f)));
							}
							Collections.sort(page1.Tuple);
						}
						serialize(page1, ta.files.get(ref.getPage()));
					}
				}
			} else if (type.equals("java.lang.double")) {
				BPTree<Double> b = deserialize_BTree(strTableName,
						col_name);
				double key_value =Double.parseDouble(strClusteringKey);
				Ref ref1 = b.search(key_value);
				Page page = deserialize(ta.files.get(ref1.getPage()));
				Vector<String> col_names = get_all_col(strTableName);
				Hashtable<String, Object> new_hash=page.Tuple.get(ref1.getIndexInPage()).Hash;
				update_trees_after_update(strTableName, ref1, htblColNameValue, page,new_hash);
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						page.Tuple.get(ref1.getIndexInPage()).Hash.replace(
								col_names.get(f), page.Tuple.get(ref1
										.getIndexInPage()).Hash.get(col_names
										.get(f)), htblColNameValue
										.get(col_names.get(f)));
					}
					Collections.sort(page.Tuple);
				}
				serialize(page, ta.files.get(ref1.getPage()));
				if (check_overflow_page_double(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "
							+ key_value + ".ser");
					for (int i = overflow_page.size() - 1; i > -1; i--) {
						Ref ref = overflow_page.get(i);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								page1.Tuple.get(ref.getIndexInPage()).Hash
										.replace(col_names.get(f), page1.Tuple
												.get(ref.getIndexInPage()).Hash
												.get(col_names.get(f)),
												htblColNameValue.get(col_names
														.get(f)));
							}
							Collections.sort(page1.Tuple);
						}
						serialize(page1, ta.files.get(ref.getPage()));
					}
				}
			} else if (type.equals("java.lang.Date")) {
				BPTree<Date> b = deserialize_BTree(strTableName,
						col_name);
				Date key_value =null;
				Ref ref1 = b.search(key_value);
				Page page = deserialize(ta.files.get(ref1.getPage()));
				Vector<String> col_names = get_all_col(strTableName);
				Hashtable<String, Object> new_hash=page.Tuple.get(ref1.getIndexInPage()).Hash;
				update_trees_after_update(strTableName, ref1, htblColNameValue, page,new_hash);
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						page.Tuple.get(ref1.getIndexInPage()).Hash.replace(
								col_names.get(f), page.Tuple.get(ref1
										.getIndexInPage()).Hash.get(col_names
										.get(f)), htblColNameValue
										.get(col_names.get(f)));
					}
					Collections.sort(page.Tuple);
				}
				serialize(page, ta.files.get(ref1.getPage()));
				if (check_overflow_page_Date(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "
							+ key_value + ".ser");
					for (int i = overflow_page.size() - 1; i > -1; i--) {
						Ref ref = overflow_page.get(i);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								page1.Tuple.get(ref.getIndexInPage()).Hash
										.replace(col_names.get(f), page1.Tuple
												.get(ref.getIndexInPage()).Hash
												.get(col_names.get(f)),
												htblColNameValue.get(col_names
														.get(f)));
							}
							Collections.sort(page1.Tuple);
						}
						serialize(page1, ta.files.get(ref.getPage()));
					}
				}
			} else if (type.equals("java.lang.Boolean")) {
				BPTree<Boolean> b = deserialize_BTree(strTableName,
						col_name);
				Boolean key_value =Boolean.parseBoolean(strClusteringKey);
				Ref ref1 = b.search(key_value);
				Page page = deserialize(ta.files.get(ref1.getPage()));
				Vector<String> col_names = get_all_col(strTableName);
				Hashtable<String, Object> new_hash=page.Tuple.get(ref1.getIndexInPage()).Hash;
				update_trees_after_update(strTableName, ref1, htblColNameValue, page,new_hash);
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						page.Tuple.get(ref1.getIndexInPage()).Hash.replace(
								col_names.get(f), page.Tuple.get(ref1
										.getIndexInPage()).Hash.get(col_names
										.get(f)), htblColNameValue
										.get(col_names.get(f)));
					}
					Collections.sort(page.Tuple);
				}
				serialize(page, ta.files.get(ref1.getPage()));
				if (check_overflow_page_boolean(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "
							+ key_value + ".ser");
					for (int i = overflow_page.size() - 1; i > -1; i--) {
						Ref ref = overflow_page.get(i);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								page1.Tuple.get(ref.getIndexInPage()).Hash
										.replace(col_names.get(f), page1.Tuple
												.get(ref.getIndexInPage()).Hash
												.get(col_names.get(f)),
												htblColNameValue.get(col_names
														.get(f)));
							}
							Collections.sort(page1.Tuple);
						}
						serialize(page1, ta.files.get(ref.getPage()));
					}
				}
			}
			
			else if (type.equals("java.awt.Polygon")) {
				strClusteringKey=remove_brackets(strClusteringKey);
                int x []=get_x_axis(strClusteringKey);
                int y []=get_y_axis(strClusteringKey);	
                polygon polygon=new polygon(x,y,x.length);
                
                String key_col=getcluster(strTableName);
				RTree<Double> b = deserialize_RTree(strTableName,col_name);	
				double key_value =polygon.area;
				Ref ref1 = b.search(key_value);
				Page page = deserialize(ta.files.get(ref1.getPage()));
				Vector<String> col_names = get_all_col(strTableName);
				Hashtable<String, Object> new_hash=page.Tuple.get(ref1.getIndexInPage()).Hash;
				update_trees_after_update(strTableName, ref1, htblColNameValue, page,new_hash);
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						page.Tuple.get(ref1.getIndexInPage()).Hash.replace(col_names.get(f), 
								page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)), 
										htblColNameValue.get(col_names.get(f)));
					}
					Collections.sort(page.Tuple);
				}
				serialize(page, ta.files.get(ref1.getPage()));
				if (check_overflow_page_double(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
					for (int i = overflow_page.size() - 1; i > -1; i--) {
						Ref ref = overflow_page.get(i);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
					
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								page1.Tuple.get(ref.getIndexInPage()).Hash
										.replace(col_names.get(f), page1.Tuple
												.get(ref.getIndexInPage()).Hash
												.get(col_names.get(f)),
												htblColNameValue.get(col_names
														.get(f)));
							}
							Collections.sort(page1.Tuple);
						}
						serialize(page1, ta.files.get(ref.getPage()));
					}
				}
				}
		}
		else{
			Table table;
			Vector<String> TableFiles = deserlizetf();
			int tableCounter;

			for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) {
				if (TableFiles.get(tableCounter).equals("data/" + strTableName + "" + ".ser")) {
					break;
				}
			}

			table = deserializet(TableFiles.get(tableCounter));
			if (table.files.size() == 0) {
				System.out.println("Empty table");
				return;
			}
			
			Page temp = deserialize(table.files.get(0));
			String clusterType = temp.Tuple.get(0).Hash.get(table.ClusteringKey).getClass().toString() + "";
			serialize(temp, table.files.get(0));
			serializet(table, TableFiles.get(tableCounter));
			serlizetf(TableFiles);

			Object ourCluster;
			if (clusterType.contains("java.lang.Integer")) {
				ourCluster = Integer.parseInt(strClusteringKey);
			} else if (clusterType.contains("java.lang.String")) {
				ourCluster = strClusteringKey.toString();
			} else if (clusterType.contains("java.lang.Double")) {
				ourCluster = Double.parseDouble(strClusteringKey);
			} else if (clusterType.contains(" java.util.Date")) {
				ourCluster = Date.parse(strClusteringKey);

			} else if (clusterType.contains(" java.util.Boolean")) {
				ourCluster = Boolean.parseBoolean(strClusteringKey);

			} else {
				strClusteringKey=remove_brackets(strClusteringKey);
                int x []=get_x_axis(strClusteringKey);
                int y []=get_y_axis(strClusteringKey);	
                polygon polygon=new polygon(x,y,x.length);
                ourCluster=polygon.area;
			}

			htblColNameValue.put(table.ClusteringKey, ourCluster);
			Tuple toBeUpdated = new Tuple(table.ClusteringKey, htblColNameValue);

			int pageIndex;
			Vector<String> col_names = get_all_col(strTableName);
			for (pageIndex = 0; pageIndex < table.files.size(); pageIndex++) {
				Page A = deserialize(table.files.get(pageIndex));
				Tuple last = A.Tuple.get(A.Tuple.size() - 1);
				if (last.compareTo(toBeUpdated) < 0) {
					serialize(A, table.files.get(pageIndex));
				} else {
					Tuple middleTuple = A.Tuple.get((A.Tuple.size() - 1) / 2);
					int middleTupleIndex = (A.Tuple.size() - 1) / 2;

					if (middleTuple.compareTo(toBeUpdated) == 0) {

						
						for (int i = middleTupleIndex ; i >= 0; i--) { // loop above the middle
							if ((A.Tuple.get(i).compareTo(toBeUpdated) == 0)) { // if_equal_to_objValue
								
			
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null) {
										A.Tuple.get(i).Hash.replace(col_names.get(f), A.Tuple.get(i).Hash.get(col_names.get(f)), htblColNameValue.get(col_names.get(f)));}}
									
							//	A.Tuple.get(i).Hash = htblColNameValue;

							} else if ((A.Tuple.get(i).compareTo(toBeUpdated) < 0)) {
								break; // no more equals
							}
						} // finished what above the middle

						A.Tuple.get(middleTupleIndex).Hash = htblColNameValue;

						for (int i = middleTupleIndex + 1 ; i < A.Tuple.size(); i++) { // loop_under_the_middle
							if ((A.Tuple.get(i).compareTo(toBeUpdated) == 0)) { // if_equal_to_objValue

								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null) {
										A.Tuple.get(i).Hash.replace(col_names.get(f), A.Tuple.get(i).Hash.get(col_names.get(f)), htblColNameValue.get(col_names.get(f)));}}
									
								
								//A.Tuple.get(i).Hash = htblColNameValue;

							} else if ((A.Tuple.get(i).compareTo(toBeUpdated) > 0)) {
								break; // no more equals
							}
						} // finished what under the middle
						
					} // if they were equal..
					else if (middleTuple.compareTo(toBeUpdated) > 0) { // middle akbar mn el badawar 3aleh
						for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
							if ((A.Tuple.get(i).compareTo(toBeUpdated) == 0)) { // if_equal_to_objValue
 
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null) {
										A.Tuple.get(i).Hash.replace(col_names.get(f), A.Tuple.get(i).Hash.get(col_names.get(f)), htblColNameValue.get(col_names.get(f)));}}
									
								
								
								//A.Tuple.get(i).Hash = htblColNameValue;

							}
						} // finished what above the middle

					} else {// middle asghar mn el badawar 3aleh
						for (int i = middleTupleIndex + 1; i < A.Tuple.size(); i++) { // loop_under_the_middle
							if ((A.Tuple.get(i).compareTo(toBeUpdated) == 0)) { // if_equal_to_objValue

								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null) {
										A.Tuple.get(i).Hash.replace(col_names.get(f), A.Tuple.get(i).Hash.get(col_names.get(f)), htblColNameValue.get(col_names.get(f)));}}
									
								
								//A.Tuple.get(i).Hash = htblColNameValue;

							}
						} // finished what under the middle
					}
				} // Else el kbera
				serialize(A, table.files.get(pageIndex));
			} // For over Pages
			serializet(table, TableFiles.get(tableCounter));
			serlizetf(TableFiles);
//			} else {
//				System.out.println("Enter correct Hashtable");
//			}

		
		// Method
		}

	}

	public void deleteFromTable(String strTableName,Hashtable<String, Object> htblColNameValue) throws DBAppException,
			IOException {
		String cluster_key = getcluster(strTableName, "data/metadata.class");
		Table ta = deserializet("data/" + strTableName + "" + ".ser");
		Tuple tup = new Tuple(getcluster(strTableName, "data/metadata.class"),htblColNameValue);
		boolean flag=true;
		Vector<String> col_names = get_all_col(strTableName);

		if (check_if_cluster_indexed(strTableName)) {
			if (htblColNameValue.get(cluster_key) != null) {
				String type = getcluster_type(strTableName);
				if (type.equals("java.lang.Integer")) {
					boolean btree=false;
					BPTree<Integer> b = deserialize_BTree(strTableName,cluster_key);
					int key_value = (int) htblColNameValue.get(cluster_key);
					Ref ref1 = b.search(key_value);
					if (ref1 == null) {
						System.out.println("Key is not in the table");
						return;
					}
					Page page = deserialize(ta.files.get(ref1.getPage()));
					for (int f = 0; f < col_names.size(); f++) {
						if (htblColNameValue.get(col_names.get(f)) != null) {
							
							if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
								polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
								polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
								if(po1.mycompareto(po2)!=0){
									flag = false;
								}
								}
							
							else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
								flag = false;
							}}}
							if (flag == true) {
								page.Tuple.remove(ref1.getIndexInPage());
								update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
								btree=true;
							}
						
					if (page.Tuple.size() != 0&&flag==true) {
						update_page(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
						serialize(page, ta.files.get(ref1.getPage()));
					} else if(flag==true) {
						update_page_2(ta, ref1.getPage(), b, cluster_key);
						File f = new File(ta.files.get(ref1.getPage()));
						f.delete();
						ta.files.remove(ref1.getPage());
						serializet(ta, "data/" + strTableName + "" + ".ser");
					}
					if (check_overflow_page_int(key_value)) {
						Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
						for (int q = overflow_page.size() - 1; q > -1; q--) {
							Ref ref = overflow_page.get(q);
							Page page1 = deserialize(ta.files.get(ref.getPage()));
							flag=true;
							for (int f = 0; f < col_names.size(); f++) {
								if (htblColNameValue.get(col_names.get(f)) != null) {
									if(ref.getIndexInPage()>page1.Tuple.size()-1){
										flag=false;
										break;
									}
									
									if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
										polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
										polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
										if(po1.mycompareto(po2)!=0){
											flag = false;
										}
										}
									
									else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
										flag = false;
									}}}
									if (flag == true) {
										page1.Tuple.remove(ref.getIndexInPage());
										update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
										btree=true;
									}
							if (page1.Tuple.size() != 0&&flag==true) {
								update_page(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
								serialize(page1, ta.files.get(ref.getPage()));
							} else if(flag==true){
								update_page_2(ta, ref.getPage(), b, cluster_key);
								File f = new File(ta.files.get(ref.getPage()));
								f.delete();
								ta.files.remove(ref.getPage());
								serializet(ta, "data/" + strTableName + ""
										+ ".ser");
							}

						}

					}
					if(btree==true){
					b.delete(key_value);
					serialize_BTree(strTableName, cluster_key, b);}
				}

			else if (type.equals("java.lang.String")) {
				boolean btree=false;
				BPTree<String> b = deserialize_BTree(strTableName,cluster_key);
				String key_value = (String) htblColNameValue.get(cluster_key);
				Ref ref1 = b.search(key_value);
				if (ref1 == null) {
					System.out.println("Key is not in the table");
					return;
				}
				Page page = deserialize(ta.files.get(ref1.getPage()));
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						
						if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
							polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
							polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
							if(po1.mycompareto(po2)!=0){
								flag = false;
							}
							}
						
						else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
							flag = false;
						}}}
						if (flag == true) {
							page.Tuple.remove(ref1.getIndexInPage());
							update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
							btree=true;
						}
					
				if (page.Tuple.size() != 0&&flag==true) {
					update_page_String(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
					serialize(page, ta.files.get(ref1.getPage()));
				} else if(flag==true) {
					update_page_2_String(ta, ref1.getPage(), b, cluster_key);
					File f = new File(ta.files.get(ref1.getPage()));
					f.delete();
					ta.files.remove(ref1.getPage());
					serializet(ta, "data/" + strTableName + "" + ".ser");
				}
				if (check_overflow_page_String(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
					for (int q = overflow_page.size() - 1; q > -1; q--) {
						Ref ref = overflow_page.get(q);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						flag=true;
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
							 if(ref.getIndexInPage()>page1.Tuple.size()-1){
									flag=false;
									break;
								}
							 
							 if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
							 
							 else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page1.Tuple.remove(ref.getIndexInPage());
									update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
									btree=true;
								}
						if (page1.Tuple.size() != 0&&flag==true) {
							update_page_String(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
							serialize(page1, ta.files.get(ref.getPage()));
						} else if(flag==true){
							update_page_2_String(ta, ref.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref.getPage()));
							f.delete();
							ta.files.remove(ref.getPage());
							serializet(ta, "data/" + strTableName + ""
									+ ".ser");
						}

					}

				}
				if(btree==true){
				b.delete(key_value);
				serialize_BTree(strTableName, cluster_key, b);}

				
			} else if (type.equals("java.lang.double")) {
				boolean btree=false;
				BPTree<Double> b = deserialize_BTree(strTableName,cluster_key);
				Double key_value = (Double) htblColNameValue.get(cluster_key);
				Ref ref1 = b.search(key_value);
				if (ref1 == null) {
					System.out.println("Key is not in the table");
					return;
				}
				Page page = deserialize(ta.files.get(ref1.getPage()));
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						
						if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
							polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
							polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
							if(po1.mycompareto(po2)!=0){
								flag = false;
							}
							}
						
					else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
							flag = false;
						}}}
						if (flag == true) {
							page.Tuple.remove(ref1.getIndexInPage());
							update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
							btree=true;
						}
					
				if (page.Tuple.size() != 0&&flag==true) {
					update_page_double(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
					serialize(page, ta.files.get(ref1.getPage()));
				} else if(flag==true) {
					update_page_2_double(ta, ref1.getPage(), b, cluster_key);
					File f = new File(ta.files.get(ref1.getPage()));
					f.delete();
					ta.files.remove(ref1.getPage());
					serializet(ta, "data/" + strTableName + "" + ".ser");
				}
				if (check_overflow_page_double(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
					for (int q = overflow_page.size() - 1; q > -1; q--) {
						Ref ref = overflow_page.get(q);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						flag=true;
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {			
								if(ref.getIndexInPage()>page1.Tuple.size()-1){
									flag=false;
									break;
								}
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page1.Tuple.remove(ref.getIndexInPage());
									update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
									btree=true;
								}
						if (page1.Tuple.size() != 0&&flag==true) {
							update_page_double(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
							serialize(page1, ta.files.get(ref.getPage()));
						} else if(flag==true){
							update_page_2_double(ta, ref.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref.getPage()));
							f.delete();
							ta.files.remove(ref.getPage());
							serializet(ta, "data/" + strTableName + ""
									+ ".ser");
						}

					}

				}
				if(btree==true){
				b.delete(key_value);
				serialize_BTree(strTableName, cluster_key, b);}

			}

			else if (type.equals("java.lang.Boolean")) {
				boolean btree=false;
				BPTree<Boolean> b = deserialize_BTree(strTableName,cluster_key);
			boolean key_value = (boolean) htblColNameValue.get(cluster_key);
				Ref ref1 = b.search(key_value);
				if (ref1 == null) {
					System.out.println("Key is not in the table");
					return;
				}
				Page page = deserialize(ta.files.get(ref1.getPage()));
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						
						if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
							polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
							polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
							if(po1.mycompareto(po2)!=0){
								flag = false;
							}
							}
						
						else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
							flag = false;
						}}}
						if (flag == true) {
							page.Tuple.remove(ref1.getIndexInPage());
							update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
							btree=true;
						}
					
				if (page.Tuple.size() != 0&&flag==true) {
					update_page_boolean(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
					serialize(page, ta.files.get(ref1.getPage()));
				} else if(flag==true) {
					update_page_2_boolean(ta, ref1.getPage(), b, cluster_key);
					File f = new File(ta.files.get(ref1.getPage()));
					f.delete();
					ta.files.remove(ref1.getPage());
					serializet(ta, "data/" + strTableName + "" + ".ser");
				}
				if (check_overflow_page_boolean(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
					for (int q = overflow_page.size() - 1; q > -1; q--) {
						Ref ref = overflow_page.get(q);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						flag=true;
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								if(ref.getIndexInPage()>page1.Tuple.size()-1){
									flag=false;
									break;
								}
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page1.Tuple.remove(ref.getIndexInPage());
									update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
									btree=true;
								}
						if (page1.Tuple.size() != 0&&flag==true) {
							update_page_boolean(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
							serialize(page1, ta.files.get(ref.getPage()));
						} else if(flag==true){
							update_page_2_boolean(ta, ref.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref.getPage()));
							f.delete();
							ta.files.remove(ref.getPage());
							serializet(ta, "data/" + strTableName + ""
									+ ".ser");
						}

					}

				}
				if(btree==true){
				b.delete(key_value);
				serialize_BTree(strTableName, cluster_key, b);}

			}

			else if (type.equals("java.util.Date")) {
				boolean btree=false;
				BPTree<Date> b = deserialize_BTree(strTableName,cluster_key);
				Date key_value = (Date) htblColNameValue.get(cluster_key);
				Ref ref1 = b.search(key_value);
				if (ref1 == null) {
					System.out.println("Key is not in the table");
					return;
				}
				Page page = deserialize(ta.files.get(ref1.getPage()));
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						
						if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
							polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
							polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
							if(po1.mycompareto(po2)!=0){
								flag = false;
							}
							}
						
						else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
							flag = false;
						}}}
						if (flag == true) {
							page.Tuple.remove(ref1.getIndexInPage());
							update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
							btree=true;
						}
					
				if (page.Tuple.size() != 0&&flag==true) {
					update_page_Date(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
					serialize(page, ta.files.get(ref1.getPage()));
				} else if(flag==true) {
					update_page_2_Date(ta, ref1.getPage(), b, cluster_key);
					File f = new File(ta.files.get(ref1.getPage()));
					f.delete();
					ta.files.remove(ref1.getPage());
					serializet(ta, "data/" + strTableName + "" + ".ser");
				}
				if (check_overflow_page_Date(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
					for (int q = overflow_page.size() - 1; q > -1; q--) {
						Ref ref = overflow_page.get(q);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						flag=true;
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								if(ref.getIndexInPage()>page1.Tuple.size()-1){
									flag=false;
									break;
								}
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page1.Tuple.remove(ref.getIndexInPage());
									update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
									btree=true;
								}
						if (page1.Tuple.size() != 0&&flag==true) {
							update_page_Date(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
							serialize(page1, ta.files.get(ref.getPage()));
						} else if(flag==true){
							update_page_2_Date(ta, ref.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref.getPage()));
							f.delete();
							ta.files.remove(ref.getPage());
							serializet(ta, "data/" + strTableName + ""
									+ ".ser");
						}

					}

				}
				if(btree==true){
				b.delete(key_value);
				serialize_BTree(strTableName, cluster_key, b);}
			} 
			
			else if (type.equals("java.awt.Polygon")) {
				boolean btree=false;
				RTree<Double> b = deserialize_RTree(strTableName,cluster_key);
				polygon a=(polygon) htblColNameValue.get(cluster_key);
				Double key_value =a.area;
				Ref ref1 = b.search(key_value);
				if (ref1 == null) {
					System.out.println("Key is not in the table");
					return;
				}
				Page page = deserialize(ta.files.get(ref1.getPage()));
				for (int f = 0; f < col_names.size(); f++) {
					if (htblColNameValue.get(col_names.get(f)) != null) {
						if(cluster_key.equals(col_names.get(f))){
						polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
						polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
						if(po1.mycompareto(po2)!=0){
							flag = false;
						}
						
						}
						else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
							flag = false;
						}}}
						if (flag == true) {
							page.Tuple.remove(ref1.getIndexInPage());
							update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
							btree=true;
						}
					
				if (page.Tuple.size() != 0&&flag==true) {
					update_page_double(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
					serialize(page, ta.files.get(ref1.getPage()));
				} else if(flag==true) {
					update_page_2_double(ta, ref1.getPage(), b, cluster_key);
					File f = new File(ta.files.get(ref1.getPage()));
					f.delete();
					ta.files.remove(ref1.getPage());
					serializet(ta, "data/" + strTableName + "" + ".ser");
				}
				if (check_overflow_page_double(key_value)) {
					Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
					for (int q = overflow_page.size() - 1; q > -1; q--) {
						Ref ref = overflow_page.get(q);
						Page page1 = deserialize(ta.files.get(ref.getPage()));
						flag=true;
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								if(ref.getIndexInPage()>page1.Tuple.size()-1){
									flag=false;
									break;
								}
								if(cluster_key.equals(col_names.get(f))){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page1.Tuple.remove(ref.getIndexInPage());
									update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
									btree=true;
								}
						if (page1.Tuple.size() != 0&&flag==true) {
							update_page_double(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
							serialize(page1, ta.files.get(ref.getPage()));
						} else if(flag==true){
							update_page_2_double(ta, ref.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref.getPage()));
							f.delete();
							ta.files.remove(ref.getPage());
							serializet(ta, "data/" + strTableName + ""
									+ ".ser");
						}

					}

				}
				if(btree==true){
				b.delete(key_value);
				serialize_RTree(strTableName, cluster_key, b);}

			}		
				return;
		}
			
		}
		 if(check_if_any_col_indexed_other_cluster(strTableName).size()>0){
			Vector <String> indcies =check_if_any_col_indexed_other_cluster(strTableName);
			for(int i=0;i<indcies.size();i++){
				if(htblColNameValue.get(indcies.get(i))!=null){
					String type=getcoltypeforindex(strTableName,indcies.get(i));
					cluster_key=indcies.get(i);
					if (type.equals("java.lang.Integer")) {
						boolean btree=false;
						BPTree<Integer> b = deserialize_BTree(strTableName,cluster_key);
						Integer key_value = (Integer) htblColNameValue.get(cluster_key);
						Ref ref1 = b.search(key_value);
						if (ref1 == null) {
							System.out.println("Key is not in the table");
							return;
						}
						
						if (check_overflow_page_int(key_value)) {
							Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
							for (int q = overflow_page.size() - 1; q > -1; q--) {
								 b = deserialize_BTree(strTableName,cluster_key);
								 Page page1=new Page();
								 flag=true;
								Ref ref = overflow_page.get(q);
								 page1 = deserialize(ta.files.get(ref.getPage()));
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null&&flag==true) {
										
										if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
											polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
											polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
											if(po1.mycompareto(po2)!=0){
												flag = false;
											}
											}
										
										if(page1.Tuple.size()<ref.getIndexInPage()){
											flag=false;
											break;
										}
										
										else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
											flag = false;
										}}}
										if (flag == true) {
											page1.Tuple.remove(ref.getIndexInPage());
											update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
											btree=true;
											
										}
								if (page1.Tuple.size() != 0&&flag==true) {
									update_page(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
									serialize(page1, ta.files.get(ref.getPage()));
								} else if(flag==true){
									update_page_2(ta, ref.getPage(), b, cluster_key);
									File f = new File(ta.files.get(ref.getPage()));
									f.delete();
									ta.files.remove(ref.getPage());
									serializet(ta, "data/" + strTableName + ""
											+ ".ser");
								}

							}

						}
						Page page = deserialize(ta.files.get(ref1.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page.Tuple.remove(ref1.getIndexInPage());
									update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
									btree=true;
								}
							
						if (page.Tuple.size() != 0&&flag==true) {
							update_page(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
							serialize(page, ta.files.get(ref1.getPage()));
						} else if(flag==true) {
							update_page_2(ta, ref1.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref1.getPage()));
							f.delete();
							ta.files.remove(ref1.getPage());
							serializet(ta, "data/" + strTableName + "" + ".ser");
						}
						
						if(btree==true){
						b.delete(key_value);
						serialize_BTree(strTableName, cluster_key, b);}
					
						
					}
					
					else if (type.equals("java.lang.String")) {
						boolean btree=false;
						BPTree<String> b = deserialize_BTree(strTableName,cluster_key);
						String key_value = (String) htblColNameValue.get(cluster_key);
						Ref ref1 = b.search(key_value);
						if (ref1 == null) {
							System.out.println("Key is not in the table");
							return;
						}
						
						if (check_overflow_page_String(key_value)) {
							Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
							for (int q = overflow_page.size() - 1; q > -1; q--) {
								 b = deserialize_BTree(strTableName,cluster_key);
								 Page page1=new Page();
								 flag=true;
								Ref ref = overflow_page.get(q);
								 page1 = deserialize(ta.files.get(ref.getPage()));
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null&&flag==true) {
										if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
											polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
											polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
											if(po1.mycompareto(po2)!=0){
												flag = false;
											}
											}
										if(page1.Tuple.size()<ref.getIndexInPage()){
											flag=false;
											break;
										}
										else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
											flag = false;
										}}}
										if (flag == true) {
											page1.Tuple.remove(ref.getIndexInPage());
											update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
											btree=true;
											
										}
								if (page1.Tuple.size() != 0&&flag==true) {
									update_page_String(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
									serialize(page1, ta.files.get(ref.getPage()));
								} else if(flag==true){
									update_page_2_String(ta, ref.getPage(), b, cluster_key);
									File f = new File(ta.files.get(ref.getPage()));
									f.delete();
									ta.files.remove(ref.getPage());
									serializet(ta, "data/" + strTableName + ""
											+ ".ser");
								}

							}

						}
						Page page = deserialize(ta.files.get(ref1.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page.Tuple.remove(ref1.getIndexInPage());
									update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
									btree=true;
								}
							
						if (page.Tuple.size() != 0&&flag==true) {
							update_page_String(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
							serialize(page, ta.files.get(ref1.getPage()));
						} else if(flag==true) {
							update_page_2_String(ta, ref1.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref1.getPage()));
							f.delete();
							ta.files.remove(ref1.getPage());
							serializet(ta, "data/" + strTableName + "" + ".ser");
						}
						
						if(btree==true){
						b.delete(key_value);
						serialize_BTree(strTableName, cluster_key, b);}
					
						
						
					} else if (type.equals("java.lang.double")) {
						boolean btree=false;
						BPTree<Double> b = deserialize_BTree(strTableName,cluster_key);
						Double key_value = (Double) htblColNameValue.get(cluster_key);
						Ref ref1 = b.search(key_value);
						if (ref1 == null) {
							System.out.println("Key is not in the table");
							return;
						}
						
						if (check_overflow_page_double(key_value)) {
							Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
							for (int q = overflow_page.size() - 1; q > -1; q--) {
								 b = deserialize_BTree(strTableName,cluster_key);
								 Page page1=new Page();
								 flag=true;
								Ref ref = overflow_page.get(q);
								 page1 = deserialize(ta.files.get(ref.getPage()));
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null&&flag==true) {
										
										if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
											polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
											polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
											if(po1.mycompareto(po2)!=0){
												flag = false;
											}
											}
										if(page1.Tuple.size()<ref.getIndexInPage()){
											flag=false;
											break;
										}
										
										else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
											flag = false;
										}}}
										if (flag == true) {
											page1.Tuple.remove(ref.getIndexInPage());
											update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
											btree=true;
											
										}
								if (page1.Tuple.size() != 0&&flag==true) {
									update_page_double(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
									serialize(page1, ta.files.get(ref.getPage()));
								} else if(flag==true){
									update_page_2_double(ta, ref.getPage(), b, cluster_key);
									File f = new File(ta.files.get(ref.getPage()));
									f.delete();
									ta.files.remove(ref.getPage());
									serializet(ta, "data/" + strTableName + ""
											+ ".ser");
								}

							}

						}
						Page page = deserialize(ta.files.get(ref1.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page.Tuple.remove(ref1.getIndexInPage());
									update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
									btree=true;
								}
							
						if (page.Tuple.size() != 0&&flag==true) {
							update_page_double(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
							serialize(page, ta.files.get(ref1.getPage()));
						} else if(flag==true) {
							update_page_2_double(ta, ref1.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref1.getPage()));
							f.delete();
							ta.files.remove(ref1.getPage());
							serializet(ta, "data/" + strTableName + "" + ".ser");
						}
						
						if(btree==true){
						b.delete(key_value);
						serialize_BTree(strTableName, cluster_key, b);}
					
						

					}

					else if (type.equals("java.lang.Boolean")) {
						boolean btree=false;
						BPTree<Boolean> b = deserialize_BTree(strTableName,cluster_key);
						Boolean key_value = (Boolean) htblColNameValue.get(cluster_key);
						Ref ref1 = b.search(key_value);
						if (ref1 == null) {
							System.out.println("Key is not in the table");
							return;
						}
						
						if (check_overflow_page_boolean(key_value)) {
							Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
							for (int q = overflow_page.size() - 1; q > -1; q--) {
								 b = deserialize_BTree(strTableName,cluster_key);
								 Page page1=new Page();
								 flag=true;
								Ref ref = overflow_page.get(q);
								 page1 = deserialize(ta.files.get(ref.getPage()));
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null&&flag==true) {
										
										if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
											polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
											polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
											if(po1.mycompareto(po2)!=0){
												flag = false;
											}
											}
										
										if(page1.Tuple.size()<ref.getIndexInPage()){
											flag=false;
											break;
										}
										
										else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
											flag = false;
										}}}
										if (flag == true) {
											page1.Tuple.remove(ref.getIndexInPage());
											update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
											btree=true;
											
										}
								if (page1.Tuple.size() != 0&&flag==true) {
									update_page_boolean(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
									serialize(page1, ta.files.get(ref.getPage()));
								} else if(flag==true){
									update_page_2_boolean(ta, ref.getPage(), b, cluster_key);
									File f = new File(ta.files.get(ref.getPage()));
									f.delete();
									ta.files.remove(ref.getPage());
									serializet(ta, "data/" + strTableName + ""
											+ ".ser");
								}

							}

						}
						Page page = deserialize(ta.files.get(ref1.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page.Tuple.remove(ref1.getIndexInPage());
									update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
									btree=true;
								}
							
						if (page.Tuple.size() != 0&&flag==true) {
							update_page_boolean(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
							serialize(page, ta.files.get(ref1.getPage()));
						} else if(flag==true) {
							update_page_2_boolean(ta, ref1.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref1.getPage()));
							f.delete();
							ta.files.remove(ref1.getPage());
							serializet(ta, "data/" + strTableName + "" + ".ser");
						}
						
						if(btree==true){
						b.delete(key_value);
						serialize_BTree(strTableName, cluster_key, b);}
					
						

					}

					else if (type.equals("java.util.Date")) {
						boolean btree=false;
						BPTree<Date> b = deserialize_BTree(strTableName,cluster_key);
						Date key_value = (Date) htblColNameValue.get(cluster_key);
						Ref ref1 = b.search(key_value);
						if (ref1 == null) {
							System.out.println("Key is not in the table");
							return;
						}
						
						if (check_overflow_page_Date(key_value)) {
							Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
							for (int q = overflow_page.size() - 1; q > -1; q--) {
								 b = deserialize_BTree(strTableName,cluster_key);
								 Page page1=new Page();
								 flag=true;
								Ref ref = overflow_page.get(q);
								 page1 = deserialize(ta.files.get(ref.getPage()));
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null&&flag==true) {
										
										if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
											polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
											polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
											if(po1.mycompareto(po2)!=0){
												flag = false;
											}
											}
										
										if(page1.Tuple.size()<ref.getIndexInPage()){
											flag=false;
											break;
										}
										
										else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
											flag = false;
										}}}
										if (flag == true) {
											page1.Tuple.remove(ref.getIndexInPage());
											update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
											btree=true;
											
										}
								if (page1.Tuple.size() != 0&&flag==true) {
									update_page_Date(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
									serialize(page1, ta.files.get(ref.getPage()));
								} else if(flag==true){
									update_page_2_Date(ta, ref.getPage(), b, cluster_key);
									File f = new File(ta.files.get(ref.getPage()));
									f.delete();
									ta.files.remove(ref.getPage());
									serializet(ta, "data/" + strTableName + ""
											+ ".ser");
								}

							}

						}
						Page page = deserialize(ta.files.get(ref1.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page.Tuple.remove(ref1.getIndexInPage());
									update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
									btree=true;
								}
							
						if (page.Tuple.size() != 0&&flag==true) {
							update_page_Date(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
							serialize(page, ta.files.get(ref1.getPage()));
						} else if(flag==true) {
							update_page_2_Date(ta, ref1.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref1.getPage()));
							f.delete();
							ta.files.remove(ref1.getPage());
							serializet(ta, "data/" + strTableName + "" + ".ser");
						}
						
						if(btree==true){
						b.delete(key_value);
						serialize_BTree(strTableName, cluster_key, b);}
					
						
					}
					else if (type.equals("java.awt.Polygon")) {
						boolean btree=false;
						RTree<Double> b = deserialize_RTree(strTableName,cluster_key);
						polygon po=(polygon) htblColNameValue.get(cluster_key);
						Double key_value = po.area;
						Ref ref1 = b.search(key_value);
						if (ref1 == null) {
							System.out.println("Key is not in the table");
							return;
						}
						
						if (check_overflow_page_double(key_value)) {
							Vector<Ref> overflow_page = deserialize_classTree("data/Overflow of "+ key_value + ".ser");
							for (int q = overflow_page.size() - 1; q > -1; q--) {
								 b = deserialize_RTree(strTableName,cluster_key);
								 Page page1=new Page();
								 flag=true;
								Ref ref = overflow_page.get(q);
								 page1 = deserialize(ta.files.get(ref.getPage()));
								for (int f = 0; f < col_names.size(); f++) {
									if (htblColNameValue.get(col_names.get(f)) != null&&flag==true) {
										
										if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
											polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
											polygon po2=(polygon)page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f));
											if(po1.mycompareto(po2)!=0){
												flag = false;
											}
											}
										if(page1.Tuple.size()<ref.getIndexInPage()){
											flag=false;
											break;
										}
										else if (!htblColNameValue.get(col_names.get(f)).equals(page1.Tuple.get(ref.getIndexInPage()).Hash.get(col_names.get(f)))) {
											flag = false;
										}}}
										if (flag == true) {
											page1.Tuple.remove(ref.getIndexInPage());
											update_trees_after_delete(strTableName, ref, htblColNameValue, page1);
											btree=true;
											
										}
								if (page1.Tuple.size() != 0&&flag==true) {
									update_page_double(page1, 1, ref.getIndexInPage(),ref.getPage(), b, cluster_key,strTableName);
									serialize(page1, ta.files.get(ref.getPage()));
								} else if(flag==true){
									update_page_2_double(ta, ref.getPage(), b, cluster_key);
									File f = new File(ta.files.get(ref.getPage()));
									f.delete();
									ta.files.remove(ref.getPage());
									serializet(ta, "data/" + strTableName + ""
											+ ".ser");
								}

							}

						}
						Page page = deserialize(ta.files.get(ref1.getPage()));
						for (int f = 0; f < col_names.size(); f++) {
							if (htblColNameValue.get(col_names.get(f)) != null) {
								
								if(getcoltypeforindex(strTableName,col_names.get(f)).equals("java.awt.Polygon")){
									polygon po1=(polygon) htblColNameValue.get(col_names.get(f));
									polygon po2=(polygon)page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f));
									if(po1.mycompareto(po2)!=0){
										flag = false;
									}
									}
								
								else if (!htblColNameValue.get(col_names.get(f)).equals(page.Tuple.get(ref1.getIndexInPage()).Hash.get(col_names.get(f)))) {
									flag = false;
								}}}
								if (flag == true) {
									page.Tuple.remove(ref1.getIndexInPage());
									update_trees_after_delete(strTableName, ref1, htblColNameValue, page);
									btree=true;
								}
							
						if (page.Tuple.size() != 0&&flag==true) {
							update_page_double(page, 1, ref1.getIndexInPage(),ref1.getPage(), b, cluster_key,strTableName);
							serialize(page, ta.files.get(ref1.getPage()));
						} else if(flag==true) {
							update_page_2_double(ta, ref1.getPage(), b, cluster_key);
							File f = new File(ta.files.get(ref1.getPage()));
							f.delete();
							ta.files.remove(ref1.getPage());
							serializet(ta, "data/" + strTableName + "" + ".ser");
						}
						
						if(btree==true){
						b.delete(key_value);
						serialize_RTree(strTableName, cluster_key, b);}
					
						
					}
					
					
					
				}
			}
		}
		

		else {
			if (checkfirstword("data/metadata.class", strTableName).size() == 0) {
				System.out.println("Enter correct table name.");
				return;
			}

			Table table;
			Vector<String> TableFiles = deserlizetf();
			int tableCounter;

			for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) {
				if (TableFiles.get(tableCounter).equals("data/" + strTableName + "" + ".ser")) {
					break;
				}
			}
			table = deserializet(TableFiles.get(tableCounter));

			if (htblColNameValue.get(table.ClusteringKey) != null) { // clusterKey does exist

				Tuple tuple = new Tuple(table.ClusteringKey, htblColNameValue); // tuple to be edited

				int pageIndex;
				for (pageIndex = 0; pageIndex < table.files.size(); pageIndex++) {

					Page A = deserialize(table.files.get(pageIndex));

					Tuple lastTuple = A.Tuple.get(A.Tuple.size() - 1); // last tuple in each page
					int rightIndex;

					if (lastTuple.Hash.get(lastTuple.getKey()).equals(tuple.Hash.get(tuple.key))) {
						rightIndex = A.Tuple.size() - 1;

						for (int temp = rightIndex; temp >= 0; temp--) {

							if (A.Tuple.get(temp).Hash.get(A.Tuple.get(temp).getKey()).equals(tuple.Hash.get(tuple.key))) {

								A.Tuple.remove(temp);

							} else {
								break;
							}
						}
					} else {

						rightIndex = Collections.binarySearch(A.Tuple, tuple);

						if (rightIndex >= 0) {

							for (int i = rightIndex + 1; i < A.Tuple.size(); i++) {
								if (A.Tuple.get(i).Hash.get(A.Tuple.get(i).getKey()).equals(tuple.Hash.get(tuple.key))) {
									A.Tuple.remove(i);
									i--;
								} else {
									// break;
								}
							}
							for (int temp = rightIndex; temp >= 0; temp--) {

								if (A.Tuple.get(temp).Hash.get(A.Tuple.get(temp).getKey())
										.equals(tuple.Hash.get(tuple.key))) {
									A.Tuple.remove(temp);

								} else {
									// break;
								}
							}

						}
					}
					if (A.Tuple.size() == 0) {

						File f = new File(table.files.get(pageIndex));
						f.delete();
						table.files.remove(pageIndex);
						serializet(table, TableFiles.get(tableCounter));
						table = deserializet(TableFiles.get(tableCounter));
						pageIndex--;

					} else {
						serialize(A, table.files.get(pageIndex));
						serializet(table, TableFiles.get(tableCounter));
					}

				} // For of pages

				serlizetf(TableFiles);
				return;
			} else {// clusterKey does not exist

				int pageIndex;
				Object[] keys = htblColNameValue.keySet().toArray();
				Object[] values = htblColNameValue.values().toArray();
				for (pageIndex = 0; pageIndex < table.files.size(); pageIndex++) {

					Page A = deserialize(table.files.get(pageIndex));
					for (int temp = 0; temp < A.Tuple.size(); temp++) {

						int counter = 0;
						for (int i = 0; i < keys.length; i++) {

							if (A.Tuple.get(temp).Hash.get(keys[i]).equals(values[i])) {
								counter++;

							}
						}

						if (counter == keys.length) {

							A.Tuple.remove(temp);
							temp--;
						}
					}

					serialize(A, table.files.get(pageIndex));
					if (A.Tuple.size() == 0) {

						File f = new File(table.files.get(pageIndex));
						f.delete();

						table.files.remove(pageIndex);
						table.Touchdate = new Timestamp(System.currentTimeMillis());
						serializet(table, TableFiles.get(tableCounter));
						table = deserializet(TableFiles.get(tableCounter));
						pageIndex--;

					} else {
						serialize(A, table.files.get(pageIndex));
						table.Touchdate = new Timestamp(System.currentTimeMillis());
						serializet(table, TableFiles.get(tableCounter));
					}
				} // For of pages

				serlizetf(TableFiles);
			}

		}// End Method
		

	}

	
	public java.util.Iterator<Tuple> select(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws IOException {
		Vector <Tuple> v1=new Vector<Tuple>();
		String clustering_key = "";
		Table table=deserializet("data/" + arrSQLTerms[0]._strTableName + "" + ".ser");
		clustering_key = getcluster(arrSQLTerms[0]._strTableName,"data/metadata.class");
		Vector<Ref> finale=new Vector<Ref>();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			Vector<Ref> result=new Vector<Ref>();
			if (arrSQLTerms[i]._strColumnName.equals(clustering_key)) {
				String cluster_type = getcluster_type(arrSQLTerms[i]._strTableName);
				if (check_if_cluster_indexed(arrSQLTerms[i]._strTableName)) {
					if (cluster_type.equals("java.lang.Integer")) {
						 result = search_for_index_cluster_int(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.lang.String")) {
						 result = search_for_index_cluster_String(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.lang.double")) {
						 result = search_for_index_cluster_double(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.lang.Boolean")) {
						 result = search_for_index_cluster_boolean(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.util.Date")) {
						 result = search_for_index_cluster_Date(arrSQLTerms[i]);
					}else if (cluster_type.equals("java.awt.Polygon")) {
						 result = search_for_index_cluster_polygon(arrSQLTerms[i]);
					}
					

				} else {
					if (cluster_type.equals("java.lang.Integer")) {
						 result = binary_for_select_int(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.lang.String")) {
						result = binary_for_select_String(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.lang.double")) {
						result = binary_for_select_double(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.lang.Boolean")) {
						result = binary_for_select_boolean(arrSQLTerms[i]);
					} else if (cluster_type.equals("java.util.Date")) {
						result = binary_for_select_Date(arrSQLTerms[i]);
					}else if (cluster_type.equals("java.awt.Polygon")) {
						result = binary_for_select_polygon(arrSQLTerms[i]);
					}
					

				}
			} else {
				if (check_Index(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName)) {
					if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.Integer")) {

						 result=search_for_index_not_cluster_int(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.String")) {
                        
						 result=search_for_index_not_cluster_String(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.double")) {

						 result=search_for_index_not_cluster_double(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.Boolean")) {

						 result=search_for_index_not_cluster_Boolean(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.util.Date")) {

						 result=search_for_index_not_cluster_Date(arrSQLTerms[i]);
					}else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.awt.Polygon")) {

						 result=search_for_index_not_cluster_polygon(arrSQLTerms[i]);
					}			
	

				} else {
					if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.Integer")) {
						 result=search_for_not_cluster_int(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.String")) {
                         
						result=search_for_not_cluster_String(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.double")) {

						result=search_for_not_cluster_double(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.lang.Boolean")) {

						result=search_for_not_cluster_Boolean(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.util.Date")) {

						result=search_for_not_cluster_Date(arrSQLTerms[i]);
					} else if (getcoltypeforindex(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName).equals("java.awt.Polygon")) {

						result=search_for_not_cluster_polygon(arrSQLTerms[i]);
					}
					
				}
			}
			
			if(i!=0){
			finale=	result_of_select(strarrOperators[i-1],finale,result);
			}
			else{
				finale=result;
			}
		}
		for (int i = 0; i < table.files.size(); i++) {
			Page p = deserialize(table.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				for(int f=0;f<finale.size();f++){
					if(finale.get(f).getPage()==i&&finale.get(f).getIndexInPage()==j){
					
						v1.add(p.Tuple.get(j));
					}
				}
			}
		}
		java.util.Iterator<Tuple> it=v1.iterator();
		return  it;
		
		

	}


	public static void writeToFile(String path, String text) {
		try {
			String oldT = readFromFile(path);
			oldT = oldT + text;
			FileWriter fw = new FileWriter(path);
			fw.write(oldT);
			fw.close();
		} catch (IOException e) {
			System.out.println("Path incorrect.");
		}
	}

	public static String readFromFile(String path) {
		String res = "";
		try {
			File file = new File(path);

			BufferedReader br = new BufferedReader(new FileReader(file));

			String st;
			while ((st = br.readLine()) != null)
				res = res + st + "\n";
			br.close();
		} catch (IOException e) {
			//System.out.println("File name is incorrect");
		}
		return res;
	}

	public static int countLines(String str) {
		String[] lines = str.split("\r\n|\r|\n");
		return lines.length;
	}

	public static boolean validaterecord(String strTableName, Hashtable<String, Object> htblColNameValue) {

		String table_metadata = "";
		ArrayList<String> table = checkfirstword("data/metadata.class", strTableName);
		for (int i = 0; i < table.size(); i++) {
			table_metadata = table_metadata + (table.get(i)) + "\n";
		}

		Set<String> value_table_keys = htblColNameValue.keySet();
		int table_columns_no = countLines(table_metadata);
		if (table_columns_no == 0) {
			return false;
		}

		String value_metadata_form = "";
		for (String key : value_table_keys) {
			value_metadata_form = value_metadata_form + strTableName + "," + "" + key + "," + htblColNameValue.get(key)
					+ "\n";
		}

		int values_columns_no = countLines(value_metadata_form);
		if (values_columns_no != table_columns_no) {
			return false;
		}

		String[] table_lines = table_metadata.split("\\n");
		String[] value_lines = value_metadata_form.split("\\n");
		for (int i = 0; i < table_columns_no; i++) {
			String[] table_line = table_lines[i].split(",");
			String[] value_line = value_lines[i].split(",");

			if (!table_line[1].equals(value_line[1])) {
				return false;
			}
		} // checking names

		for (int i = 0; i < table_columns_no; i++) {
			String[] table_line = table_lines[i].split(",");
			if (table_line[2].contentEquals(htblColNameValue.get(table_line[1]).getClass().toString().toLowerCase())) {
				return false;
			}
		} // checking type

		return true;

	}
	
	public static ArrayList<String> checkfirstword(String path, String Tname) {
		String res = "";

		ArrayList<String> aList = new ArrayList<String>();
		String s = "";
		String first = "";

		try {
			File file = new File(path);

			BufferedReader br = new BufferedReader(new FileReader(file));

			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				first = words[0];
				if (first.equals(Tname))
					aList.add(s);
			}
			br.close();

		} catch (IOException e) {
			//System.out.println("File name is incorrect");
		}
		return aList;
	}
	
	
	public static String getcluster(String name, String path)
			throws IOException {
		String s = "";
		String t = "";
		File file = new File(path);

		BufferedReader br = new BufferedReader(new FileReader(file));

		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(name)) {
					if (words[3].equals("True"))
						t = words[1];
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		br.close();

		return t;
	}

	public static String getcluster_type(String name) throws IOException {
		String s = "";
		String t = "";
		File file = new File("data/metadata.class");

		BufferedReader br = new BufferedReader(new FileReader(file));

		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(name)) {
					if (words[3].equals("True"))
						t = words[2];
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		br.close();

		return t;
	}

	public static boolean gettype(String name, String path,
			Hashtable<String, Object> htblColNameValue) throws IOException {
		String s = "";
		String t = "";
		int c = 0;
		boolean flag = false;
		File file = new File(path);

		BufferedReader br = new BufferedReader(new FileReader(file));

		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");

				if (words[0].equals(name)) {
					t = words[1];
					flag = false;
					c = 0;
					Set<String> keys1 = htblColNameValue.keySet();
					for (String key : keys1) {
						String text1 = key;
						if (text1.equals(t)) {
							c = c + 1;
							flag = true;

						}

					}
					if (c == 0) {
						return false;
					}

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		br.close();
		return flag;
	}

	public static void serialize(Page p, String path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Page deserialize(String path) {
		Page p = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Page) in.readObject();
			in.close();
			fileIn.close();
			// System.out.println("deSerialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("Employee class not found1");
			return p;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found2");
			c.printStackTrace();
			return p;
		}
		return p;

	}

	public static Page createpage(Table t, String name) {
		int c = 0;
		for (int i = 0; i <= t.files.size(); i++) {
			c = i;
		}
		Page p = new Page();
		// c=c+1;
		String s = "data/" + name + "" + c + "" + ".ser";
		t.files.add(c, s);
		serializet(t, "data/" + name + "" + ".ser");

		return p;
	}

	public static void serializet(Table t, String path) {
		try {

			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Table deserializet(String path) {
		Table t = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			t = (Table) in.readObject();
			in.close();
			fileIn.close();
			// System.out.println("deSerialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("Employee class not found1");
			return t;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found2");
			c.printStackTrace();
			return t;
		}
		return t;
	}

	public static int binarySearch(Page p, int f, int l, Tuple t) {
		if (l >= f) {
			int mid = f + (l - f) / 2;
			// If the element is present at the
			// middle itself
			if (p.Tuple.get(mid).compareTo(t) == 0) {
				return mid;
			}

			// If element is smaller than mid, then
			// it can only be present in left subarray
			if (p.Tuple.get(mid).compareTo(t) > 0) {
				return binarySearch(p, f, mid - 1, t);
			}

			// Else the element can only be present
			// in right subarray
			// System.out.println(mid);
			return binarySearch(p, mid + 1, l, t);
		}

		// We reach here when element is not present
		// in array
		return -1;
	}

	public static void serlizetf(Vector<String> TableFiles) {
		try {
			File f = new File("data/TableFiles.class");
			f.delete();
			f.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(f);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(TableFiles);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public static Vector<String> deserlizetf() {
		Vector<String> TableFiles = null;
		try {
			FileInputStream fileIn = new FileInputStream(
					"data/TableFiles.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			TableFiles = (Vector<String>) in.readObject();
			in.close();
			fileIn.close();
			// System.out.println("deSerialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("Employee class not found tablefiles");
			return TableFiles;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found2");
			c.printStackTrace();
			return TableFiles;
		}
		return TableFiles;
	}

	public static boolean checktype(String name, String path,
			Hashtable<String, Object> htblColNameValue) {
		String s = "";
		String t = "";
		int c = 0;
		boolean flag = false;
		File file = new File(path);

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");

				if (words[0].equals(name)) {
					t = words[2];
					flag = false;
					c = 0;
					Set<String> keys1 = htblColNameValue.keySet();
					for (String key : keys1) {
						String text1 = ""
								+ htblColNameValue.get(key).getClass();
						if (text1.toLowerCase().equals(
								("class" + " " + t).toLowerCase())) {
							c = c + 1;
							flag = true;

						}
					}
					if (c == 0) {
						return false;
					}

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;

	}

	public static String getcoltypeforindex(String strTableName,
			String strColName) {
		String s = "";
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[1].equals(strColName)
						&& words[0].equals(strTableName)) {
					return words[2];
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void update_metadata_forINDEX(String strTableName,
			String strColName) {
		String s = "";
		boolean flag = true;
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		int counter = counter();
		int c = 3;
		try {
			while ((s = br.readLine()) != null || c > 0) {
				c = c - 1;
				String[] words = s.split(",");
				if (words[1].equals(strColName)
						&& words[0].equals(strTableName)) {
					words[4] = "True";
				}
				line = line + words[0] + "," + words[1] + "," + words[2] + ","
						+ words[3] + "," + words[4] + "\n";
			}
			writeToFile_updated("data/metadata.class", line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void writeToFile_updated(String path, String text) {
		try {
			FileWriter fw = new FileWriter(path);
			fw.write(text);
			fw.close();
		} catch (IOException e) {
			System.out.println("Path incorrect.");
		}
	}

	public static int counter() {
		String res = "";
		int counter = 0;
		try {
			File file = new File("data/metadata.class");

			BufferedReader br = new BufferedReader(new FileReader(file));

			String st;
			while ((st = br.readLine()) != null) {
				counter++;
				res = res + st + "\n";
			}
			br.close();
		} catch (IOException e) {
			//System.out.println("File name is incorrect");
		}
		return counter;
	}

	public static boolean check_Index(String strTableName, String strColName) {
		String s = "";
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[1].equals(strColName) && words[4].equals("True")
						&& words[0].equals(strTableName)) {
					return true;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static boolean check_if_cluster_indexed(String strTableName) {
		String s = "";
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(strTableName) && words[3].equals("True")
						&& words[4].equals("True")) {
					return true;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static void serialize_BTree(String strTableName, String strColName,
			BPTree Tree) {
		try {
			File f = new File("data/B Tree index for " + strTableName + "_"
					+ strColName + ".ser");
			f.delete();
			f.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(f);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(Tree);
			out.close();
			fileOut.close();
			// System.out.println("Serialized BTREE");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
    
	public static void serialize_RTree(String strTableName, String strColName,RTree Tree) {
		try {
			File f = new File("data/R Tree index for " + strTableName + "_"
					+ strColName + ".ser");
			f.delete();
			f.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(f);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(Tree);
			out.close();
			fileOut.close();
			// System.out.println("Serialized BTREE");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public static BPTree deserialize_BTree(String strTableName,
			String strColName) {

		if (getcoltypeforindex(strTableName, strColName).equals(
				"java.lang.Integer")) {
			BPTree<Integer> b = null;
			try {
				FileInputStream fileIn = new FileInputStream(
						"data/B Tree index for " + strTableName + "_"
								+ strColName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				b = (BPTree<Integer>) in.readObject();
				in.close();
				fileIn.close();
				// System.out.println("deSerialized BTree ");
			} catch (IOException i) {
				i.printStackTrace();
				System.out.println("Employee class not found tablefiles");
				return b;
			} catch (ClassNotFoundException c) {
				System.out.println("Employee class not found2");
				c.printStackTrace();
				return b;
			}
			return b;
		}

		else if (getcoltypeforindex(strTableName, strColName).equals(
				"java.lang.String")) {
			BPTree<String> b = null;
			try {
				FileInputStream fileIn = new FileInputStream(
						"data/B Tree index for " + strTableName + "_"
								+ strColName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				b = (BPTree<String>) in.readObject();
				in.close();
				fileIn.close();
				//System.out.println("deSerialized BTree ");
			} catch (IOException i) {
				i.printStackTrace();
				System.out.println("Employee class not found tablefiles");
				return b;
			} catch (ClassNotFoundException c) {
				System.out.println("Employee class not found2");
				c.printStackTrace();
				return b;
			}
			return b;
		}

		else if (getcoltypeforindex(strTableName, strColName).equals(
				"java.lang.double")) {
			BPTree<Double> b = null;
			try {
				FileInputStream fileIn = new FileInputStream(
						"data/B Tree index for " + strTableName + "_"
								+ strColName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				b = (BPTree<Double>) in.readObject();
				in.close();
				fileIn.close();
				//System.out.println("deSerialized BTree ");
			} catch (IOException i) {
				i.printStackTrace();
				System.out.println("Employee class not found tablefiles");
				return b;
			} catch (ClassNotFoundException c) {
				System.out.println("Employee class not found2");
				c.printStackTrace();
				return b;
			}
			return b;

		}

		else if (getcoltypeforindex(strTableName, strColName).equals(
				"java.lang.Boolean")) {
			BPTree<Boolean> b = null;
			try {
				FileInputStream fileIn = new FileInputStream(
						"data/B Tree index for " + strTableName + "_"
								+ strColName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				b = (BPTree<Boolean>) in.readObject();
				in.close();
				fileIn.close();
				//System.out.println("deSerialized BTree ");
			} catch (IOException i) {
				i.printStackTrace();
				System.out.println("Employee class not found tablefiles");
				return b;
			} catch (ClassNotFoundException c) {
				System.out.println("Employee class not found2");
				c.printStackTrace();
				return b;
			}
			return b;
		}

		else if (getcoltypeforindex(strTableName, strColName).equals(
				"java.util.Date")) {
			BPTree<Date> b = null;
			try {
				FileInputStream fileIn = new FileInputStream(
						"data/B Tree index for " + strTableName + "_"
								+ strColName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				b = (BPTree<Date>) in.readObject();
				in.close();
				fileIn.close();
				//System.out.println("deSerialized BTree ");
			} catch (IOException i) {
				i.printStackTrace();
				System.out.println("Employee class not found tablefiles");
				return b;
			} catch (ClassNotFoundException c) {
				System.out.println("Employee class not found2");
				c.printStackTrace();
				return b;
			}
			return b;
		}
		return null;
	}
    
	public static RTree deserialize_RTree(String strTableName,String strColName) {

		if (getcoltypeforindex(strTableName, strColName).equals("java.awt.Polygon")) {
			RTree<Double> b = null;
			try {
				FileInputStream fileIn = new FileInputStream(
						"data/R Tree index for " + strTableName + "_"
								+ strColName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				b = (RTree<Double>) in.readObject();
				in.close();
				fileIn.close();
				// System.out.println("deSerialized BTree ");
			} catch (IOException i) {
				i.printStackTrace();
				System.out.println("Employee class not found tablefiles");
				return b;
			} catch (ClassNotFoundException c) {
				System.out.println("Employee class not found2");
				c.printStackTrace();
				return b;
		}
return b;
	
	}
	return null;
}
	public static int read_config_file() {
		FileReader fileReader = null;
		try {
			try {
				fileReader = new FileReader("config/DBApp.properties");
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			Properties p = new Properties();
			try {
				p.load(fileReader);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String NodeSize = p.getProperty("NodeSize");
			int i = Integer.parseInt(NodeSize);
			return i;
		} finally {
			if (fileReader != null) {
				try {
					fileReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public void serialize_classtree(Vector<Ref> overflow, String path) {
		try {
			File f = new File(path);
			f.delete();
			f.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(f);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(overflow);
			out.close();
			fileOut.close();
			// System.out.println("Serialized BTREE");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public Vector<Ref> deserialize_classTree(String path) {
		Vector<Ref> v = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector<Ref>) in.readObject();
			in.close();
			fileIn.close();
			// System.out.println("deSerialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("Employee class not found1");
			return v;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found2");
			c.printStackTrace();
			return v;
		}
		return v;
	}

	public void serialize_Overflow_page(Vector<String> big_vector) {
		try {
			File f = new File("data/Overflow of big_vector.ser");
			f.delete();
			f.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(f);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(big_vector);
			out.close();
			fileOut.close();
			// System.out.println("Serialized BTREE");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public void serialize_Overflow_numbers(Vector<Object> Overflow_numbers) {
		try {
			
			File f = new File("data/Overflow numbers.ser");
			f.delete();
			f.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(f);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(Overflow_numbers);
			out.close();
			fileOut.close();
			// System.out.println("Serialized BTREE");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public Vector<String> deserialize_Overflow_page() {
		Vector<String> v = null;
		try {
			FileInputStream fileIn = new FileInputStream(
					"data/Overflow of big_vector.ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector<String>) in.readObject();
			in.close();
			fileIn.close();
			// System.out.println("deSerialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("Employee class not found1");
			return v;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found2");
			c.printStackTrace();
			return v;
		}
		return v;
	}

	public Vector<Object> deserialize_Overflow_numbers() {
		Vector<Object> v = null;
		try {
			FileInputStream fileIn = new FileInputStream(
					"data/Overflow numbers.ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector<Object>) in.readObject();
			in.close();
			fileIn.close();
			// System.out.println("deSerialized data is saved in /tmp/tes");
		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("Employee class not found1");
			return v;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found2");
			c.printStackTrace();
			return v;
		}
		return v;
	}

	public void update_ref(int Key, Page p) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i)instanceof Integer){
			if ((Integer) overflow_numbers.get(i) > Key) {
				Vector<Ref> v1 = new Vector<Ref>();
				Vector<Ref> overflow_page = db
						.deserialize_classTree("data/Overflow of "
								+ overflow_numbers.get(i) + ".ser");
				for (int j = 0; j < overflow_page.size(); j++) {
					Ref r1 = new Ref(overflow_page.get(j).getPage(),
							overflow_page.get(j).getIndexInPage() + 1);
					if (r1.getIndexInPage() == p.Tuple.capacity()) {
						Ref r2 = new Ref(overflow_page.get(j).getPage() + 1, 0);
						v1.add(r2);
					} else {
						v1.add(r1);
					}
				}

				serialize_classtree(v1,
						"data/Overflow of " + overflow_numbers.get(i) + ".ser");
			}
		}}
	}

	public void update_ref_String(String Key, Page p) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i)instanceof String){
			if (((String) overflow_numbers.get(i)).compareTo(Key) > 0) {
				Vector<Ref> v1 = new Vector<Ref>();
				Vector<Ref> overflow_page = db
						.deserialize_classTree("data/Overflow of "
								+ overflow_numbers.get(i) + ".ser");
				for (int j = 0; j < overflow_page.size(); j++) {
					Ref r1 = new Ref(overflow_page.get(j).getPage(),
							overflow_page.get(j).getIndexInPage() + 1);
					if (r1.getIndexInPage() == p.Tuple.capacity()) {
						Ref r2 = new Ref(overflow_page.get(j).getPage() + 1, 0);
						v1.add(r2);
					} else {
						v1.add(r1);
					}
					
				}
              
				serialize_classtree(v1,
						"data/Overflow of " + overflow_numbers.get(i) + ".ser");
			}
		}}
	}

	public void update_ref_double(double Key, Page p) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i)instanceof Double){
			if ((double) overflow_numbers.get(i) > Key) {
				Vector<Ref> v1 = new Vector<Ref>();
				Vector<Ref> overflow_page = db
						.deserialize_classTree("data/Overflow of "
								+ overflow_numbers.get(i) + ".ser");
				for (int j = 0; j < overflow_page.size(); j++) {
					Ref r1 = new Ref(overflow_page.get(j).getPage(),
							overflow_page.get(j).getIndexInPage() + 1);
					if (r1.getIndexInPage() == p.Tuple.capacity()) {
						Ref r2 = new Ref(overflow_page.get(j).getPage() + 1, 0);
						v1.add(r2);
					} else {
						v1.add(r1);
					}
				}

				serialize_classtree(v1,
						"data/Overflow of " + overflow_numbers.get(i) + ".ser");
			}
		}}
	}
	
	public void update_ref_Boolean(boolean Key, Page p) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i)instanceof Boolean){
			if (((Boolean) overflow_numbers.get(i)).compareTo(Key)>0) {
				Vector<Ref> v1 = new Vector<Ref>();
				Vector<Ref> overflow_page = db
						.deserialize_classTree("data/Overflow of "
								+ overflow_numbers.get(i) + ".ser");
				for (int j = 0; j < overflow_page.size(); j++) {
					Ref r1 = new Ref(overflow_page.get(j).getPage(),
							overflow_page.get(j).getIndexInPage() + 1);
					if (r1.getIndexInPage() == p.Tuple.capacity()) {
						Ref r2 = new Ref(overflow_page.get(j).getPage() + 1, 0);
						v1.add(r2);
					} else {
						v1.add(r1);
					}
				}

				serialize_classtree(v1,
						"data/Overflow of " + overflow_numbers.get(i) + ".ser");
			}
		}}
	}
	public void update_ref_Date(Date Key, Page p) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i)instanceof Date){
			if (((Date) overflow_numbers.get(i)).compareTo(Key)>0) {
				Vector<Ref> v1 = new Vector<Ref>();
				Vector<Ref> overflow_page = db
						.deserialize_classTree("data/Overflow of "
								+ overflow_numbers.get(i) + ".ser");
				for (int j = 0; j < overflow_page.size(); j++) {
					Ref r1 = new Ref(overflow_page.get(j).getPage(),
							overflow_page.get(j).getIndexInPage() + 1);
					if (r1.getIndexInPage() == p.Tuple.capacity()) {
						Ref r2 = new Ref(overflow_page.get(j).getPage() + 1, 0);
						v1.add(r2);
					} else {
						v1.add(r1);
					}
				}

				serialize_classtree(v1,
						"data/Overflow of " + overflow_numbers.get(i) + ".ser");
			}
		}}
	}

	public boolean check_overflow_page_int(int key) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i) instanceof Integer){
			if ((int) overflow_numbers.get(i) == key) {
				return true;
			}}
		}

		return false;
	}

	public boolean check_overflow_page_String(String key) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if (overflow_numbers.get(i) instanceof String) {
				String a = (String) overflow_numbers.get(i);
				if (a.equals(key)) {
					return true;
				}
			}
		}

		return false;
	}

	public boolean check_overflow_page_double(double key) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i) instanceof Double){
			double a = (double) overflow_numbers.get(i);
			if (a == key) {
				return true;
			}}
		}

		return false;
	}

	public boolean check_overflow_page_Date(Date key) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i) instanceof Date){
			Date a = (Date) overflow_numbers.get(i);
			if (a.equals(key)) {
				return true;
			}}
		}

		return false;
	}

	public boolean check_overflow_page_boolean(boolean key) {
		DBApp db = new DBApp();
		Vector<Object> overflow_numbers = db.deserialize_Overflow_numbers();
		for (int i = 0; i < overflow_numbers.size(); i++) {
			if(overflow_numbers.get(i) instanceof Boolean){
			boolean a = (boolean) overflow_numbers.get(i);
			if (a == key) {
				return true;
			}}
		}

		return false;
	}

	public static void update_page(Page p_a, int counter, int index_in_page,int page_index, BPTree<Integer> b, String cluster_key,String table_name) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				b.update((Integer) p_a.Tuple.get(i).Hash.get(cluster_key), old,n);
			}
			serialize_BTree(table_name,cluster_key,b);
			counter--;
		}

	}

	public static void update_page_String(Page p_a, int counter,
			int index_in_page, int page_index, BPTree<String> b,
			String cluster_key,String table_name ) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				b.update((String) p_a.Tuple.get(i).Hash.get(cluster_key), old,
						n);
			}
			serialize_BTree(table_name,cluster_key,b);
			counter--;
		}

	}

	public static void update_page_double(Page p_a, int counter,
			int index_in_page, int page_index, BPTree<Double> b,
			String cluster_key,String table_name) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				b.update((Double) p_a.Tuple.get(i).Hash.get(cluster_key), old,
						n);
			}
			serialize_BTree(table_name,cluster_key,b);
			counter--;
		}

	}
    
	public static void update_page_double(Page p_a, int counter,
			int index_in_page, int page_index, RTree<Double> b,
			String cluster_key,String table_name) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				polygon po=(polygon)p_a.Tuple.get(i).Hash.get(cluster_key);
				b.update(po.area, old,n);
			}
			serialize_RTree(table_name,cluster_key,b);
			counter--;
		}

	}
	
	public static void update_page_Date(Page p_a, int counter,
			int index_in_page, int page_index, BPTree<Date> b,
			String cluster_key,String table_name) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				b.update((Date) p_a.Tuple.get(i).Hash.get(cluster_key), old, n);
			}
			serialize_BTree(table_name,cluster_key,b);
			counter--;
		}

	}

	public static void update_page_boolean(Page p_a, int counter,
			int index_in_page, int page_index, BPTree<Boolean> b,
			String cluster_key,String table_name) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				b.update((boolean) p_a.Tuple.get(i).Hash.get(cluster_key), old,
						n);
			}
			serialize_BTree(table_name,cluster_key,b);
			counter--;
		}

	}

	public static void update_page_2(Table t, int page_index,
			BPTree<Integer> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				b.update((Integer) p.Tuple.get(j).Hash.get(cluster_key), old, n);
			}
		}

	}

	public static void update_page_2_String(Table t, int page_index,
			BPTree<String> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				b.update((String) p.Tuple.get(j).Hash.get(cluster_key), old, n);
			}
		}


	}

	public static void update_page_2_double(Table t, int page_index,
			BPTree<Double> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				b.update((Double) p.Tuple.get(j).Hash.get(cluster_key), old, n);
			}
		}

	}

	public static void update_page_2_double(Table t, int page_index,
			RTree<Double> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				polygon po=(polygon)p.Tuple.get(j).Hash.get(cluster_key);
				b.update(po.area, old, n);
			}
		}

	}
	
	public static void update_page_2_Date(Table t, int page_index,  
			BPTree<Date> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				b.update((Date) p.Tuple.get(j).Hash.get(cluster_key), old, n);
			}
		}

	}

	public static void update_page_2_boolean(Table t, int page_index,
			BPTree<Boolean> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				b.update((Boolean) p.Tuple.get(j).Hash.get(cluster_key), old, n);
			}
		}

	}

	public static Vector<String> get_indexed_col(String strTableName) {
		String s = "";
		Vector<String> v = new Vector<String>();
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(strTableName) && words[4].equals("True")) {
					v.add(words[1]);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}

	public static Vector<String> get_all_col(String strTableName) {
		String s = "";
		Vector<String> v = new Vector<String>();
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(strTableName)) {
					v.add(words[1]);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}

	

	public static Vector<Ref> search_for_index_cluster_int(SQLTerm s) {
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Integer> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		Vector<Ref> v1=new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
				return result;
			} else {
				result.add(b.search((int) s._objValue));
				return result;
			}

		} else if (s._strOperator.equals("!=")) {
			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
			} else {
				result.add(b.search((int) s._objValue));
			}
            if(result.get(0)==null){
            	result.remove(0);
            result.add(new Ref(0,0));}
            
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);
			 v1 = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				v1.add(v.get(i));
			}
			return v1;

		} else if (s._strOperator.equals(">")) {
			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
			} else {
				result.add(b.search((int) s._objValue));
			}
			 if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((int) s._objValue,null,-1));
			 }
			result = get_biggerthan_cluster(result.get(result.size() - 1),table);
			return result;

		} else if (s._strOperator.equals("<")) {
			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
			} else {
				result.add(b.search((int) s._objValue));
			}
			if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((int) s._objValue,null,-1));
			 }
			result = get_smallerthan_cluster(result.get(0), table);
			return result;

		} else if (s._strOperator.equals(">=")) {

			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
			} else {
				result.add(b.search((int) s._objValue));
			}
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);

			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;

		} else if (s._strOperator.equals("<=")) {

			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
			} else {
				result.add(b.search((int) s._objValue));
			}
			Vector<Ref> v = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;
		}
		return result;

	}
	public static Vector<Ref> search_for_index_cluster_String(SQLTerm s) {
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<String> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		Vector<Ref> v1=new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
				return result;
			} else {
				result.add(b.search((String) s._objValue));
				return result;
			}

		} else if (s._strOperator.equals("!=")) {
			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
			} else {
				result.add(b.search((String) s._objValue));
			}
            if(result.get(0)==null){
            	result.remove(0);
            result.add(new Ref(0,0));}
            
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);
			 v1 = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				v1.add(v.get(i));
			}
			return v1;

		} else if (s._strOperator.equals(">")) {
			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
			} else {
				result.add(b.search((String) s._objValue));
			}
			 if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((String) s._objValue,null,-1));
			 }
			result = get_biggerthan_cluster(result.get(result.size() - 1),table);
			return result;

		} else if (s._strOperator.equals("<")) {
			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
			} else {
				result.add(b.search((String) s._objValue));
			}
			if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((String) s._objValue,null,-1));
			 }
			result = get_smallerthan_cluster(result.get(0), table);
			return result;

		} else if (s._strOperator.equals(">=")) {

			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
			} else {
				result.add(b.search((String) s._objValue));
			}
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);

			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;

		} else if (s._strOperator.equals("<=")) {

			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
			} else {
				result.add(b.search((String) s._objValue));
			}
			Vector<Ref> v = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;
		}
		return result;

	}
	
	public static Vector<Ref> search_for_index_cluster_double(SQLTerm s) {
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Double> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		Vector<Ref> v1=new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
				return result;
			} else {
				result.add(b.search((Double) s._objValue));
				return result;
			}

		} else if (s._strOperator.equals("!=")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
			} else {
				result.add(b.search((Double) s._objValue));
			}
            if(result.get(0)==null){
            	result.remove(0);
            result.add(new Ref(0,0));}
            
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);
			 v1 = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				v1.add(v.get(i));
			}
			return v1;

		} else if (s._strOperator.equals(">")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
			} else {
				result.add(b.search((Double) s._objValue));
			}
			 if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((Double) s._objValue,null,-1));
			 }
			result = get_biggerthan_cluster(result.get(result.size() - 1),table);
			return result;

		} else if (s._strOperator.equals("<")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
			} else {
				result.add(b.search((Double) s._objValue));
			}
			if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((Double) s._objValue,null,-1));
			 }
			result = get_smallerthan_cluster(result.get(0), table);
			return result;

		} else if (s._strOperator.equals(">=")) {

			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
			} else {
				result.add(b.search((Double) s._objValue));
			}
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);

			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;

		} else if (s._strOperator.equals("<=")) {

			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
			} else {
				result.add(b.search((Double) s._objValue));
			}
			Vector<Ref> v = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;
		}
		return result;

	}
	
	public static Vector<Ref> search_for_index_cluster_boolean(SQLTerm s) {
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Boolean> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		Vector<Ref> v1=new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
				return result;
			} else {
				result.add(b.search((Boolean) s._objValue));
				return result;
			}

		} else if (s._strOperator.equals("!=")) {
			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
			} else {
				result.add(b.search((Boolean) s._objValue));
			}
            if(result.get(0)==null){
            	result.remove(0);
            result.add(new Ref(0,0));}
            
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);
			 v1 = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				v1.add(v.get(i));
			}
			return v1;

		} else if (s._strOperator.equals(">")) {
			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
			} else {
				result.add(b.search((Boolean) s._objValue));
			}
			 if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((Boolean) s._objValue,null,-1));
			 }
			result = get_biggerthan_cluster(result.get(result.size() - 1),table);
			return result;

		} else if (s._strOperator.equals("<")) {
			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
			} else {
				result.add(b.search((Boolean) s._objValue));
			}
			if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((Boolean) s._objValue,null,-1));
			 }
			result = get_smallerthan_cluster(result.get(0), table);
			return result;

		} else if (s._strOperator.equals(">=")) {

			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
			} else {
				result.add(b.search((Boolean) s._objValue));
			}
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);

			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;

		} else if (s._strOperator.equals("<=")) {

			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
			} else {
				result.add(b.search((Boolean) s._objValue));
			}
			Vector<Ref> v = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;
		}
		return result;

	}
	
	public static Vector<Ref> search_for_index_cluster_Date(SQLTerm s) {
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Date> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		Vector<Ref> v1=new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
				return result;
			} else {
				result.add(b.search((Date) s._objValue));
				return result;
			}

		} else if (s._strOperator.equals("!=")) {
			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
			} else {
				result.add(b.search((Date) s._objValue));
			}
            if(result.get(0)==null){
            	result.remove(0);
            result.add(new Ref(0,0));}
            
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);
			 v1 = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				v1.add(v.get(i));
			}
			return v1;

		} else if (s._strOperator.equals(">")) {
			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
			} else {
				result.add(b.search((Date) s._objValue));
			}
			 if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((Date) s._objValue,null,-1));
			 }
			result = get_biggerthan_cluster(result.get(result.size() - 1),table);
			return result;

		} else if (s._strOperator.equals("<")) {
			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
			} else {
				result.add(b.search((Date) s._objValue));
			}
			if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search((Date) s._objValue,null,-1));
			 }
			result = get_smallerthan_cluster(result.get(0), table);
			return result;

		} else if (s._strOperator.equals(">=")) {

			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
			} else {
				result.add(b.search((Date) s._objValue));
			}
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);

			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;

		} else if (s._strOperator.equals("<=")) {

			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
			} else {
				result.add(b.search((Date) s._objValue));
			}
			Vector<Ref> v = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;
		}
		return result;

	}
	
	public static Vector<Ref> search_for_index_cluster_polygon(SQLTerm s) {
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		RTree<Double> b = deserialize_RTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		Vector<Ref> v1=new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
				return result;
			} else {
				result.add(b.search(((polygon) s._objValue).area));
				return result;
			}

		} else if (s._strOperator.equals("!=")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
			} else {
				result.add(b.search(((polygon) s._objValue).area));
			}
            if(result.get(0)==null){
            	result.remove(0);
            result.add(new Ref(0,0));}
            
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);
			 v1 = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				v1.add(v.get(i));
			}
			return v1;

		} else if (s._strOperator.equals(">")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
			} else {
				result.add(b.search(((polygon) s._objValue).area));
			}
			 if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search(((polygon) s._objValue).area,null,-1));
			 }
			result = get_biggerthan_cluster(result.get(result.size() - 1),table);
			return result;

		} else if (s._strOperator.equals("<")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
			} else {
				result.add(b.search(((polygon) s._objValue).area));
			}
			if(result.get(0)==null){
				 result.remove(0);
				result.add(b.search(((polygon) s._objValue).area,null,-1));
			 }
			result = get_smallerthan_cluster(result.get(0), table);
			return result;

		} else if (s._strOperator.equals(">=")) {

			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
			} else {
				result.add(b.search(((polygon) s._objValue).area));
			}
			Vector<Ref> v = get_biggerthan_cluster(result.get(result.size() - 1), table);

			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;

		} else if (s._strOperator.equals("<=")) {

			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
			} else {
				result.add(b.search(((polygon) s._objValue).area));
			}
			Vector<Ref> v = get_smallerthan_cluster(result.get(0), table);
			for (int i = 0; i < v.size(); i++) {
				result.add(v.get(i));
			}
			return result;
		}
		return result;

	}

	public static Vector<Ref> search_for_index_not_cluster_int(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Integer> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_int((int) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (int) s._objValue + ".ser");
				result.add(b.search((int) s._objValue));
				return result;
			} else {
				result.add(b.search((int) s._objValue));
				return result;
			}}
			else{
				return search_for_not_cluster_int(s);
			}
	}
    
	public static Vector<Ref> search_for_index_not_cluster_String(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<String> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_String((String) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (String) s._objValue + ".ser");
				result.add(b.search((String) s._objValue));
				return result;
			} else {
				result.add(b.search((String) s._objValue));
				return result;
			}}
			else{
				return search_for_not_cluster_String(s);
			}
	}
    
	public static Vector<Ref> search_for_index_not_cluster_Boolean(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Boolean> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_boolean((Boolean) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Boolean) s._objValue + ".ser");
				result.add(b.search((Boolean) s._objValue));
				return result;
			} else {
				result.add(b.search((Boolean) s._objValue));
				return result;
			}}
			else{
				return search_for_not_cluster_Boolean(s);
			}
	}
	
	public static Vector<Ref> search_for_index_not_cluster_double(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Double> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_double((Double) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Double) s._objValue + ".ser");
				result.add(b.search((Double) s._objValue));
				return result;
			} else {
				result.add(b.search((Double) s._objValue));
				return result;
			}}
			else{
				return search_for_not_cluster_double(s);
			}
	}

	public static Vector<Ref> search_for_index_not_cluster_Date(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		BPTree<Date> b = deserialize_BTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_Date((Date) s._objValue)) {
				result = db.deserialize_classTree("data/Overflow of "+ (Date) s._objValue + ".ser");
				result.add(b.search((Date) s._objValue));
				return result;
			} else {
				result.add(b.search((Date) s._objValue));
				return result;
			}}
			else{
				
				return search_for_not_cluster_Date(s);
			}
	}
	public static Vector<Ref> search_for_index_not_cluster_polygon(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		RTree<Double> b = deserialize_RTree(s._strTableName, s._strColumnName);
		Vector<Ref> result = new Vector<Ref>();
		DBApp db = new DBApp();
		if (s._strOperator.equals("=")) {
			if (db.check_overflow_page_double(((polygon) s._objValue).area)) {
				result = db.deserialize_classTree("data/Overflow of "+ ((polygon) s._objValue).area + ".ser");
				result.add(b.search(((polygon) s._objValue).area));
				return result;
			} else {
				result.add(b.search(((polygon) s._objValue).area));
				return result;
			}}
			else{
				
				return search_for_not_cluster_polygon(s);
			}
	}
	
	
	public static Vector<Ref> get_biggerthan_cluster(Ref reference,Table table){
		Vector<Ref> result = new Vector<Ref>();
		Page page = deserialize(table.files.get(reference.getPage()));
		for(int i=reference.getIndexInPage()+1;i<page.Tuple.size();i++){
			 result.add(new Ref (reference.getPage(),i));
		}
		for (int i = reference.getPage()+1; i < table.files.size(); i++) {
			Page p = deserialize(table.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
               result.add(new Ref (i,j));
			}
		}
		return result;
	}
	
	public static Vector<Ref> get_smallerthan_cluster(Ref reference,Table table){
		Vector<Ref> result = new Vector<Ref>();
		for (int i = 0; i < reference.getPage(); i++) {
			Page p = deserialize(table.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
               result.add(new Ref (i,j));
			}
		}
		Page page = deserialize(table.files.get(reference.getPage()));
		for(int i=0;i<reference.getIndexInPage();i++){
			 result.add(new Ref (reference.getPage(),i));
		}
		return result;
	}
	
	public static Vector<Ref> search_for_not_cluster_String(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		Vector<Ref> result = new Vector<Ref>();
		if (s._strOperator.equals("=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String s1=(String) p.Tuple.get(j).Hash.get(s._strColumnName);
					String s2=(String)s._objValue;
                      if(s1.equals(s2)){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
		}
		else if (s._strOperator.equals("!=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String s1=(String) p.Tuple.get(j).Hash.get(s._strColumnName);
					String s2=(String)s._objValue;
                      if(!s1.equals(s2)){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
			
		}

		else if (s._strOperator.equals(">")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String s1=(String) p.Tuple.get(j).Hash.get(s._strColumnName);
					String s2=(String)s._objValue;
                      if(s1.compareTo(s2)>0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String s1=(String) p.Tuple.get(j).Hash.get(s._strColumnName);
					String s2=(String)s._objValue;
                      if(s1.compareTo(s2)<0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals(">=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String s1=(String) p.Tuple.get(j).Hash.get(s._strColumnName);
					String s2=(String)s._objValue;
                      if(s1.compareTo(s2)>0||s1.compareTo(s2)==0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					String s1 = (String) p.Tuple.get(j).Hash.get(s._strColumnName);
					String s2 = (String) s._objValue;
					if (s1.compareTo(s2) < 0 || s1.compareTo(s2) == 0) {
						result.add(new Ref(i, j));
					}
				}
			}
		}
		return result;

	}
	
	public static Vector<Ref> search_for_not_cluster_int(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		Vector<Ref> result = new Vector<Ref>();
		if (s._strOperator.equals("=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int s1=(int) p.Tuple.get(j).Hash.get(s._strColumnName);
					int s2=(int)s._objValue;
                      if(s1==s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
		}
		else if (s._strOperator.equals("!=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int s1=(int) p.Tuple.get(j).Hash.get(s._strColumnName);
					int s2=(int)s._objValue;
                      if(s1!=s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
			
		}

		else if (s._strOperator.equals(">")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int s1=(int) p.Tuple.get(j).Hash.get(s._strColumnName);
					int s2=(int)s._objValue;
                      if(s1>s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int s1=(int) p.Tuple.get(j).Hash.get(s._strColumnName);
					int s2=(int)s._objValue;
                      if(s1<s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals(">=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int s1=(int) p.Tuple.get(j).Hash.get(s._strColumnName);
					int s2=(int)s._objValue;
                      if(s1>=s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					int s1 = (int) p.Tuple.get(j).Hash.get(s._strColumnName);
					int s2 = (int) s._objValue;
					if (s1<=s2) {
						result.add(new Ref(i, j));
					}
				}
			}
		}
		return result;

	}
	
	public static Vector<Ref> search_for_not_cluster_double(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		Vector<Ref> result = new Vector<Ref>();
		if (s._strOperator.equals("=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=(double) p.Tuple.get(j).Hash.get(s._strColumnName);
					double s2=(double)s._objValue;
                      if(s1==s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
		}
		else if (s._strOperator.equals("!=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=(double) p.Tuple.get(j).Hash.get(s._strColumnName);
					double s2=(double)s._objValue;
                      if(s1!=s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
			
		}

		else if (s._strOperator.equals(">")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=(double) p.Tuple.get(j).Hash.get(s._strColumnName);
					double s2=(double)s._objValue;
                      if(s1>s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=(double) p.Tuple.get(j).Hash.get(s._strColumnName);
					double s2=(double)s._objValue;
                      if(s1<s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals(">=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=(double) p.Tuple.get(j).Hash.get(s._strColumnName);
					double s2=(double)s._objValue;
                      if(s1>=s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1 = (double) p.Tuple.get(j).Hash.get(s._strColumnName);
					double s2 = (double) s._objValue;
					if (s1<=s2) {
						result.add(new Ref(i, j));
					}
				}
			}
		}
		return result;

	}
	
	public static Vector<Ref> search_for_not_cluster_Boolean(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		Vector<Ref> result = new Vector<Ref>();
		if (s._strOperator.equals("=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean s1=(Boolean) p.Tuple.get(j).Hash.get(s._strColumnName);
					Boolean s2=(Boolean)s._objValue;
                      if(s1.compareTo(s2)==0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
		}
		else if (s._strOperator.equals("!=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean s1=(Boolean) p.Tuple.get(j).Hash.get(s._strColumnName);
					Boolean s2=(Boolean)s._objValue;
                      if(s1.compareTo(s2)!=0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
			
		}

		else if (s._strOperator.equals(">")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean s1=(Boolean) p.Tuple.get(j).Hash.get(s._strColumnName);
					Boolean s2=(Boolean)s._objValue;
                      if(s1.compareTo(s2)>0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean s1=(Boolean) p.Tuple.get(j).Hash.get(s._strColumnName);
					Boolean s2=(Boolean)s._objValue;
                      if(s1.compareTo(s2)<0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals(">=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean s1=(Boolean) p.Tuple.get(j).Hash.get(s._strColumnName);
					Boolean s2=(Boolean)s._objValue;
                      if(s1.compareTo(s2)>0||s1.compareTo(s2)==0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Boolean s1 = (Boolean) p.Tuple.get(j).Hash.get(s._strColumnName);
					Boolean s2 = (Boolean) s._objValue;
					if (s1.compareTo(s2) < 0 || s1.compareTo(s2) == 0) {
						result.add(new Ref(i, j));
					}
				}
			}
		}
		return result;

	}
    
	public static Vector<Ref> search_for_not_cluster_Date(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		Vector<Ref> result = new Vector<Ref>();
		if (s._strOperator.equals("=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date s1=(Date) p.Tuple.get(j).Hash.get(s._strColumnName);
					Date s2=(Date)s._objValue;
                      if(s1.compareTo(s2)==0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
		}
		else if (s._strOperator.equals("!=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date s1=(Date) p.Tuple.get(j).Hash.get(s._strColumnName);
					Date s2=(Date)s._objValue;
                      if(s1.compareTo(s2)!=0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
			
		}

		else if (s._strOperator.equals(">")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date s1=(Date) p.Tuple.get(j).Hash.get(s._strColumnName);
					Date s2=(Date)s._objValue;
                      if(s1.compareTo(s2)>0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date s1=(Date) p.Tuple.get(j).Hash.get(s._strColumnName);
					Date s2=(Date)s._objValue;
                      if(s1.compareTo(s2)<0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals(">=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date s1=(Date) p.Tuple.get(j).Hash.get(s._strColumnName);
					Date s2=(Date)s._objValue;
                      if(s1.compareTo(s2)>0||s1.compareTo(s2)==0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					Date s1 = (Date) p.Tuple.get(j).Hash.get(s._strColumnName);
					Date s2 = (Date) s._objValue;
					if (s1.compareTo(s2) < 0 || s1.compareTo(s2) == 0) {
						result.add(new Ref(i, j));
					}
				}
			}
		}
		return result;

	}
	
	public static Vector<Ref> search_for_not_cluster_polygon(SQLTerm s){
		Table table = deserializet("data/" + s._strTableName + "" + ".ser");
		Vector<Ref> result = new Vector<Ref>();
		if (s._strOperator.equals("=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					polygon s1=(polygon)p.Tuple.get(j).Hash.get(s._strColumnName);
					polygon s2=(polygon)s._objValue;
                      if(s1.mycompareto(s2)==0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
		}
		else if (s._strOperator.equals("!=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					polygon s1=(polygon)p.Tuple.get(j).Hash.get(s._strColumnName);
					polygon s2=(polygon)s._objValue;
                      if(s1.mycompareto(s2)!=0){
                    	  result.add(new Ref(i,j));
                      }
				}
			}
			
		}

		else if (s._strOperator.equals(">")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=((polygon) p.Tuple.get(j).Hash.get(s._strColumnName)).area;
					double s2=((polygon)s._objValue).area;
                      if(s1>s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=((polygon) p.Tuple.get(j).Hash.get(s._strColumnName)).area;
					double s2=((polygon)s._objValue).area;
                      if(s1<s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals(">=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=((polygon) p.Tuple.get(j).Hash.get(s._strColumnName)).area;
					double s2=((polygon)s._objValue).area;
                      if(s1>=s2){
                    	  result.add(new Ref(i,j));
                      }
				}
			}

		}

		else if (s._strOperator.equals("<=")) {
			for (int i = 0; i < table.files.size(); i++) {
				Page p = deserialize(table.files.get(i));
				for (int j = 0; j < p.Tuple.size(); j++) {
					double s1=((polygon) p.Tuple.get(j).Hash.get(s._strColumnName)).area;
					double s2=((polygon)s._objValue).area;
					if (s1<=s2) {
						result.add(new Ref(i, j));
					}
				}
			}
		}
		return result;

	}

	public static void update_ref(String table_name,String col_name){
		DBApp db=new DBApp();
		Page p=new Page();
		int index_in_page=0;
		int page_index=0;
		try {
			if(getcluster_type(table_name).equals("java.lang.Integer")){
				Vector <Object> overflow_numbers=db.deserialize_Overflow_numbers();
				for(int i=0;i<overflow_numbers.size();i++){
					Vector <Ref> new_Ref=new Vector<Ref>();	
				Vector <Ref> ref_vector=db.deserialize_classTree("data/Overflow of "+overflow_numbers.get(i)+".ser");
				index_in_page=ref_vector.get(0).getIndexInPage();
				page_index=ref_vector.get(0).getPage();
				new_Ref.add(ref_vector.get(0));
				for(int j=1;j<ref_vector.size();j++){
				index_in_page++;
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				else{
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				}
				db.serialize_classtree(new_Ref,"data/Overflow of "+overflow_numbers.get(i)+".ser");
				BPTree <Integer> b=deserialize_BTree(table_name,col_name);
				Ref r1=new_Ref.get(new_Ref.size()-1);
				index_in_page=r1.getIndexInPage()+1;
				page_index=r1.getPage();
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
				}
				b.update((Integer)overflow_numbers.get(i),new_Ref.get(0),new Ref(page_index,index_in_page));
				serialize_BTree(table_name,col_name,b);
				}
			}
			else if(getcluster_type(table_name).equals("java.lang.String")){
					Vector <Object> overflow_numbers=db.deserialize_Overflow_numbers();
					for(int i=0;i<overflow_numbers.size();i++){
						Vector <Ref> new_Ref=new Vector<Ref>();	
					Vector <Ref> ref_vector=db.deserialize_classTree("data/Overflow of "+overflow_numbers.get(i)+".ser");
					index_in_page=ref_vector.get(0).getIndexInPage();
					page_index=ref_vector.get(0).getPage();
					new_Ref.add(ref_vector.get(0));
					for(int j=1;j<ref_vector.size();j++){
					index_in_page++;
					if(index_in_page==p.Tuple.capacity()){
						index_in_page=0;
						page_index=page_index+1;
						Ref r_new=new Ref(page_index,index_in_page);
						new_Ref.add(r_new);
					}
					else{
						Ref r_new=new Ref(page_index,index_in_page);
						new_Ref.add(r_new);
					}
					}
					db.serialize_classtree(new_Ref,"data/Overflow of "+overflow_numbers.get(i)+".ser");
					BPTree <String> b=deserialize_BTree(table_name,col_name);
					Ref r1=new_Ref.get(new_Ref.size()-1);
					index_in_page=r1.getIndexInPage()+1;
					page_index=r1.getPage();
					if(index_in_page==p.Tuple.capacity()){
						index_in_page=0;
						page_index=page_index+1;
					}
					b.update((String)overflow_numbers.get(i),new_Ref.get(0),new Ref(page_index,index_in_page));
					serialize_BTree(table_name,col_name,b);
					}
				}
			else if(getcluster_type(table_name).equals("java.lang.double")){
				Vector <Object> overflow_numbers=db.deserialize_Overflow_numbers();
				for(int i=0;i<overflow_numbers.size();i++){
					Vector <Ref> new_Ref=new Vector<Ref>();	
				Vector <Ref> ref_vector=db.deserialize_classTree("data/Overflow of "+overflow_numbers.get(i)+".ser");
				index_in_page=ref_vector.get(0).getIndexInPage();
				page_index=ref_vector.get(0).getPage();
				new_Ref.add(ref_vector.get(0));
				for(int j=1;j<ref_vector.size();j++){
				index_in_page++;
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				else{
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				}
				db.serialize_classtree(new_Ref,"data/Overflow of "+overflow_numbers.get(i)+".ser");
				BPTree <Double> b=deserialize_BTree(table_name,col_name);
				Ref r1=new_Ref.get(new_Ref.size()-1);
				index_in_page=r1.getIndexInPage()+1;
				page_index=r1.getPage();
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
				}
				b.update((Double)overflow_numbers.get(i),new_Ref.get(0),new Ref(page_index,index_in_page));
				serialize_BTree(table_name,col_name,b);
				}
			}
			else if(getcluster_type(table_name).equals("java.lang.Boolean")){
				Vector <Object> overflow_numbers=db.deserialize_Overflow_numbers();
				for(int i=0;i<overflow_numbers.size();i++){
					Vector <Ref> new_Ref=new Vector<Ref>();	
				Vector <Ref> ref_vector=db.deserialize_classTree("data/Overflow of "+overflow_numbers.get(i)+".ser");
				index_in_page=ref_vector.get(0).getIndexInPage();
				page_index=ref_vector.get(0).getPage();
				new_Ref.add(ref_vector.get(0));
				for(int j=1;j<ref_vector.size();j++){
				index_in_page++;
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				else{
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				}
				db.serialize_classtree(new_Ref,"data/Overflow of "+overflow_numbers.get(i)+".ser");
				BPTree <Boolean> b=deserialize_BTree(table_name,col_name);
				Ref r1=new_Ref.get(new_Ref.size()-1);
				index_in_page=r1.getIndexInPage()+1;
				page_index=r1.getPage();
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
				}
				b.update((Boolean)overflow_numbers.get(i),new_Ref.get(0),new Ref(page_index,index_in_page));
				serialize_BTree(table_name,col_name,b);
				}
			}
			else if(getcluster_type(table_name).equals("java.util.Date")){
				Vector <Object> overflow_numbers=db.deserialize_Overflow_numbers();
				for(int i=0;i<overflow_numbers.size();i++){
					Vector <Ref> new_Ref=new Vector<Ref>();	
				Vector <Ref> ref_vector=db.deserialize_classTree("data/Overflow of "+overflow_numbers.get(i)+".ser");
				index_in_page=ref_vector.get(0).getIndexInPage();
				page_index=ref_vector.get(0).getPage();
				new_Ref.add(ref_vector.get(0));
				for(int j=1;j<ref_vector.size();j++){
				index_in_page++;
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				else{
					Ref r_new=new Ref(page_index,index_in_page);
					new_Ref.add(r_new);
				}
				}
				db.serialize_classtree(new_Ref,"data/Overflow of "+overflow_numbers.get(i)+".ser");
				BPTree <Date> b=deserialize_BTree(table_name,col_name);
				Ref r1=new_Ref.get(new_Ref.size()-1);
				index_in_page=r1.getIndexInPage()+1;
				page_index=r1.getPage();
				if(index_in_page==p.Tuple.capacity()){
					index_in_page=0;
					page_index=page_index+1;
				}
				b.update((Date)overflow_numbers.get(i),new_Ref.get(0),new Ref(page_index,index_in_page));
				serialize_BTree(table_name,col_name,b);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}
	
	
	public  String getcluster(String name)
			throws IOException {
		String s = "";
		String t = "";
		File file = new File("data/metadata.class");

		BufferedReader br = new BufferedReader(new FileReader(file));

		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(name)) {
					if (words[3].equals("True"))
						t = words[1];
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		br.close();

		return t;
	}

	

	public static Vector<Ref> binary_for_select_int(SQLTerm s) {

		Vector<Ref> results = new Vector<Ref>(); // result vector
		Vector<String> TableFiles = deserlizetf(); // deseralizing the TableFiles

		int tableCounter;
		for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) { // getting the table index
			if (TableFiles.get(tableCounter).equals("data/" + s._strTableName + "" + ".ser")) {
				break;
			}
		}

		Table wantedT = deserializet(TableFiles.get(tableCounter)); // wanted Table

		if (wantedT.files.size() == 0) {
			return results;
		} else {

			if (s._strOperator.equals("=")) {
				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if ((int) (lastTuple.Hash.get(s._strColumnName)) < (int) (s._objValue)) { // compare the objValue
																								// with the last tuple

						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if ((int) (middleTuple.Hash.get(s._strColumnName)) == (int) (s._objValue)) { // compare_the_objValue_with_the_middle_tuple

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) == (int) (s._objValue)) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else {
									break; // no more equals
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) == (int) (s._objValue)) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else {
									break; // no more equals
								}
							}

						} // finished what under the middle
						else if ((int) (middleTuple.Hash.get(s._strColumnName)) > (int) (s._objValue)) { // the_objValue_is_does_only_exist_above_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) == (int) (s._objValue)) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) < (int) (s._objValue)) {

									break; // no more equals
								}
							}

						} // finished what above the middle
						else { // the objValue does only exist under the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) == (int) (s._objValue)) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
									break; // no more equals
								}
							}
						} // finished what under the middle

					} // End of the loop of the page
					serialize(page, wantedT.files.get(pageIndex));
				} // End of the loop over the pages
			} // End of the "="

			else if (s._strOperator.equals("!=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					for (int i = 0; i < page.Tuple.size(); i++) { // loop over all the page
						if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) != (int) (s._objValue)) { // if_not_equal_to_objValue
							Ref temp1 = new Ref(pageIndex, i);
							results.add(temp1);
						} // IF
					} // End of for loop of the page
				} // End of loop over pages
			} // End of "!="

			else if (s._strOperator.equals(">")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if ((int) (lastTuple.Hash.get(s._strColumnName)) <= (int) (s._objValue)) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if ((int) (middleTuple.Hash.get(s._strColumnName)) > (int) (s._objValue)) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) { // if_bigger_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < or = (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">"

			else if (s._strOperator.equals("<")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if ((int) (page.Tuple.get(0).Hash.get(s._strColumnName)) >= (int) (s._objValue)) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if ((int) (middleTuple.Hash.get(s._strColumnName)) < (int) (s._objValue)) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) < (int) (s._objValue)) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) < (int) (s._objValue)) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or = (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) < (int) (s._objValue)) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
									break; // no more smaller
								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<"

			else if (s._strOperator.equals(">=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if ((int) (lastTuple.Hash.get(s._strColumnName)) < (int) (s._objValue)) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if ((int) (middleTuple.Hash.get(s._strColumnName)) >= (int) (s._objValue)) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) >= (int) (s._objValue)) { // if_bigger_than_or_euqal_to_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) >= (int) (s._objValue)) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) >= (int) (s._objValue)) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">="

			else if (s._strOperator.equals("<=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if ((int) (page.Tuple.get(0).Hash.get(s._strColumnName)) > (int) (s._objValue)) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if ((int) (middleTuple.Hash.get(s._strColumnName)) <= (int) (s._objValue)) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) <= (int) (s._objValue)) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) <= (int) (s._objValue)) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) <= (int) (s._objValue)) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} 
//									else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
//									break; // no more smaller
//								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<="

		} // Big else
		return results;
	}// Method
 
	public static Vector<Ref> binary_for_select_String(SQLTerm s) {

		Vector<Ref> results = new Vector<Ref>(); // result vector
		Vector<String> TableFiles = deserlizetf(); // deseralizing the TableFiles

		int tableCounter;
		for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) { // getting the table index
			if (TableFiles.get(tableCounter).equals("data/" + s._strTableName + "" + ".ser")) {
				break;
			}
		}

		Table wantedT = deserializet(TableFiles.get(tableCounter)); // wanted Table

		if (wantedT.files.size() == 0) {
			return results;
		} else {

			if (s._strOperator.equals("=")) {
				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((String) (lastTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0 ) { // compare the objValue
																								// with the last tuple

						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((String) (middleTuple.Hash.get(s._strColumnName))).equals(((String) (s._objValue)))) { // compare_the_objValue_with_the_middle_tuple

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((String) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else {
									break; // no more equals
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() ; i++) { // loop under the
																									// middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((String) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else {
									break; // no more equals
								}
							}

						} // finished what under the middle
						else if (((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) { // the_objValue_is_does_only_exist_above_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((String) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0) {

									break; // no more equals
								}
							}

						} // finished what above the middle
						else { // the objValue does only exist under the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((String) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) {
									break; // no more equals
								}
							}
						} // finished what under the middle

					} // End of the loop of the page
					serialize(page, wantedT.files.get(pageIndex));
				} // End of the loop over the pages
			} // End of the "="

			else if (s._strOperator.equals("!=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					for (int i = 0; i < page.Tuple.size(); i++) { // loop over all the page
						if (!((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((String) (s._objValue)))) { // if_not_equal_to_objValue
							Ref temp1 = new Ref(pageIndex, i);
							results.add(temp1);
						} // IF
					} // End of for loop of the page
				} // End of loop over pages
			} // End of "!="

			else if (s._strOperator.equals(">")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((String) (lastTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0||((String) (lastTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) { // if_bigger_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < or = (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">"

			else if (s._strOperator.equals("<")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((String) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0||((String) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String)(s._objValue)))<0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or = (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) {
									break; // no more smaller
								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<"

			else if (s._strOperator.equals(">=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((String) (lastTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0||((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0||((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0||((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0||((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">="

			else if (s._strOperator.equals("<=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((String) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0||((String) (middleTuple.Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0||((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0||((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))<0||((String) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((String) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} 
//									else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
//									break; // no more smaller
//								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<="

		} // Big else
		return results;
	}// Method

	public static Vector<Ref> binary_for_select_double(SQLTerm s) {

		Vector<Ref> results = new Vector<Ref>(); // result vector
		Vector<String> TableFiles = deserlizetf(); // deseralizing the TableFiles

		int tableCounter;
		for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) { // getting the table index
			if (TableFiles.get(tableCounter).equals("data/" + s._strTableName + "" + ".ser")) {
				break;
			}
		}

		Table wantedT = deserializet(TableFiles.get(tableCounter)); // wanted Table

		if (wantedT.files.size() == 0) {
			return results;
		} else {

			if (s._strOperator.equals("=")) {
				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Double) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0 ) { // compare the objValue
																								// with the last tuple

						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if ((Double) (middleTuple.Hash.get(s._strColumnName))==((Double) (s._objValue))) { // compare_the_objValue_with_the_middle_tuple

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
								if ((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))==(Double) (s._objValue)) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else {
									break; // no more equals
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() ; i++) { // loop under the
																									// middle
								if ((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))==(Double) (s._objValue)) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else {
									break; // no more equals
								}
							}

						} // finished what under the middle
						else if (((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) { // the_objValue_is_does_only_exist_above_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Double) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0) {

									break; // no more equals
								}
							}

						} // finished what above the middle
						else { // the objValue does only exist under the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Double) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) {
									break; // no more equals
								}
							}
						} // finished what under the middle

					} // End of the loop of the page
					serialize(page, wantedT.files.get(pageIndex));
				} // End of the loop over the pages
			} // End of the "="

			else if (s._strOperator.equals("!=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					for (int i = 0; i < page.Tuple.size(); i++) { // loop over all the page
						if (!((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Double) (s._objValue)))) { // if_not_equal_to_objValue
							Ref temp1 = new Ref(pageIndex, i);
							results.add(temp1);
						} // IF
					} // End of for loop of the page
				} // End of loop over pages
			} // End of "!="

			else if (s._strOperator.equals(">")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Double) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0||((Double) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) { // if_bigger_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < or = (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">"

			else if (s._strOperator.equals("<")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((Double) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0||((Double) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double)(s._objValue)))<0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or = (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) {
									break; // no more smaller
								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<"

			else if (s._strOperator.equals(">=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Double) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0||((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0||((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0||((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0||((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">="

			else if (s._strOperator.equals("<=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((Double) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0||((Double) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0||((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0||((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))<0||((Double) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Double) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} 
//									else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
//									break; // no more smaller
//								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<="

		} // Big else
		return results;
	}// Method
	
	public static Vector<Ref> binary_for_select_boolean(SQLTerm s) {

		Vector<Ref> results = new Vector<Ref>(); // result vector
		Vector<String> TableFiles = deserlizetf(); // deseralizing the TableFiles

		int tableCounter;
		for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) { // getting the table index
			if (TableFiles.get(tableCounter).equals("data/" + s._strTableName + "" + ".ser")) {
				break;
			}
		}

		Table wantedT = deserializet(TableFiles.get(tableCounter)); // wanted Table

		if (wantedT.files.size() == 0) {
			return results;
		} else {

			if (s._strOperator.equals("=")) {
				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Boolean) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0 ) { // compare the objValue
																								// with the last tuple

						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Boolean) (middleTuple.Hash.get(s._strColumnName))).equals(((Boolean) (s._objValue)))) { // compare_the_objValue_with_the_middle_tuple

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Boolean) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else {
									break; // no more equals
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() ; i++) { // loop under the
																									// middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Boolean) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else {
									break; // no more equals
								}
							}

						} // finished what under the middle
						else if (((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) { // the_objValue_is_does_only_exist_above_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Boolean) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0) {

									break; // no more equals
								}
							}

						} // finished what above the middle
						else { // the objValue does only exist under the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Boolean) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) {
									break; // no more equals
								}
							}
						} // finished what under the middle

					} // End of the loop of the page
					serialize(page, wantedT.files.get(pageIndex));
				} // End of the loop over the pages
			} // End of the "="

			else if (s._strOperator.equals("!=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					for (int i = 0; i < page.Tuple.size(); i++) { // loop over all the page
						if (!((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Boolean) (s._objValue)))) { // if_not_equal_to_objValue
							Ref temp1 = new Ref(pageIndex, i);
							results.add(temp1);
						} // IF
					} // End of for loop of the page
				} // End of loop over pages
			} // End of "!="

			else if (s._strOperator.equals(">")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Boolean) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0||((Boolean) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) { // if_bigger_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < or = (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">"

			else if (s._strOperator.equals("<")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((Boolean) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0||((Boolean) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean)(s._objValue)))<0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or = (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) {
									break; // no more smaller
								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<"

			else if (s._strOperator.equals(">=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Boolean) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0||((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0||((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0||((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0||((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">="

			else if (s._strOperator.equals("<=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((Boolean) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0||((Boolean) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0||((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0||((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))<0||((Boolean) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Boolean) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} 
//									else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
//									break; // no more smaller
//								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<="

		} // Big else
		return results;
	}// Method
	
	public static Vector<Ref> binary_for_select_Date(SQLTerm s) {

		Vector<Ref> results = new Vector<Ref>(); // result vector
		Vector<String> TableFiles = deserlizetf(); // deseralizing the TableFiles

		int tableCounter;
		for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) { // getting the table index
			if (TableFiles.get(tableCounter).equals("data/" + s._strTableName + "" + ".ser")) {
				break;
			}
		}

		Table wantedT = deserializet(TableFiles.get(tableCounter)); // wanted Table

		if (wantedT.files.size() == 0) {
			return results;
		} else {

			if (s._strOperator.equals("=")) {
				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Date) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0 ) { // compare the objValue
																								// with the last tuple

						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Date) (middleTuple.Hash.get(s._strColumnName))).equals(((Date) (s._objValue)))) { // compare_the_objValue_with_the_middle_tuple

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Date) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else {
									break; // no more equals
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() ; i++) { // loop under the
																									// middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Date) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else {
									break; // no more equals
								}
							}

						} // finished what under the middle
						else if (((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) { // the_objValue_is_does_only_exist_above_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Date) (s._objValue)))) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0) {

									break; // no more equals
								}
							}

						} // finished what above the middle
						else { // the objValue does only exist under the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Date) (s._objValue)))) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) {
									break; // no more equals
								}
							}
						} // finished what under the middle

					} // End of the loop of the page
					serialize(page, wantedT.files.get(pageIndex));
				} // End of the loop over the pages
			} // End of the "="

			else if (s._strOperator.equals("!=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					for (int i = 0; i < page.Tuple.size(); i++) { // loop over all the page
						if (!((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).equals(((Date) (s._objValue)))) { // if_not_equal_to_objValue
							Ref temp1 = new Ref(pageIndex, i);
							results.add(temp1);
						} // IF
					} // End of for loop of the page
				} // End of loop over pages
			} // End of "!="

			else if (s._strOperator.equals(">")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Date) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0||((Date) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) { // if_bigger_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < or = (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">"

			else if (s._strOperator.equals("<")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((Date) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0||((Date) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date)(s._objValue)))<0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or = (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) {
									break; // no more smaller
								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<"

			else if (s._strOperator.equals(">=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((Date) (lastTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0||((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0||((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0||((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0||((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">="

			else if (s._strOperator.equals("<=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((Date) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0||((Date) (middleTuple.Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0||((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0||((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))<0||((Date) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((Date) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} 
//									else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
//									break; // no more smaller
//								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<="

		} // Big else
		return results;
	}// Method
	
	
	public static Vector<Ref> binary_for_select_polygon(SQLTerm s) {

		Vector<Ref> results = new Vector<Ref>(); // result vector
		Vector<String> TableFiles = deserlizetf(); // deseralizing the TableFiles

		int tableCounter;
		for (tableCounter = 0; tableCounter < TableFiles.size(); tableCounter++) { // getting the table index
			if (TableFiles.get(tableCounter).equals("data/" + s._strTableName + "" + ".ser")) {
				break;
			}
		}

		Table wantedT = deserializet(TableFiles.get(tableCounter)); // wanted Table

		if (wantedT.files.size() == 0) {
			return results;
		} else {

			if (s._strOperator.equals("=")) {
				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((polygon) (lastTuple.Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))<0 ) { // compare the objValue
																								// with the last tuple

						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((polygon) (middleTuple.Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))==0) { // compare_the_objValue_with_the_middle_tuple

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))==0) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else {
									break; // no more equals
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() ; i++) { // loop under the
																									// middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))==0) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else {
									break; // no more equals
								}
							}

						} // finished what under the middle
						else if (((polygon) (middleTuple.Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))>0) { // the_objValue_is_does_only_exist_above_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))==0) { // if_equal_to_objValue
									Ref temp = new Ref(pageIndex, i);
									results.add(temp);
								} else if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))<0) {

									break; // no more equals
								}
							}

						} // finished what above the middle
						else { // the objValue does only exist under the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size() - 1; i++) { // loop under the
																									// middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))==0) { // if_equal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								} else if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))>0) {
									break; // no more equals
								}
							}
						} // finished what under the middle

					} // End of the loop of the page
					serialize(page, wantedT.files.get(pageIndex));
				} // End of the loop over the pages
			} // End of the "="

			else if (s._strOperator.equals("!=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));
					for (int i = 0; i < page.Tuple.size(); i++) { // loop over all the page
						if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).mycompareto(((polygon) (s._objValue)))!=0) { // if_not_equal_to_objValue
							Ref temp1 = new Ref(pageIndex, i);
							results.add(temp1);
						} // IF
					} // End of for loop of the page
				} // End of loop over pages
			} // End of "!="

			else if (s._strOperator.equals(">")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((polygon) (lastTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0||((polygon) (lastTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((polygon) (middleTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) { // if_bigger_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < or = (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) { // if_bigger_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">"

			else if (s._strOperator.equals("<")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((polygon) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0||((polygon) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((polygon) (middleTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon)(s._objValue)))<0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or = (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) {
									break; // no more smaller
								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<"

			else if (s._strOperator.equals(">=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					Tuple lastTuple = page.Tuple.get(page.Tuple.size() - 1); // last tuple in each page

					if (((polygon) (lastTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0) { // compare_the_objValue_with_the_last_tuple
						serialize(page, wantedT.files.get(pageIndex)); // this is not the page that we want.

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((polygon) (middleTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0||((polygon) (middleTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0||((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								}

							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0||((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// thiss means that its either < (BOTH UNWANTED)
							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0||((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // if_bigger_than_or_euqal_to_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle

						}
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of ">="

			else if (s._strOperator.equals("<=")) {

				int pageIndex;
				for (pageIndex = 0; pageIndex < wantedT.files.size(); pageIndex++) {

					Page page = deserialize(wantedT.files.get(pageIndex));

					if (((polygon) (page.Tuple.get(0).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) { // law_awel_tuple_is_equal_or_bigger_then_return
						serialize(page, wantedT.files.get(pageIndex));
						return results;

					} else {// we want this page

						Tuple middleTuple = page.Tuple.get((page.Tuple.size() - 1) / 2);
						int middleTupleIndex = (page.Tuple.size() - 1) / 2;

						if (((polygon) (middleTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0||((polygon) (middleTuple.Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // check_the_middle

							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0||((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} else if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))>0) {
									break; // no more smaller
								}
							} // finished what above the middle

							Ref temp = new Ref(pageIndex, middleTupleIndex);
							results.add(temp); // add the middle

							for (int i = middleTupleIndex + 1; i < page.Tuple.size(); i++) { // loop under the middle
								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0||((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // if_smaller_than_objValue
									Ref temp1 = new Ref(pageIndex, i);
									results.add(temp1);
								}
							} // end of the what under the middle
						} // End of middle check

						else {// this means that its either > or (BOTH UNWANTED)
							for (int i = middleTupleIndex - 1; i >= 0; i--) { // loop above the middle

								if (((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))<0||((polygon) (page.Tuple.get(i).Hash.get(s._strColumnName))).compareTo(((polygon) (s._objValue)))==0) { // if_smaller_than_objValue

									Ref temp = new Ref(pageIndex, i);
									results.add(temp);

								} 
//									else if ((int) (page.Tuple.get(i).Hash.get(s._strColumnName)) > (int) (s._objValue)) {
//									break; // no more smaller
//								}
							}
						} // end of the last else
					} // End of the Big else

					serialize(page, wantedT.files.get(pageIndex));

				} // End of loop over pages
			} // End of the "<="

		} // Big else
		return results;
	}// Method

	public static String remove_brackets(String text){
		text=text.replaceAll("[\\p{Ps}\\p{Pe}]", "");
		return text;
	}
	public static Vector<Ref> result_of_select(String operator, Vector<Ref> v1,Vector<Ref> v2) {
		Vector<Ref> finale = new Vector<Ref>();
		if (operator.equals("AND")) {
			for (int i = 0; i < v1.size(); i++) {
				for (int j = 0; j < v2.size(); j++) {
					if (v1.get(i).getPage() == v2.get(j).getPage()&& v1.get(i).getIndexInPage() == v2.get(j).getIndexInPage()) {
						finale.add(v1.get(i));
					}
				}
			}
			return finale;
		} else if (operator.equals("OR")) {
			finale=v2;
			boolean flag=true;
			for (int i = 0; i < v1.size(); i++) {
				flag=true;
				for (int j = 0; j < v2.size(); j++) {
					if (v1.get(i).getPage() == v2.get(j).getPage()&& v1.get(i).getIndexInPage() == v2.get(j).getIndexInPage()) {
						flag=false;
						break;
					}
				}
				if(flag!=false){
					finale.add(v1.get(i));
				}
			}
			return finale;

		} else if (operator.equals("XOR")) {
			boolean flag=true;
			for (int i = 0; i < v1.size(); i++) {
				flag=true;
				for (int j = 0; j < v2.size(); j++) {
					if (v1.get(i).getPage() == v2.get(j).getPage()&& v1.get(i).getIndexInPage() == v2.get(j).getIndexInPage()) {
						flag=false;
						break;
					}
				}
				if(flag!=false){
					finale.add(v1.get(i));
				}
			}
			for (int i = 0; i < v2.size(); i++) {
				flag=true;
				for (int j = 0; j < v1.size(); j++) {
					if (v2.get(i).getPage() == v1.get(j).getPage()&& v2.get(i).getIndexInPage() == v1.get(j).getIndexInPage()) {
						flag=false;
						break;
					}
				}
				if(flag!=false){
					finale.add(v2.get(i));
				}
			}
			
			return finale;
			
		}
		return finale;
	}
	
	public static Vector<String> check_if_any_col_indexed_other_cluster(String strTableName) {
		String s = "";
		Vector<String> v = new Vector<String>();
		File file = new File("data/metadata.class");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((s = br.readLine()) != null) {
				String[] words = s.split(",");
				if (words[0].equals(strTableName) && words[4].equals("True")&&words[3].equals("False")) {
					v.add(words[1]);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}
    
	public static void update_page_for_non_cluster(Page p_a, int counter, int index_in_page,int page_index, BPTree<Integer> b, String cluster_key,String table_name) {
		if (counter > 0) {
			for (int i = index_in_page; i < p_a.Tuple.size(); i++) {
				Ref old = new Ref(page_index, i + 1);
				Ref n = new Ref(page_index, i);
				b.update((Integer) p_a.Tuple.get(i).Hash.get(cluster_key), old,n);
			}
			serialize_BTree(table_name,cluster_key,b);
			counter--;
		}

	}
	
	public static void update_page_2_for_non_cluster(Table t, int page_index,BPTree<Integer> b, String cluster_key) {
		for (int i = page_index; i < t.files.size(); i++) {
			Page p = deserialize(t.files.get(i));
			for (int j = 0; j < p.Tuple.size(); j++) {
				Ref old = new Ref(i, j);
				Ref n = new Ref(i - 1, j);
				b.update((Integer) p.Tuple.get(j).Hash.get(cluster_key), old, n);
			}
		}

	}
	
    public static int[] get_x_axis(String text){
    	String[] words = text.split(",");
    	int [] a= new int[words.length/2];
    	int c=0;
    	for(int i=0;i<words.length;i++){
			if(i%2==0){
				a[c]=Integer.parseInt(words[i]);
				c++;
			}
			
		}
    	return a;
    }
    public static int[] get_y_axis(String text){
    	String[] words = text.split(",");
    	int [] a= new int[words.length/2];
    	int c=0;
    	for(int i=0;i<words.length;i++){
			if(i%2!=0){
				a[c]=Integer.parseInt(words[i]);
				c++;
			}
			
		}
    	return a;
    }

	public static void update_trees_after_insert(String strTableName) {
		Vector<String> all_col = get_indexed_col(strTableName);
		String cluster_key = "";
		Table table = deserializet("data/" + strTableName + "" + ".ser");
		try {
			cluster_key = getcluster(strTableName, "data/metadata.class");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int z = 0; z < all_col.size(); z++) {
			if (!all_col.get(z).equals(cluster_key)) {
				String strColName = all_col.get(z);
				String type = getcoltypeforindex(strTableName, strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				int size = read_config_file();

				if (getcoltypeforindex(strTableName, strColName).equals(
						"java.lang.Integer")) {
					BPTree<Integer> b = new BPTree<Integer>(size);
					t = deserializet("data/" + strTableName + "" + ".ser");
					for (int i = 0; i < t.files.size(); i++) {
						Page p = deserialize(t.files.get(i));
						for (int j = 0; j < p.Tuple.size(); j++) {
							int col_data = (int) p.Tuple.get(j).Hash
									.get(strColName);
							Ref refrence = new Ref(i, j);
							b.insert(col_data, refrence, strColName,strTableName);
						}
					}
					serialize_BTree(strTableName, strColName, b);
				} else if (getcoltypeforindex(strTableName, strColName).equals(
						"java.lang.String")) {
					BPTree<String> b = new BPTree<String>(size);
					t = deserializet("data/" + strTableName + "" + ".ser");
					for (int i = 0; i < t.files.size(); i++) {
						Page p = deserialize(t.files.get(i));
						for (int j = 0; j < p.Tuple.size(); j++) {
							String col_data = (String) p.Tuple.get(j).Hash
									.get(strColName);
							Ref refrence = new Ref(i, j);
							b.insert(col_data, refrence, strColName,strTableName);
						}
					}
					serialize_BTree(strTableName, strColName, b);
				} else if (getcoltypeforindex(strTableName, strColName).equals(
						"java.lang.double")) {
					BPTree<Double> b = new BPTree<Double>(size);
					t = deserializet("data/" + strTableName + "" + ".ser");
					for (int i = 0; i < t.files.size(); i++) {
						Page p = deserialize(t.files.get(i));
						for (int j = 0; j < p.Tuple.size(); j++) {
							double col_data = (double) p.Tuple.get(j).Hash
									.get(strColName);
							Ref refrence = new Ref(i, j);
							b.insert(col_data, refrence, strColName,strTableName);
						}
					}
					serialize_BTree(strTableName, strColName, b);
				} else if (getcoltypeforindex(strTableName, strColName).equals(
						"java.lang.Boolean")) {
					BPTree<Boolean> b = new BPTree<Boolean>(size);
					t = deserializet("data/" + strTableName + "" + ".ser");
					for (int i = 0; i < t.files.size(); i++) {
						Page p = deserialize(t.files.get(i));
						for (int j = 0; j < p.Tuple.size(); j++) {
							Boolean col_data = (Boolean) p.Tuple.get(j).Hash
									.get(strColName);
							Ref refrence = new Ref(i, j);
							b.insert(col_data, refrence, strColName,strTableName);
						}
					}
					serialize_BTree(strTableName, strColName, b);
				} else if (getcoltypeforindex(strTableName, strColName).equals(
						"java.util.Date")) {
					BPTree<Date> b = new BPTree<Date>(size);
					t = deserializet("data/" + strTableName + "" + ".ser");
					for (int i = 0; i < t.files.size(); i++) {
						Page p = deserialize(t.files.get(i));
						for (int j = 0; j < p.Tuple.size(); j++) {
							Date col_data = (Date) p.Tuple.get(j).Hash
									.get(strColName);
							Ref refrence = new Ref(i, j);
							b.insert(col_data, refrence, strColName,strTableName);
						}
					}
					serialize_BTree(strTableName, strColName, b);
				}
			}

		}
	}

	public static void update_trees_after_delete(String strTableName, Ref ref,
			Hashtable<String, Object> htblColNameValue, Page page) {
		Vector<String> all_col = get_indexed_col(strTableName);
		String cluster_key = "";
		Table table = deserializet("data/" + strTableName + "" + ".ser");
		try {
			cluster_key = getcluster(strTableName, "data/metadata.class");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int z = 0; z < all_col.size(); z++) {
			if (!all_col.get(z).equals(cluster_key)) {
				String strColName = all_col.get(z);
				String type = getcoltypeforindex(strTableName, strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				int size = read_config_file();
				if ((String) htblColNameValue.get(strColName) != null) {
					if (getcoltypeforindex(strTableName, strColName).equals(
							"java.lang.String")) {
						BPTree<String> b = deserialize_BTree(strTableName,
								strColName);
						b.delete_using_Ref(
								(String) htblColNameValue.get(strColName), ref);
						update_page_String(page, 1, ref.getIndexInPage(),
								ref.getPage(), b, strColName, strTableName);
						serialize_BTree(strTableName, strColName, b);

					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.lang.Integer")) {
						BPTree<Integer> b = deserialize_BTree(strTableName,
								strColName);
						b.delete_using_Ref(
								(Integer) htblColNameValue.get(strColName), ref);
						update_page(page, 1, ref.getIndexInPage(),
								ref.getPage(), b, strColName, strTableName);
						serialize_BTree(strTableName, strColName, b);

					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.lang.double")) {
						BPTree<Double> b = deserialize_BTree(strTableName,
								strColName);
						b.delete_using_Ref(
								(Double) htblColNameValue.get(strColName), ref);
						update_page_double(page, 1, ref.getIndexInPage(),
								ref.getPage(), b, strColName, strTableName);
						serialize_BTree(strTableName, strColName, b);

					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.lang.Boolean")) {
						BPTree<Boolean> b = deserialize_BTree(strTableName,
								strColName);
						b.delete_using_Ref(
								(Boolean) htblColNameValue.get(strColName), ref);
						update_page_boolean(page, 1, ref.getIndexInPage(),
								ref.getPage(), b, strColName, strTableName);
						serialize_BTree(strTableName, strColName, b);

						//
					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.util.Date")) {
						BPTree<Date> b = deserialize_BTree(strTableName,
								strColName);
						b.delete_using_Ref(
								(Date) htblColNameValue.get(strColName), ref);
						update_page_Date(page, 1, ref.getIndexInPage(),
								ref.getPage(), b, strColName, strTableName);
						serialize_BTree(strTableName, strColName, b);
					}
					 else if (getcoltypeforindex(strTableName, strColName)
								.equals("java.awt.Polygon")) {
							RTree<Double> b = deserialize_RTree(strTableName,
									strColName);
							b.delete_using_Ref(
									(Double) htblColNameValue.get(strColName), ref);
							update_page_double(page, 1, ref.getIndexInPage(),
									ref.getPage(), b, strColName, strTableName);
							serialize_RTree(strTableName, strColName, b);
						}
					
				}

			}
		}
	}
	public static void update_trees_after_update(String strTableName, Ref ref,
			Hashtable<String, Object> htblColNameValue, Page page,Hashtable<String, Object> new_hash) {
		Vector<String> all_col = get_indexed_col(strTableName);
		String cluster_key = "";
		Table table = deserializet("data/" + strTableName + "" + ".ser");
		try {
			cluster_key = getcluster(strTableName, "data/metadata.class");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int z = 0; z < all_col.size(); z++) {
			if (!all_col.get(z).equals(cluster_key)) {
				String strColName = all_col.get(z);
				String type = getcoltypeforindex(strTableName, strColName);
				Table t = deserializet("data/" + strTableName + "" + ".ser");
				int size = read_config_file();
				if ((String) htblColNameValue.get(strColName) != null) {
					if (getcoltypeforindex(strTableName, strColName).equals(
							"java.lang.String")) {
						BPTree<String> b = deserialize_BTree(strTableName,strColName);
						b.delete_using_Ref((String) new_hash.get(strColName), ref);
						b.insert((String) htblColNameValue.get(strColName),ref,"not cluster","no table");
						serialize_BTree(strTableName, strColName, b);

					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.lang.Integer")) {
						BPTree<Integer> b = deserialize_BTree(strTableName,strColName);
						b.delete_using_Ref((Integer) new_hash.get(strColName), ref);
						b.insert((Integer) htblColNameValue.get(strColName),ref,"not cluster","no table");
						serialize_BTree(strTableName, strColName, b);
						

					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.lang.double")) {
						BPTree<Double> b = deserialize_BTree(strTableName,strColName);
						b.delete_using_Ref((Double) new_hash.get(strColName), ref);
						b.insert((Double) htblColNameValue.get(strColName),ref,"not cluster","no table");
						serialize_BTree(strTableName, strColName, b);

					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.lang.Boolean")) {
						BPTree<Boolean> b = deserialize_BTree(strTableName,strColName);
						b.delete_using_Ref((Boolean) new_hash.get(strColName), ref);
						b.insert((Boolean) htblColNameValue.get(strColName),ref,"not cluster","no table");
						serialize_BTree(strTableName, strColName, b);

						//
					} else if (getcoltypeforindex(strTableName, strColName)
							.equals("java.util.Date")) {
						BPTree<Date> b = deserialize_BTree(strTableName,strColName);
						b.delete_using_Ref((Date) new_hash.get(strColName), ref);
						b.insert((Date) htblColNameValue.get(strColName),ref,"not cluster","no table");
						serialize_BTree(strTableName, strColName, b);
					}
					 else if (getcoltypeforindex(strTableName, strColName)
								.equals("java.awt.Polygon")) {
							RTree<String> b = deserialize_RTree(strTableName,strColName);
							b.delete_using_Ref((String) new_hash.get(strColName), ref);
							b.insert((String) htblColNameValue.get(strColName),ref,"not cluster","no table");
							serialize_RTree(strTableName, strColName, b);
						}
					
				}

			}
		}
	}
	
	
	
	
}
