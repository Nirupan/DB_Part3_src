package edu.buffalo.cse.sql.interpret;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.buffalo.cse.sql.data.Datum;


public class SortMergeJoin {

	@SuppressWarnings("null")
	public static void SortMerge(List<Datum[]> list1, List<Datum[]> list2) {
		System.out.println("PATH :: Sort merger");
		// List<Datum[]> a1=new ArrayList<Datum[]>();
		// List<Datum[]> a2=new ArrayList<Datum[]>();

		// List<Datum[]> list1=new ArrayList<Datum[]>();
		// List<Datum[]> list2=new ArrayList<Datum[]>();

		List<String> iKey1 = null;

		// List<String> iKey2 = null;

		int ik1 = 0, ik2 = 0;

		Datum[] d1 = null;

		Datum[] d2 = null;

		// list1.add(0, "5,6");

		int s1 = list1.size();
		int s2 = list2.size();

		String[] str1 = new String[s1 - 2];

		String[] str2 = new String[s2 - 2];

		Map<String, Datum> mapKeyRow3 = new HashMap<String, Datum>();

		Map<String, Datum> mapKeyRow4 = new HashMap<String, Datum>();

		Iterator itr1 = list1.iterator();

		System.out.println("List1 values");
		// while (itr1.hasNext()) {
		// Datum[] temp = (Datum[]) itr1.next();

		for (int k1 = 2; k1 < s1; k1++) {

			Datum[] temp = list1.get(k1);

			for (int q1 = 0; q1 < temp.length; q1++) {
				// System.out.print("index=" + q1 + " value=" + temp[q1]);
				// String str1=temp[q1].toString();

			}
			// System.out.println();

			System.out.println(temp[1].toString());

			str1[ik1] = temp[1].toString();

			// System.out.println("str1[ik1] is"+str1[ik2]+"temp is"+temp);

			mapKeyRow3.put(str1[ik1], temp[0]);

			System.out.println("ik1 value is" + ik1);

			// System.out.println("1 is "+mapKeyRow3.get(ik1));

			ik1++;

			// System.out.println("string is"+str1);

			// System.out.println("String elements are");

			// iKey1.add(q1, temp[q1].toString());

			// System.out.println(list1.get(k1));

			// iKey1=list1.get(k1);
		}

		System.out.println("List2 values");

		System.out.println("Index 1 values");
		for (int k1 = 2; k1 < s2; k1++) {

			Datum[] temp2 = list2.get(k1);
			for (int q1 = 0; q1 < temp2.length; q1++) {
				// System.out.print("index=" + q1 + " value=" + temp2[q1]);

			}
			// System.out.println();

			System.out.println(temp2[1].toString());

			str2[ik2] = temp2[1].toString();

			System.out.println("Temp val in str form");

			System.out.println("temp2 is in str form " + temp2[0]);

			// System.out.println("str1[ik1] is"+str1[ik2]+"temp is"+temp2);

			mapKeyRow4.put(str2[ik2], temp2[0]);

			System.out.println("ik2 value is" + ik2);

			// System.out.println("2 is "+mapKeyRow4.get(ik2));

			ik2++;

			System.out.println();
			// List<String> iKey2 = temp2[1].toString();
			// iKey2.add(temp2[1].toString());
			// System.out.println("Display iKey element");
			// System.out.println(iKey1.get(0));

			/*
			 * 
			 * iKey1.add(ik1, temp2[1].toString());
			 * 
			 * System.out.println("Display iKey element");
			 * System.out.println(iKey1.get(ik1)); ik1++;
			 */

			// System.out.println(list2.get(k1));

			// iKey1=list1.get(k1);
		}

		/*
		 * System.out.println("The string1 values are"); for(int
		 * y=0;y<str1.length;y++) { System.out.println(str1[y]); }
		 * 
		 * 
		 * System.out.println("The string2 values are"); for(int
		 * y=0;y<str2.length;y++) { System.out.println(str2[y]); }
		 */

		System.out.println("Sorted set 1");

		System.out.println("Key set" + mapKeyRow3.keySet());

		System.out.println("Value set" + mapKeyRow3.values());

		/*
		 * 
		 * HashMap map = new LinkedHashMap();
		 * 
		 * 
		 * 
		 * List yourMapKeys = new ArrayList(mapKeyRow3.keySet()); List
		 * yourMapValues = new ArrayList(mapKeyRow3.values()); TreeSet sortedSet
		 * = new TreeSet(yourMapValues); Object[] sortedArray =
		 * sortedSet.toArray(); int size = sortedArray.length;
		 * 
		 * for (int i=0; i<size; i++) { map.put
		 * (yourMapKeys.get(yourMapValues.indexOf(sortedArray[i])),
		 * sortedArray[i]); } //To iterate your new Sorted Map Set ref =
		 * map.keySet(); Iterator it = ref.iterator();
		 * 
		 * while (it.hasNext()) { String file = (String)it.next(); }
		 */

		SortedSet<String> keys = new TreeSet<String>(mapKeyRow3.keySet());
		// System.out.println("Size of keys"+keys.size());
		for (String key : keys) {
			// Datum[] value = mapKeyRow3.get(key);

			System.out.println(mapKeyRow3.get(key));
			// do something
		}

		System.out.println("Sorted set 2");

		System.out.println("Key set" + mapKeyRow4.keySet());

		System.out.println("Value set" + mapKeyRow4.values().toString());

		SortedSet<String> keys2 = new TreeSet<String>(mapKeyRow4.keySet());
		// System.out.println("Size of keys"+keys.size());
		for (String key2 : keys2) {
			// Datum[] value = mapKeyRow3.get(key);

			System.out.println(mapKeyRow4.get(key2));
			// do something
		}

		Set ss = mapKeyRow3.keySet();
		TreeMap ts = new TreeMap(mapKeyRow3);
		Iterator it = ss.iterator();

		int i = 0, j = 0;

		String s11, s22;

		// mapKeyRow.put(key, value)

		int c1 = 0;

		while (c1 < s1) {

			// mapKeyRow.put(iKey1.get(c1), list1.get(c1));
			c1++;
		}

		/*
		 * System.out.println("Some new shit"); Object[] key =
		 * mapKeyRow3.keySet().toArray(); Arrays.sort(key); for (int i1 = 0; i1
		 * < key.length; i1++) { System.out.println(mapKeyRow3.get(key[i1])); }
		 */

		int c2 = 0;

		while (c2 < s2) {

			// mapKeyRow.put(iKey2.get(c2), list2.get(c2));
			c2++;
		}

		/*
		 * System.out.println("Some new shit part 2"); Object[] key2 =
		 * mapKeyRow4.keySet().toArray(); Arrays.sort(key2); for (int i2 = 0; i2
		 * < key2.length; i2++) { System.out.println(mapKeyRow4.get(key2[i2]));
		 * }
		 */

		System.out.println("The final values");

		for (String key : keys) {
			for (String key2 : keys2) {

				/*
				 * System.out.println("key1 is"+key);
				 * 
				 * System.out.println("key2 is"+key2);
				 * 
				 * 
				 * System.out.println("s11 is"+mapKeyRow3.get(key));
				 * 
				 * System.out.println("s22 is"+mapKeyRow4.get(key2));
				 */

				if (key.compareTo(key2) == 0) {
					System.out.println("tuple 1 val is" + "\t"
							+ mapKeyRow3.get(key) + "\t" + "Key value is"
							+ "\t" + key + "\t" + "\t" + "tuple 2 val is"
							+ "\t" + mapKeyRow4.get(key2));

				}

			}
		}

		/*
		 * while (i <= s1 && j <= s2) {
		 * 
		 * s11 = mapKeyRow3.get(i).toString();
		 * 
		 * s22 = mapKeyRow4.get(j).toString();
		 * 
		 * System.out.println("s11 is"+s11);
		 * 
		 * System.out.println("s22 is"+s22);
		 * 
		 * 
		 * /* if(mapKeyRow.get(key[i])==mapKeyRow2.get(key2[j])) {
		 * System.out.println(mapKeyRow.get(key[i])); i++; j++; }
		 */

		/*
		 * if (s11.compareTo(s22) == 0) {
		 * 
		 * System.out.println("equal value"+mapKeyRow3.get(i)); i++; j++;
		 * 
		 * }
		 * 
		 * else if (s11.compareTo(s22) < 0) { i++; }
		 * 
		 * else if (s11.compareTo(s22) > 0) { j++; }
		 * 
		 * /* else if((mapKeyRow.get(key[i]).toString() !=
		 * mapKeyRow2.get(key2[j]).toString())) { i++; }
		 * 
		 * else if(mapKeyRow.get(key[i])>mapKeyRow2.get(key2[j])) { j++;
		 * 
		 * }
		 */

		// }

	}

}
