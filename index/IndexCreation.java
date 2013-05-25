package edu.buffalo.cse.sql.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.hardcoded_indexes.*;


public class IndexCreation {

	public static void createIndexes(String query){
		System.out.println("PATH :: got query = "+query);

		Map<String,List<String>> map_class_from_file = new HashMap<String,List<String>>();

		/**FOR TPCH_Q1**/
		List<String> list_q1 = new ArrayList<String> ();
		list_q1.add("//hardcoded_indexes//Lineitem_shipdate.java");
		map_class_from_file.put("test/TPCH_Q1.SQL", list_q1);

		/**FOR TPCH_Q3**/
		List<String> list_q3 = new ArrayList<String> ();
		list_q3.add("//hardcoded_indexes//Customer_custkey.java");
		list_q3.add("//hardcoded_indexes//Lineitem_orderkey.java");
		list_q3.add("//hardcoded_indexes//Orders_orderdate.java");
		list_q3.add("//hardcoded_indexes//Lineitem_shipdate.java");
		list_q3.add("//hardcoded_indexes//Customer_mktsegment.java");
		map_class_from_file.put("test/TPCH_Q3.SQL", list_q3);

		/**FOR TPCH_Q5**/
		List<String> list_q5 = new ArrayList<String> ();
		list_q5.add("//hardcoded_indexes//Customer_custkey.java");
		list_q5.add("//hardcoded_indexes//Lineitem_orderkey.java");
		list_q5.add("//hardcoded_indexes//Lineitem_suppkey.java");
		list_q5.add("//hardcoded_indexes//Customer_nationkey.java");
		list_q5.add("//hardcoded_indexes//Supplier_nationkey.java");
		list_q5.add("//hardcoded_indexes//Nation_regionkey.java");
		list_q5.add("//hardcoded_indexes//Orders_orderdate.java");
		list_q5.add("//hardcoded_indexes//Region_name.java");
		map_class_from_file.put("test/TPCH_Q5.SQL", list_q5);

		
		/**FOR TPCH_Q6**/
		List<String> list_q6 = new ArrayList<String> ();
		list_q6.add("//hardcoded_indexes//Lineitem_shipdate.java");
		list_q6.add("//hardcoded_indexes//Lineitem_discount.java");
		list_q6.add("//hardcoded_indexes//Lineitem_quantity.java");
		map_class_from_file.put("test/TPCH_Q6.SQL", list_q6);

		/**FOR TPCH_Q10**/
		List<String> list_q10 = new ArrayList<String> ();
		list_q10.add("//hardcoded_indexes//Customer_custkey.java");
		list_q10.add("//hardcoded_indexes//Lineitem_orderkey.java");
		list_q10.add("//hardcoded_indexes//Orders_orderdate.java");
		list_q10.add("//hardcoded_indexes//Customer_nationkey.java");
		list_q10.add("//hardcoded_indexes//Lineitem_returnflag.java");
		map_class_from_file.put("test/TPCH_Q10.SQL", list_q10);

		/**FOR TPCH_Q19**/
		List<String> list_q19 = new ArrayList<String> ();
		list_q19.add("//hardcoded_indexes//Part_partkey.java");
		list_q19.add("//hardcoded_indexes//Lineitem_quantity.java");
		list_q19.add("//hardcoded_indexes//Part_size.java");
		list_q19.add("//hardcoded_indexes//Part_brand.java");
		list_q19.add("//hardcoded_indexes//Lineitem_shipmode.java");
		list_q19.add("//hardcoded_indexes//Lineitem_shipinstruct.java");
		list_q19.add("//hardcoded_indexes//Part_container.java");
		map_class_from_file.put("test/TPCH_Q19.SQL", list_q19);
		
		List<String> list_run_time = new ArrayList<String> ();
		list_run_time = map_class_from_file.get(query);

		for(int i=0;i<list_run_time.size();i++)
		{
			switch(list_run_time.get(i)){			
			case "//hardcoded_indexes//Customer_custkey.java":
				System.out.println("starting //hardcoded_indexes//Customer_custkey.java");
				Customer_custkey.main(null);
				break;
			case "//hardcoded_indexes//Customer_mktsegment.java":
				System.out.println("starting //hardcoded_indexes//Customer_mktsegment.java");
				Customer_custkey.main(null);
				break;
			case "//hardcoded_indexes//Customer_nationkey.java":
				System.out.println("starting //hardcoded_indexes//Customer_nationkey.java");
				Customer_nationkey.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_discount.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_discount.java");
				Lineitem_discount.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_orderkey.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_orderkey.java");
				Lineitem_orderkey.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_quantity.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_quantity.java");
				Lineitem_quantity.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_returnflag.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_returnflag.java");
				Customer_nationkey.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_shipdate.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_shipdate.java");
				Lineitem_shipdate.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_shipinstruct.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_shipinstruct.java");
				Lineitem_shipdate.main(null);
				break;
			case "//hardcoded_indexes//Lineitem_shipmode.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_shipmode.java");
				Lineitem_shipdate.main(null);
				break;	
			case "//hardcoded_indexes//Lineitem_suppkey.java":
				System.out.println("starting //hardcoded_indexes//Lineitem_suppkey.java");
				Lineitem_suppkey.main(null);
				break;
			case "//hardcoded_indexes//Nation_regionkey.java":
				System.out.println("starting //hardcoded_indexes//Nation_regionkey.java");
				Nation_regionkey.main(null);
				break;
			case "//hardcoded_indexes//Orders_orderdate.java":
				System.out.println("starting //hardcoded_indexes//Orders_orderdate.java");
				Orders_orderdate.main(null);
				break;
			case "//hardcoded_indexes//Part_brand.java":
				System.out.println("starting //hardcoded_indexes//Part_brand.java");
				Customer_nationkey.main(null);
				break;
			case "//hardcoded_indexes//Part_container.java":
				System.out.println("starting //hardcoded_indexes//Part_container.java");
				Customer_nationkey.main(null);
				break;
			case "//hardcoded_indexes//Part_partkey.java":
				System.out.println("starting //hardcoded_indexes//Part_partkey.java");
				Part_partkey.main(null);
				break;
			case "//hardcoded_indexes//Part_size.java":
				System.out.println("starting //hardcoded_indexes//Part_size.java");
				Part_size.main(null);
				break;
			case "//hardcoded_indexes//Region_name.java":
				System.out.println("starting //hardcoded_indexes//Region_name.java");
				Customer_nationkey.main(null);
				break;
			case "//hardcoded_indexes//Supplier_nationkey.java":
				System.out.println("starting //hardcoded_indexes//Supplier_nationkey.java");
				Customer_nationkey.main(null);
				break;
			default : System.out.println("Invalid value");
				break;
			}
			
		}

		}

}
