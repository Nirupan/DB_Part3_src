package edu.buffalo.cse.sql.hardcoded_indexes;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.index.GenericIndexKeySpec;
import edu.buffalo.cse.sql.index.HashIndex;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.interpret.ScanInterpreter;

public class Orders_orderdate {
	
	public static void main(String args[]) {

		Map<String, Schema.TableFromFile> tables = new HashMap<String, Schema.TableFromFile>();
/*
		orderkey int,
		  custkey int,
		  orderstatus string,
		  totalprice float,
		  orderdate date,
		  orderpriority string,
		  clerk string,
		  shippriority int,
		  comment string

*/		
		
		Schema.TableFromFile table_S = new Schema.TableFromFile(new File("orders.tbl"));
		table_S.add(new Schema.Column("orders", "orderkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("orders", "custkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("orders", "orderstatus", Schema.Type.STRING));
	    table_S.add(new Schema.Column("orders", "totalprice", Schema.Type.FLOAT));
	    table_S.add(new Schema.Column("orders", "orderdate", Schema.Type.DATE));
	    table_S.add(new Schema.Column("orders", "orderpriority", Schema.Type.STRING));
	    table_S.add(new Schema.Column("orders", "clerk", Schema.Type.STRING));
	    table_S.add(new Schema.Column("orders", "shippriority", Schema.Type.INT));
	    table_S.add(new Schema.Column("orders", "comment", Schema.Type.STRING));
	    
	    tables.put("orders",table_S);
	    ScanInterpreter.readData(tables);
		
	    List<Datum[]> ls = ScanInterpreter.mpScanned.get("orders");
	    System.out.println("Size of ls = "+ls.size());
	   
	    /**p1**/
	    Iterator<Datum[]> dataSource = ls.iterator();
	    
	    /**p2**/
	    BufferManager bm = new BufferManager(1024);
	    FileManager fm = new FileManager(bm);
	    
	    /**p3**/
	    File path = new File("C:\\Users\\Adi\\UB1\\My_spring_2013\\Databases\\DBProject" +
	    		"\\DB_Part3\\src\\edu\\buffalo\\cse\\sql\\indexedfiles\\orders_orderdate.dat");
	    
	    /**p4**/
	    int[] keyCols = new int[1];
	    keyCols[0]=4;
	    
	    IndexKeySpec key = new GenericIndexKeySpec(getSchema(8,1), keyCols);
	    
	    /**p5**/
	    int directorySize=10;
	    
			ISAMIndex.create(fm, path, dataSource, key);
	    
	}
	
	public static Schema.Type[] getSchema(int values,int keys)
	  {
		int[] curr = new int[keys];
	    int cols = values + curr.length;
	    Schema.Type[] sch = new Schema.Type[cols];
	    for(int i = 0; i < cols; i++){ 
	    	
	    	if(i==0 || i==1 || i==4 || i==7)
	    		sch[i] = Schema.Type.INT; 
	    	if(i==2 || i==5 || i==6 || i==8)
	    		sch[i] = Schema.Type.STRING; 
	    	if(i==3)
	    		sch[i] = Schema.Type.FLOAT;
	    	
	    
	    }
	    return sch;
	  }
	  


}
