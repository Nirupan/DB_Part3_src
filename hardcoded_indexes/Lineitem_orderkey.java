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
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.interpret.ScanInterpreter;

public class Lineitem_orderkey {

	public static void main(String args[]) {
		
		/*
		orderkey int,
		  partkey int,
		  suppkey int,
		  linenumber int,
		  quantity float,
		  extendedprice float,
		  discount float,
		  tax float,
		  returnflag string,
		  linestatus string,
		  shipdate date,
		  commitdate date,
		  receiptdate date,
		  shipinstruct string,
		  shipmode string,
		  comment string
		*/ 

		Map<String, Schema.TableFromFile> tables = new HashMap<String, Schema.TableFromFile>();
		Schema.TableFromFile table_S = new Schema.TableFromFile(new File("lineitem.tbl"));
		table_S.add(new Schema.Column("lineitem", "orderkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("lineitem", "partkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("lineitem", "suppkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("lineitem", "linenumber", Schema.Type.INT));
	    table_S.add(new Schema.Column("lineitem", "quantity", Schema.Type.FLOAT));
	    table_S.add(new Schema.Column("lineitem", "extendedprice", Schema.Type.FLOAT));
	    table_S.add(new Schema.Column("lineitem", "discount", Schema.Type.FLOAT));
	    table_S.add(new Schema.Column("lineitem", "tax", Schema.Type.FLOAT));
	    table_S.add(new Schema.Column("lineitem", "returnflag", Schema.Type.STRING));
	    table_S.add(new Schema.Column("lineitem", "linestatus", Schema.Type.STRING));
	    table_S.add(new Schema.Column("lineitem", "shipdate", Schema.Type.DATE));
	    table_S.add(new Schema.Column("lineitem", "commitdate", Schema.Type.DATE));
	    table_S.add(new Schema.Column("lineitem", "receiptdate", Schema.Type.DATE));
	    table_S.add(new Schema.Column("lineitem", "shipinstruct", Schema.Type.STRING));
	    table_S.add(new Schema.Column("lineitem", "shipmode", Schema.Type.STRING));
	    table_S.add(new Schema.Column("lineitem", "comment", Schema.Type.STRING));
	    

	    tables.put("lineitem",table_S);
	    ScanInterpreter.readData(tables);
		
	    List<Datum[]> ls = ScanInterpreter.mpScanned.get("lineitem");
	    System.out.println("Size of ls = "+ls.size());
	   
	    /**p1**/
	    Iterator<Datum[]> dataSource = ls.iterator();
	    
	    /**p2**/
	    BufferManager bm = new BufferManager(1024);
	    FileManager fm = new FileManager(bm);
	    
	    /**p3**/
	    File path = new File("C:\\Users\\Adi\\UB1\\My_spring_2013\\Databases\\DBProject" +
	    		"\\DB_Part3\\src\\edu\\buffalo\\cse\\sql\\indexedfiles\\lineitem_orderkey.dat");
	    
	    /**p4**/
	    int[] keyCols = new int[1];
	    keyCols[0]=0;
	    
	    IndexKeySpec key = new GenericIndexKeySpec(getSchema(15,1), keyCols);
	    
	    /**p5**/
	    int directorySize=10;
	    
			HashIndex.create(fm, path, dataSource, key, directorySize);
		
	}
	
	public static Schema.Type[] getSchema(int values,int keys)
	  {
		int[] curr = new int[keys];
	    int cols = values + curr.length;
	    Schema.Type[] sch = new Schema.Type[cols];
	    for(int i = 0; i < cols; i++){ 
	    
	    	/*
			  0-orderkey int,
			  1-partkey int,
			  2-suppkey int,
			  3-linenumber int,
			  4-quantity float,
			  5-extendedprice float,
			  6-discount float,
			  7-tax float,
			  8-returnflag string,
			  9-linestatus string,
			  10-shipdate date,
			  11-commitdate date,
			  12-receiptdate date,
			  13-shipinstruct string,
			  14-shipmode string,
			  15-comment string
			*/ 

	    	
	    	
	    	if(i==0 || i==1 ||  i==2 || i==3 || i==10 || i==11 || i==12)
	    		sch[i] = Schema.Type.INT; 
	    	if(i==8 || i==9 || i==13 || i==14 || i==15)
	    		sch[i] = Schema.Type.STRING; 
	    	if(i==4 || i==5 || i==6 || i==7)
	    		sch[i] = Schema.Type.FLOAT;
	    
	    }
	    return sch;
	  }

}
