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

public class Part_container {
	
public static void main(String args[]) {
		
		/*
  0-partkey int,
  1-name string,
  2-mfgr string,
  3-brand string,
  4-type string,
  5-size int,
  6-container string,
  7-retailprice float,
  8-comment string

		*/ 

		Map<String, Schema.TableFromFile> tables = new HashMap<String, Schema.TableFromFile>();
		Schema.TableFromFile table_S = new Schema.TableFromFile(new File("part.tbl"));
		table_S.add(new Schema.Column("part", "partkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("part", "name", Schema.Type.STRING));
	    table_S.add(new Schema.Column("part", "mfgr", Schema.Type.STRING));
	    table_S.add(new Schema.Column("part", "brand", Schema.Type.STRING));
	    table_S.add(new Schema.Column("part", "type", Schema.Type.STRING));
	    table_S.add(new Schema.Column("part", "size", Schema.Type.INT));
	    table_S.add(new Schema.Column("part", "container", Schema.Type.STRING));
	    table_S.add(new Schema.Column("part", "retailprice", Schema.Type.FLOAT));
	    table_S.add(new Schema.Column("part", "comment", Schema.Type.STRING));
	    

	    tables.put("part",table_S);
	    ScanInterpreter.readData(tables);
		
	    List<Datum[]> ls = ScanInterpreter.mpScanned.get("part");
	    System.out.println("Size of ls = "+ls.size());
	   
	    /**p1**/
	    Iterator<Datum[]> dataSource = ls.iterator();
	    
	    /**p2**/
	    BufferManager bm = new BufferManager(1024);
	    FileManager fm = new FileManager(bm);
	    
	    /**p3**/
	    File path = new File("C:\\Users\\Adi\\UB1\\My_spring_2013\\Databases\\DBProject" +
	    		"\\DB_Part3\\src\\edu\\buffalo\\cse\\sql\\indexedfiles\\part_container.dat");
	    
	    /**p4**/
	    int[] keyCols = new int[1];
	    keyCols[0]=6;
	    
	    IndexKeySpec key = new GenericIndexKeySpec(getSchema(8,1), keyCols);
	    
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
	    	  0-partkey int,
  			  1-name string,
  			  2-mfgr string,
  			  3-brand string,
  			  4-type string,
  			  5-size int,
  			  6-container string,
  			  7-retailprice float,
  			  8-comment string

			*/ 

	    	
	    	
	    	if(i==0 || i==5)
	    		sch[i] = Schema.Type.INT; 
	    	if(i==1 || i==2 || i==3 || i==4 || i==6)
	    		sch[i] = Schema.Type.STRING; 
	    	if(i==7)
	    		sch[i] = Schema.Type.FLOAT; 
	    	
	    }
	    return sch;
	  }


}
