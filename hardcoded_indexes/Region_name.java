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

public class Region_name {
	
	public static void main(String args[]) {

		Map<String, Schema.TableFromFile> tables = new HashMap<String, Schema.TableFromFile>();
		Schema.TableFromFile table_S = new Schema.TableFromFile(new File("region.tbl"));
		table_S.add(new Schema.Column("region", "nationkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("region", "name", Schema.Type.STRING));
	    table_S.add(new Schema.Column("region", "regionkey", Schema.Type.INT));
	    table_S.add(new Schema.Column("region", "comment", Schema.Type.STRING));
	    tables.put("region",table_S);
	    ScanInterpreter.readData(tables);
		
	    List<Datum[]> ls = ScanInterpreter.mpScanned.get("region");
	    System.out.println("Size of ls = "+ls.size());
	   
	    /**p1**/
	    Iterator<Datum[]> dataSource = ls.iterator();
	    
	    /**p2**/
	    BufferManager bm = new BufferManager(5000);
	    FileManager fm = new FileManager(bm);
	    
	    /**p3**/
	    File path = new File("C:\\Users\\Adi\\UB1\\My_spring_2013\\Databases\\DBProject" +
	    		"\\DB_Part3\\src\\edu\\buffalo\\cse\\sql\\indexedfiles\\region_name.dat");
	    
	    /**p4**/
	    int[] keyCols = new int[1];
	    keyCols[0]=1;
	    
	    IndexKeySpec key = new GenericIndexKeySpec(getSchema(7,1), keyCols);
	    
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
	    	
	    	if(i==0 || i==2)
	    		sch[i] = Schema.Type.INT; 
	    	if(i==1 || i==3)
	    		sch[i] = Schema.Type.STRING; 
	    	
	    
	    }
	    return sch;
	  }


}
