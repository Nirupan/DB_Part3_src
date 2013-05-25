package edu.buffalo.cse.sql.interpret;

import java.util.Iterator;
import java.util.List;
import edu.buffalo.cse.sql.data.Datum;

public class RelationReader {

	public static void readRelation(List<Datum[]> datumList) {

		Iterator itr = datumList.iterator();	
		while(itr.hasNext()){			
			Datum[] arrayDatum=(Datum[])itr.next();
			for(int i=0; i<arrayDatum.length; i++){
				System.out.print(arrayDatum[i]+" ");
			}
			System.out.println();
		}
	}
}