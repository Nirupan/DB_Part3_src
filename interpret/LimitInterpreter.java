package edu.buffalo.cse.sql.interpret;

import java.util.List;

import edu.buffalo.cse.sql.data.Datum;

public class LimitInterpreter {

	
	public static void interpretLimit(List<Datum[]> datumList, int lt)
	{

	System.out.println("Size of datumList is"+ datumList.size());
			
		    int s1,s2;


	s1=datumList.size();
	s2=lt;

	for(int i=0;i<s2;i++)
	{

		System.out.println(datumList.toString());
	}

}

}