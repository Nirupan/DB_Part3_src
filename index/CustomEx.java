package edu.buffalo.cse.sql.index;

public class CustomEx extends Exception{

	public static void showError(Exception ex){
		if(ex instanceof IndexOutOfBoundsException){
			System.out.println("index out");
		}
		//ex.printStackTrace();
		System.out.println("MESSAGE ->"+ex.getMessage());
		System.out.println("CAUSE ->"+ex.getCause());
	}
}
