
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Type;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;
 
public class HashIndex implements IndexFile {
  
  public ManagedFile file;
  public IndexKeySpec keySpec;
  public static Map mpIsOverFlowLinkCreated = new HashMap();
  public static int iExtraPage=1;

  public HashIndex(ManagedFile file, IndexKeySpec keySpec)
    throws IOException, SqlException
  {
	  this.file=file;
	  this.keySpec=keySpec;
  }
  
  public static HashIndex create(FileManager fm,File path,Iterator<Datum[]> dataSource,IndexKeySpec key,int directorySize)
	{
	  mpIsOverFlowLinkCreated.clear();
	  iExtraPage=1;	  
	  for(int i=0; i<directorySize;i++){
		  mpIsOverFlowLinkCreated.put(i, false);
	  }
	  int iExtraPage = 1;
	  int iBucketIndex=0;
		try {
			if(path.exists()){
			System.out.println("deleting old file.....");
				path.delete();
			}
			
			/**get the key length in columns**/
			int iKeyLength = key.keySchema().length;
			
			/**put record in bucket**/
			ManagedFile mngFile = fm.open(path);
			mngFile.ensureSize(directorySize + iExtraPage);	
			System.out.println("directorySize + iExtraPage = "+(directorySize + iExtraPage));
			System.out.println("mngFile.size() = "+mngFile.size());
			for (int i=0; i<mngFile.size();i++){
				ByteBuffer buffer = mngFile.pin(i);
				//ByteBuffer buffer = mngFile.pin(i);	
				DatumBuffer db = new DatumBuffer(buffer, key.rowSchema());
				db.initialize(4);
				DatumSerialization.write(buffer, 0, new Datum.Int(-1));
				mngFile.unpin(i, true);
				System.out.println("written");
			}
			
			/**put directory size permanently in buffer 0**/
			//ByteBuffer bb =  mngFile.getBuffer(0);
			Datum[] dirSize = new Datum[1];
			dirSize[0] = new Datum.Int(directorySize);
			ByteBuffer bb =	mngFile.pin(0);
			DatumBuffer db = new DatumBuffer(bb, key.rowSchema());
			db.write(dirSize);
			mngFile.unpin(0, true);
			System.out.println("Written to 0, directory size of "+dirSize);
			
			while (dataSource.hasNext()) {
				Datum[] temp = dataSource.next();
				
				/**aditya :: commented for 3**/
				/*Datum[] datKeyCols = new Datum[iKeyLength];
				for(int a=0;a < iKeyLength;a++){
					datKeyCols[a] = temp[a];
				}*/
				
				/**printing record**/
				Datum Singletemp=null;
				System.out.println(temp[0]);
				
				/**compute hash key**/
				ByteBuffer bbBucket=null;
				try{
				if(temp[0].toString().contains("'")){
					throw new IndexOutOfBoundsException();
				}
					/**this is added**/
				Datum[] datKeyCols = key.createKey(temp);
				//System.out.println("SIZEEE of datKeyCols =============="+datKeyCols.length+" "+datKeyCols[0]);
				//-----
				
				int hKey = key.hashKey(datKeyCols);
				Datum[] hashedKey = new Datum[1];
				hashedKey[0]=new Datum.Int(hKey);
				//System.out.println("Computed hash key = "+hKey);
				
				/**determine bucket number by taking modulus**/
				int iBucketNum = hKey % directorySize;
				iBucketIndex = iBucketNum+1;
				//System.out.println("Determined Bucket number = "+iBucketIndex);
				
				/**put record in bucket**/
				/**previous**/
				//	bbBucket =  mngFile.pin(page).getBuffer(iBucketIndex);
					
					/**new**/
					
				}catch(Exception ex){
					/**this indicates that iBucketIndex may not be within range
					 * this happens 2 times for starting 2 rows since we are storing column name and table name there**/
					System.out.println("CAUGHT HERE .... processed");
					CustomEx.showError(ex);
					continue;
				}
				DatumBuffer dbBucket=null;
				try{
					//bbBucket =  mngFile.getBuffer(iBucketIndex);
					bbBucket = mngFile.pin(iBucketIndex);
					dbBucket = new DatumBuffer(bbBucket, key.rowSchema());
					
				dbBucket.write(temp);
				mngFile.unpin(iBucketIndex,true);
				}catch(InsufficientSpaceException e){
			//		System.out.println("PATH ->>>>>>>>>>>>>");
					
					mngFile.unpin(iBucketIndex,true);

				boolean retVal=	WriteToOverFlowPage(iBucketIndex,directorySize,temp,dbBucket,key,
							 mngFile);
				
				}
			}
			
		//	System.out.println();
			/**new**/
			//for (int i=0; i<mngFile.size();i++){
			//	mngFile.dirty(i);
			//}
			fm.close(path);	
			HashIndex hIndex = new HashIndex(mngFile, key);			
			return hIndex;			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SqlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
  
static boolean WriteToOverFlowPage(int iBucketIndex,int directorySize, Datum[] temp, DatumBuffer dbBucket, IndexKeySpec key,
		 ManagedFile mngFile){
		
		try{	
		/**to check if page was already overflown in past**/
		if(mpIsOverFlowLinkCreated.get(iBucketIndex) == null){
			mpIsOverFlowLinkCreated.put(iBucketIndex,false);						
		}
		/**this is the first overflow page**/
	//	if(false == (boolean)mpIsOverFlowLinkCreated.get(iBucketIndex)){
			
		//	ByteBuffer buffe = mngFile.getBuffer(iBucketIndex);
			ByteBuffer buffe =	mngFile.pin(iBucketIndex);
			Datum dd=DatumSerialization.read(buffe, 0,Type.INT);
			mngFile.unpin(iBucketIndex);
		//	System.out.println(" rec= "+d.toInt());
			
		//	System.out.println("-iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii "+dd.toInt());
			int m=dd.toInt();
			if(m==-1){
			
			/**need to create an overflow page - so add extra page**/
			iExtraPage++;	
			
			/**update map of overflow links**/
			mpIsOverFlowLinkCreated.remove(iBucketIndex);
			mpIsOverFlowLinkCreated.put(iBucketIndex,true);

			/**increase manage file size by 1**/
			mngFile.ensureSize(directorySize+iExtraPage);

			/**decide page to be written**/
			int iPageToBeWritten = directorySize + iExtraPage-1;

		//	System.out.println("mmmmmmmmmmmmmmmmmmmmmmmmmmm "+iPageToBeWritten);
	//		System.out.println("mmmmmmmmmmmmmmmmmmmmmmmmmmm "+mngFile.size());

			
			/**decide value for overflow page identification and put link to overflow page**/
			//System.out.println("OVERFLOW PAGE NO. CREATED NEWLY= "+(iPageToBeWritten));
			//ByteBuffer buff = mngFile.getBuffer(iBucketIndex);
			ByteBuffer buff = mngFile.pin(iBucketIndex);
			DatumSerialization.write(buff, 0, new Datum.Int(iPageToBeWritten));
			mngFile.unpin(iBucketIndex,true);
			
			/**put the data in new overflow bucket created**/
			//ByteBuffer overflowBuffer =  mngFile.getBuffer(iPageToBeWritten);
			/**new **/
			ByteBuffer overflowBuffer = mngFile.pin(iPageToBeWritten);
			DatumBuffer overflowBucket = new DatumBuffer(overflowBuffer, key.rowSchema());
			overflowBucket.initialize(4);
			DatumSerialization.write(overflowBuffer, 0, new Datum.Int(-1));
			
			
			try{
			overflowBucket.write(temp);
			}catch(InsufficientSpaceException e){
			//	System.out.println("000000000000000000000000000000000000000000000000000000000000000000");
			//	e.printStackTrace();
			}
			/**new**/
			mngFile.unpin(iPageToBeWritten,true);
		//	System.out.println("ACTUAL PAGE = "+iBucketIndex);
		//	System.out.println("WRITTEN OVERFLOW PAGE !!"+iPageToBeWritten);
			return true;
			
		}else{			
			/**decide page to be written**/
			int iPageToBeWritten = -1;

			/** reading last entry of main page**/
		//	Datum[] bktEntry = dbBucket.read(dbBucket.length() - 1);
		//	System.out.println();
			
		//	ByteBuffer buff = mngFile.getBuffer(iBucketIndex);
		//	Datum inext = DatumSerialization.read(buff, 0, Type.INT);
		//	System.out.println("inext = == == === "+inext);
			
			/*for (int yy = 0; yy < bktEntry.length; yy++) {
				try{
				if (bktEntry[yy].toInt() == Integer.MIN_VALUE) {
					iPageToBeWritten = bktEntry[yy + 1].toInt();
				//	System.out.println("NEXT PAGE LINK -> " + iPageToBeWritten);
					break;
				}
				}catch (CastError c){
					System.out.println("------------------------------------------error");
					System.out.println(bktEntry[yy+1]);
				}
				
			}
			*/
			//ByteBuffer buffer = mngFile.getBuffer(iBucketIndex);
			ByteBuffer buffer = mngFile.pin(iBucketIndex);
			Datum d=DatumSerialization.read(buffer, 0,Type.INT);
			mngFile.unpin(iBucketIndex);
		//	System.out.println(" rec= "+d.toInt());
			iPageToBeWritten=d.toInt();
			
			
			/**writing to the already existing overflow page **/
			//ByteBuffer overflowBuffer =  mngFile.getBuffer(iPageToBeWritten);
			
			ByteBuffer overflowBuffer =	mngFile.pin(iPageToBeWritten);
			DatumBuffer overflowBucket = new DatumBuffer(overflowBuffer, key.rowSchema());
			try{
				overflowBucket.write(temp);
				mngFile.unpin(iPageToBeWritten,true);
				return true;
			}catch(InsufficientSpaceException ex){
			//	System.out.println("NEXT OVERFLOW --");
				mngFile.unpin(iPageToBeWritten,true);
				WriteToOverFlowPage(iPageToBeWritten,directorySize,temp,overflowBucket,key,
						 mngFile);
				return true;
			}
		}
		}catch(Exception excp){
			System.out.println("OTHER EXCEPTION");
			System.out.println("RETURNING false");
			excp.printStackTrace();
			return false;
		}	  
  }
  
  public IndexIterator scan() 
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanTo(Datum[] toKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanFrom(Datum[] fromKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScan(Datum[] start, Datum[] end)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public Datum[] get(Datum[] key) throws SqlException, IOException
  {
	  int iKeyLength = keySpec.keySchema().length;
	  System.out.println("iKeyLength = "+iKeyLength);
	  
	  /**create and keep output place holder**/
      Datum[] output = new Datum[keySpec.rowSchema().length];
	  System.out.println("Inside get : HashIndex");

	  /**get back directory size**/
	  ByteBuffer byteBuffZero = file.getBuffer(0);
      DatumBuffer dbBucketZero = new DatumBuffer(byteBuffZero,this.keySpec.rowSchema()); 
      Datum[] dirSize = dbBucketZero.read(0);
      int iDirectorySize = dirSize[0].toInt();
      System.out.println("iDirectorySize = "+iDirectorySize);

      /**compute hash key**/
	  int hKey = keySpec.hashKey(key);
	  Datum[] hashedKey = new Datum[1];
	  hashedKey[0]=new Datum.Int(hKey);
	  System.out.println("Computed hash key = "+hKey);
		
      /**determine bucket number**/
	  int iBucketNum = (hKey % iDirectorySize);
	  System.out.println("Computed iBucketNum = "+(iBucketNum+1));
	  	  
	  /**reading data**/
	  ByteBuffer byteBuff = file.getBuffer(iBucketNum+1);
      DatumBuffer dbBucket = new DatumBuffer(byteBuff,this.keySpec.rowSchema());
      System.out.println("Entering bucket : "+(iBucketNum+1));
    
	  /**compute Bucket number**/
      System.out.println("dbBucket size = "+dbBucket.length());
      Datum dOverFlowPageNum;
      boolean blMatch=false;
      while(true){
      
	  for(int j=0;j < key.length;j++){
			for (int k = 0; k <= dbBucket.length(); k++) {
				Datum[] bktEntry = dbBucket.read(k);
				
				blMatch = true;
				for(int iCompare=0; iCompare < iKeyLength-1; iCompare++){
					if(!(bktEntry[iCompare].equals(key[iCompare]))){
						blMatch=false;
						break;
					}
				}
				if(blMatch){
					output = bktEntry;
					break;
				}
			}
			if(blMatch){
				break;
			}
	  }
	  
	  if(blMatch){
			break;
		}
	  	  
		/**to check for overflow bucket**/
	  	dOverFlowPageNum=DatumSerialization.read(byteBuff, 0,Type.INT);
		System.out.println("value dOverFlowPageNum = "+dOverFlowPageNum.toInt());	
			if(dOverFlowPageNum.toInt() == -1){
				break;
			}
			else{
				byteBuff = file.getBuffer(dOverFlowPageNum.toInt());
				dbBucket = new DatumBuffer(byteBuff, this.keySpec.rowSchema());

			}
	      
      }
	  return output;
  }
    
}