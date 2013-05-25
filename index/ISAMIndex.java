package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;

//import edu.buffalo.cse.sql.index.BinarytreeDemo;

public class ISAMIndex implements IndexFile {

	public ManagedFile file;
	public IndexKeySpec keySpec;
	public static int iRows=0;
	public static int iGotPage;
	public static int iGotOffset;
	  
	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
			throws IOException, SqlException {
		  this.file=file;
		  this.keySpec=keySpec;
	}

	public static ISAMIndex create(FileManager fm, File path,Iterator<Datum[]> dataSource, IndexKeySpec key)
			{
		try {
			
			int iMinSize = 1;
			int iPageNo = 0;
			if (path.exists())
				path.delete();
			System.out.println("File manager = " + fm.toString());
			System.out.println("file path = " + path);
			System.out.println("IndexKeySpec = " + key);
			System.out.println("iMinSize = "+iMinSize);
			System.out.println("The tree begins");
			
			/**create file and ensure size of 1 at start**/
			ManagedFile mngFile = fm.open(path);
			mngFile.ensureSize(iMinSize);
			
			boolean isStartOfPageWritten = false;
							
			while (dataSource.hasNext()) {
				iRows++;
				Datum[] temp = dataSource.next();
				int hKey = key.hashKey(key.createKey(temp));
				/**wrting rows to file**/
				//ByteBuffer bbff = mngFile.getBuffer(iMinSize-1);
				ByteBuffer bbff = mngFile.pin(iMinSize-1);
				DatumBuffer db =new DatumBuffer(bbff, key.rowSchema());
				db.initialize();
				try{
					db.write(temp);
					mngFile.unpin(iMinSize-1, true);
				}catch(InsufficientSpaceException e){
					mngFile.unpin(iMinSize-1, true);
					
				    /**preparing pointer to next**/
					iMinSize++;
				    mngFile.ensureSize(iMinSize);
					    
					/**going to new page and writing data**/
				//	ByteBuffer nextLeafBuffer =  mngFile.getBuffer(iMinSize-1);
					ByteBuffer nextLeafBuffer =  mngFile.pin(iMinSize-1);
					DatumBuffer nextLeafBucket = new DatumBuffer(nextLeafBuffer, key.rowSchema());
					nextLeafBucket.initialize();
					nextLeafBucket.write(temp);
					mngFile.unpin(iMinSize-1, true);
					iPageNo++;
					//System.out.println("Page no.: = "+iPageNo);
				}
			}
			
			/**to find first level above leaf**/
			System.out.println("Printing first row in each page");
			
			int iStart=0;
			boolean isLeafPage = true;
			/**leaf = level 0**/
			int iNumberOfLevelsIncludingLeaf = 0;
			Map mpLevel_PhyPage = new HashMap();
		
			System.out.println("Starting index creation from pageno.::  "+iMinSize);
			
			while (true) {
				iNumberOfLevelsIncludingLeaf++;
				List<Integer> lsIndexATLevel1 = new ArrayList<Integer>();
				for (int iTemp = iStart+1; iTemp < iMinSize; iTemp++) {
				//	ByteBuffer leafPage = mngFile.getBuffer(iTemp);
					ByteBuffer leafPage = mngFile.pin(iTemp);

					DatumBuffer dbLeafPage = new DatumBuffer(leafPage,key.rowSchema());
					Datum[] row = null;
					if(isLeafPage)
						row = dbLeafPage.read(0);
					else
						row = dbLeafPage.read(1);
					mngFile.unpin(iTemp);
					
					/**
					 * 0          42
					 * 1          43
					 * 2          ...
					 * 3
					 * 
					 * 
					 * 41       
					 * page 0     page 1
					 * 
					 * **/
					
						lsIndexATLevel1.add(iTemp-iStart-1);
					if (!(iTemp == 0)) {
						lsIndexATLevel1.add(row[0].toInt());
					}
					/*for (int i = 0; i < row.length; i++) {
						System.out.print(" " + row[i]);
					}*/
					//System.out.println();
				}

				for (Integer j : lsIndexATLevel1) {
					System.out.print("\t" + j);
				}

				/** to write indexes to file page **/
				int iIndex = 0;
				int iCountOfIndexPages = 0;
				System.out.println();
				System.out.println("=================== iMinIndex B4:: " + iMinSize);
				int[] info = createIndexPages(iMinSize, mngFile,lsIndexATLevel1, key, iIndex, iCountOfIndexPages);
				System.out.println("=================== iCountOfIndexPages :: " + info[0]);
				System.out.println("=================== iMinIndex :: " + info[1]);

				iCountOfIndexPages = info[0];
				mpLevel_PhyPage.put(iNumberOfLevelsIncludingLeaf, iCountOfIndexPages);			
				isLeafPage=false;
				iStart = iMinSize;
				iMinSize = info[1];
				
				if (iCountOfIndexPages == 1) {
					break;
				}			
			}
			//---------------------------------------------------------------------------------
			
			System.out.println("Number of levels including leaf = "+iNumberOfLevelsIncludingLeaf);			
			iMinSize++;
			mngFile.ensureSize(iMinSize);
		//	ByteBuffer lastPage = mngFile.getBuffer(iMinSize-1);
			ByteBuffer lastPage = mngFile.pin(iMinSize-1);
			DatumBuffer lastPageDatum = new DatumBuffer(lastPage, key.rowSchema());
			
			TreeMap ts = new TreeMap(mpLevel_PhyPage);
			Iterator itr = ts.keySet().iterator();
			System.out.println("printing map : ");
			while(itr.hasNext()){
				int mpKey = (int)itr.next();
				Datum[] dt = new Datum[2];
				dt[0] = new Datum.Int(mpKey);
				int iVal = (int)mpLevel_PhyPage.get(mpKey);
				dt[1] = new Datum.Int(iVal);
				lastPageDatum.write(dt);
				System.out.println(mpKey + " " +iVal);
			}
					
			/**to write number of records to map**/
			Datum[] dt = new Datum[2];
			dt[0] = new Datum.Int(Integer.MIN_VALUE);
			dt[1] = new Datum.Int(iRows);
			lastPageDatum.write(dt);
			mngFile.unpin(iMinSize-1,true);
			
			/**aditya :: commented**/
			/*for (int i=0; i<mngFile.size();i++){
				mngFile.dirty(i);
			}*/
			fm.close(path);
			ISAMIndex hIndex = new ISAMIndex(mngFile, key);
			return hIndex;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SqlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}
		// return null;
		return null;

	}
	
	static int[] createIndexPages(int iMinSize,ManagedFile mngFile,List<Integer> lsIndexATLevel1,IndexKeySpec key,int iIndex,int iCountOfIndexPages){
	//	System.out.println("inside createIndexPages");
		iCountOfIndexPages++;
		Datum[] ptrORindex=null;
		try{
			iMinSize++;
			mngFile.ensureSize(iMinSize);
			//ByteBuffer indexPage = mngFile.getBuffer(iMinSize-1);
			ByteBuffer indexPage = mngFile.pin(iMinSize-1);
			DatumBuffer dbIndexPage = new DatumBuffer(indexPage, key.rowSchema());
			
			for(; iIndex < lsIndexATLevel1.size();iIndex++){
				ptrORindex = new Datum[1];
				ptrORindex[0] = new Datum.Int(lsIndexATLevel1.get(iIndex));
				dbIndexPage.write(ptrORindex);
				
			}
			
			mngFile.unpin(iMinSize-1, true);
			
			int[] info = new int[2];
			info[0] = iCountOfIndexPages;
			info[1] = iMinSize;
			
			return info;
			}catch(InsufficientSpaceException e){
			
			/**observation : in the list, the pointer value is present at even locations i.e 0,2,4,6,....
			 * key value present at odd locations i.e 1,3,5,7,...
			 * 
			 * At each page start, we require pointer
			 * if (iIndex%2 == 0), we are at even location and the new page will contain the pointer as its first entry
			 * so we are all set if iIndex%2 == 0
			 * 
			 * if (iIndex%2 != 0), the iIndex is odd i.e it is key value and page start will now contain key value
			 * but we need pointer at file start, we we will do iIndex--;
			 * 
			 * so general page structure will be
			 * ptr key ptr key ptr key ........... ptr
			 * 
			 * Also, if iIndex%2 == 0, then the last iIndex will be odd, thus, the last entry to the file would be key,
			 * however, since we want last value as ptr
			 * **/
			if(iIndex%2 == 0){		
				System.out.println("TRUE :: "+ptrORindex[0]);
			}else{
				iIndex--;
				//System.out.println("FALSE :: "+ptrORindex[0]);
			}
			
			int[] info=	createIndexPages(iMinSize, mngFile, lsIndexATLevel1, key, iIndex,iCountOfIndexPages);
			return info;
			}catch(Exception ex){
				ex.printStackTrace();
			}
		
		return null;
	}
	
	
	public IndexIterator scan() throws SqlException, IOException {
		//throw new SqlException("Unimplemented");
		/**get back directory size**/
		  //ByteBuffer infoBuffer = file.getBuffer(file.size()-1);
		  ByteBuffer infoBuffer = file.pin(file.size()-1);
	      DatumBuffer infoDatumBuffer = new DatumBuffer(infoBuffer,this.keySpec.rowSchema()); 
	      TreeMap treeMap = new TreeMap();
	      Datum[] temp=null;
	      for (int i=0;i < infoDatumBuffer.length(); i++){
		       temp = new Datum[2]; 
		      temp = infoDatumBuffer.read(i);
		      treeMap.put(temp[0].toInt(), temp[1].toInt());	  
	      }
	      file.unpin(file.size()-1);
	      Iterator itr  = treeMap.descendingKeySet().iterator();
	      int iTotalOffset = 0;
	      int iRecordCount = 0;
	      while(itr.hasNext()){
	    	int iIndexLevelNo = (int) itr.next();
	    	if(iIndexLevelNo == Integer.MIN_VALUE){
	    		iRecordCount = (int) treeMap.get(iIndexLevelNo);
	    		continue;
	    	}
			int iIndexPageSize = (int) treeMap.get(iIndexLevelNo);
			iTotalOffset = iTotalOffset + iIndexPageSize;	    	  
	      }
	      System.out.println("iTotalOffset = "+iTotalOffset);
	      int iLastPage = file.size() - 1 - iTotalOffset;
	      
	      int typeFlag=0;
	      IndexIterator isamItr = new ISAMIterator(file,0,iLastPage,keySpec,iRecordCount,typeFlag,0);
	      return isamItr;
	}

	public IndexIterator rangeScanTo(Datum[] toKey) throws SqlException,
			IOException {

		//throw new SqlException("Unimplemented");
		get(toKey);
		int typeFlag=2;
		/**to calculate total number of records in output range**/
		int iRecordCount = 0;
		//iRecordCount = iGotPage*(d.length())+iGotOffset;
		System.out.println("iRecordCount = "+iRecordCount);	      
		IndexIterator isamItr = new ISAMIterator(file,0,iGotPage,keySpec,iRecordCount,typeFlag,iGotOffset);
		
		return isamItr;
		}

	public IndexIterator rangeScanFrom(Datum[] fromKey) throws SqlException,
			IOException {
		//throw new SqlException("Unimplemented");
		//ByteBuffer infoBuffer = file.getBuffer(file.size()-1);
		  ByteBuffer infoBuffer = file.pin(file.size()-1);
	      DatumBuffer infoDatumBuffer = new DatumBuffer(infoBuffer,this.keySpec.rowSchema()); 
	      TreeMap treeMap = new TreeMap();
	      Datum[] temp=null;
	      for (int i=0;i < infoDatumBuffer.length(); i++){
		       temp = new Datum[2]; 
		      temp = infoDatumBuffer.read(i);
		      treeMap.put(temp[0].toInt(), temp[1].toInt());	  
	      }     
	      file.unpin(file.size()-1);
	      Iterator itr  = treeMap.descendingKeySet().iterator();
	      int iTotalOffset = 0;
	      int iRecordCount = 0;
	      while(itr.hasNext()){
	    	int iIndexLevelNo = (int) itr.next();
	    	if(iIndexLevelNo == Integer.MIN_VALUE){
	    		iRecordCount = (int) treeMap.get(iIndexLevelNo);
	    		continue;
	    	}
			int iIndexPageSize = (int) treeMap.get(iIndexLevelNo);
			iTotalOffset = iTotalOffset + iIndexPageSize;	    	  
	      }
	      System.out.println("iTotalOffset = "+iTotalOffset);
	      int iLastPage = file.size() - 1 - iTotalOffset;
	    
		get(fromKey);
		int typeFlag=1;
		 
		/**to calculate total number of records in output range**/
		ByteBuffer b;
		int k=0;
		if(iGotPage-1 >=0){
		//	b = file.getBuffer(iGotPage-1);
			b = file.pin(iGotPage-1);
			k=1;
		}
		else{
		//	b = file.getBuffer(iGotPage);
			b = file.pin(iGotPage);
			k=2;
		}
	    DatumBuffer d = new DatumBuffer(b,this.keySpec.rowSchema()); 
		System.out.println("infoDatumBuffer.length() = "+d.length());
		iRecordCount = iRecordCount - iGotPage*(d.length())+iGotOffset;
	    System.out.println("iRecordCount = "+iRecordCount);	      
		IndexIterator isamItr = new ISAMIterator(file,iGotPage,iLastPage,keySpec,iRecordCount,typeFlag,iGotOffset);
		if(k==1) file.unpin(iGotPage-1);
		if(k==2) file.unpin(iGotPage);
		
		return isamItr;
	}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)
			throws SqlException, IOException {
	//	throw new SqlException("Unimplemented");
		
		get(start);
		int iStartPage = iGotPage;
		int iStartOffset = iGotOffset;
		get(end);
		int iEndPage = iGotPage;
		int iEndOffset = iGotOffset;
		
		int iRecordCount = ((iEndPage*42)+iEndOffset) - ((iStartPage*42)+iStartOffset)+1;
		System.out.println("iRecordCount = "+iRecordCount);
		int typeFlag = 1;
		IndexIterator isamItr = new ISAMIterator(file,iStartPage,iEndPage,keySpec,iRecordCount,typeFlag,iGotOffset);

		return isamItr;
	}

	public Datum[] get(Datum[] key) throws SqlException, IOException {
		
		System.out.println("Inside get method for ISAM index");	
		int iKeyToFind = key[0].toInt(); 
		
		/**get back directory size**/
		// ByteBuffer infoBuffer = file.getBuffer(file.size()-1);
		   ByteBuffer infoBuffer = file.pin(file.size()-1);
	      DatumBuffer infoDatumBuffer = new DatumBuffer(infoBuffer,this.keySpec.rowSchema()); 
	      TreeMap treeMap = new TreeMap();
	      for (int i=0;i < infoDatumBuffer.length(); i++){
		      Datum[] temp = new Datum[2]; 
		      temp = infoDatumBuffer.read(i);
		      treeMap.put(temp[0].toInt(), temp[1].toInt());	  
	      }
	      file.unpin(file.size()-1);
	      Iterator itr  = treeMap.descendingKeySet().iterator();
	      int iNextLevel_NextPage_Offset = 0;
	      int iEffectiveIndexPage_ToBeAccessed = file.size()-1;
	      /**this loop will run till it finds the exact leaf page where there 
	       * is possibility of finding the data with inputted key
	       * thus, it will find all the page numbers which we need to search
	       * to find record from the leaf page 
	       * We will start from the root which is first entry in map (sorted by key which is level number
	       * and root is at highest level)
	       * **/
		  while (itr.hasNext()) {
				int iIndexLevelNo = (int) itr.next();
				if(iIndexLevelNo == Integer.MIN_VALUE) continue;
				int iIndexPageSize = (int) treeMap.get(iIndexLevelNo);
				System.out.println(" " + iIndexLevelNo + " " + iIndexPageSize);			
				iEffectiveIndexPage_ToBeAccessed=iEffectiveIndexPage_ToBeAccessed-iIndexPageSize+iNextLevel_NextPage_Offset;
				//ByteBuffer indexBuffer = indexBuffer = file.getBuffer(iEffectiveIndexPage_ToBeAccessed);
				ByteBuffer indexBuffer  = file.pin(iEffectiveIndexPage_ToBeAccessed);
				
				DatumBuffer indexPageDatumBuffer = new DatumBuffer(indexBuffer,keySpec.keySchema());
				List lsIndexPageData = new ArrayList();
				for(int i=0;i<indexPageDatumBuffer.length();i++){
			    	  Datum[] root = indexPageDatumBuffer.read(i);
			    	  for(int j=0;j<root.length;j++){
			    		  System.out.print(" "+root[j]);
			    		  lsIndexPageData.add(root[j].toInt());
			    	  }
			     }
				file.unpin(iEffectiveIndexPage_ToBeAccessed);
				
				System.out.println();
				Iterator lsItr = lsIndexPageData.iterator();
				int iPointerToPagePrev = 0;
				int iPointerToPageNext = 0;
				
				/**skip first because we are using a pointer to previous page variable
				 * instead of this
				 * **/
				lsItr.next();
				while(lsItr.hasNext()){
					/**pointer to previous page initialization , default = 0**/
					iPointerToPagePrev = iPointerToPageNext;
					int iKey = (int)lsItr.next();
					iPointerToPageNext = (int) lsItr.next();
					
					System.out.println("GOT key present = "+iKey);
					/** if incoming key to be found is lesser than the key scanned
					 * we will break as we do not need to search ahead
					 * else - we will continue search till we do not find the scanned key
					 * which is greater than inputted key
					 * **/
					if(iKeyToFind < iKey){
						iNextLevel_NextPage_Offset = iPointerToPagePrev;
						System.out.println("GOT CONDITION MATCH");
						System.out.println("GOT iKeyToFind = "+iKeyToFind);
						System.out.println("GOT key present = "+iKey);
						System.out.println("GOT NEXT PAGE OFFSET = "+iNextLevel_NextPage_Offset);
						break;
					}
					
					/**when code reaches here, we need to initialze previous by current next
					 * bcoz in new iteration, current next page will be new past page**/
					//iPointerToPagePrev = iPointerToPageNext;
				}
				
		   }
		  System.out.println("NOW accessing  :: "+iNextLevel_NextPage_Offset);
		  /**to find the actual record matching the key 
		   * in the computed leaf page number**/
		  
		  iGotPage = iNextLevel_NextPage_Offset;
		  //ByteBuffer actualPage = file.getBuffer(iNextLevel_NextPage_Offset);
		  ByteBuffer actualPage = file.pin(iNextLevel_NextPage_Offset);
		  DatumBuffer actualPageDatumBuffer = new DatumBuffer(actualPage,keySpec.rowSchema());
		  Datum[] temp = null;
		for (int k = 0; k < actualPageDatumBuffer.length(); k++) {
			temp = actualPageDatumBuffer.read(k);
			if (temp[0].toInt() == iKeyToFind) {
				iGotOffset=k;
				break;
			}
		}
		file.unpin(iNextLevel_NextPage_Offset);
		  return temp;
		}
}
