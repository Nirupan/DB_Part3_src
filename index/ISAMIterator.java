package edu.buffalo.cse.sql.index;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;

public class ISAMIterator implements IndexIterator{

	ManagedFile file;
	int FirstPageNumber;
	int LastPageNumber;
	IndexKeySpec keySpec;
	int iRecordCount;
	int typeFlag;
	int iGotOffset;
	
	static int iRow=0;	
	static int iCurrentIteration_PageNo;
	static int iCurrentIteration_RecordNo;
	static ByteBuffer scanBuff = null;
	static DatumBuffer scanDatumBuff = null;
	
	public ISAMIterator(ManagedFile file,int FirstPageNumber,int LastPageNumber,IndexKeySpec keySpec,int iRecordCount,int typeFlag,int iGotOffset) {
		this.file = file;
		this.FirstPageNumber = FirstPageNumber;
		this.LastPageNumber = LastPageNumber+1;
		this.iRecordCount = iRecordCount;
		System.out.println("LastPageNumber = "+LastPageNumber);
		this.iCurrentIteration_PageNo = FirstPageNumber;
		this.iCurrentIteration_RecordNo=0;
		this.keySpec = keySpec;
		this.typeFlag=typeFlag;
		this.iGotOffset=iGotOffset;
		/**starting condition for "rangeFrom" scan**/
		if(typeFlag==1){
			iCurrentIteration_RecordNo=iGotOffset;
		}
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			
			/**exit condition for rangeTo scan**/
			if(typeFlag==2){
				if(iCurrentIteration_PageNo == LastPageNumber-1){
				if(iCurrentIteration_RecordNo == iGotOffset){
					return false;
				}
				}
			}
			/**exit condition for normal scan**/
			else if(iRow==iRecordCount){ 
				return false;
			}			
			
			
			if(iCurrentIteration_PageNo < LastPageNumber){		
				if(scanDatumBuff!=null && (iCurrentIteration_RecordNo < scanDatumBuff.length())){
					return true;
				} else {
					System.out.println("iCurrentIteration_PageNo = "+iCurrentIteration_PageNo);
					scanBuff = file.getBuffer(iCurrentIteration_PageNo);
					scanDatumBuff = new DatumBuffer(scanBuff,keySpec.rowSchema());
					iCurrentIteration_PageNo++;
					iCurrentIteration_RecordNo = 0;
					return true;
				}
			}else{
				return false;
			}
		} catch (BufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public Datum[] next() {	
		iRow++;
		Datum[] temp = scanDatumBuff.read(iCurrentIteration_RecordNo);
		iCurrentIteration_RecordNo++;
		return temp;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws SqlException {
		// TODO Auto-generated method stub
		
	}
	
	
}