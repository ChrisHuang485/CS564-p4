#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
        status = db.createFile(fileName);
        if(status != OK)
            return status;

        // try to open file
        status = db.openFile(fileName, file);
        if(status != OK)
            return status;

        // make the header page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if(status != OK)
            return status;

        // cast as header page
        hdrPage = (FileHdrPage*) newPage;

        // input the name
        int length = fileName.size();
        for(int i = 0; i != length; ++i) {
            hdrPage->fileName[i] = fileName[i];
        }

        // add null-terminating character and initialize other stuff
        hdrPage->fileName[length] = '\0';
        hdrPage->firstPage = 0;
        hdrPage->lastPage = 0;
        hdrPage->pageCnt = 0;
        hdrPage->recCnt = 0;


        // make first actual data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if(status != OK)
            return status;

        // initialize contents
        newPage->init(newPageNo);

        // set end of file marker
        newPage->setNextPage(-1);

        // update header page stats
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // unpin pages and marks dirty bit to true
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if(status != OK)
            return status;

        status = bufMgr->unPinPage(file, newPageNo, true);
        if(status != OK)
            return status;

        // close file
        status = db.closeFile(file);
        if(status != OK)
            return status;

        return OK;
		
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return db.destroyFile(fileName);
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status  status;
    Page*   pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        File* file = filePtr;

        // gets the page number of header
        int pageNo = -1;
        status = file->getFirstPage(pageNo);
        if(status != OK)
            returnStatus = status;

        // reads header page in
        Page* page_ptr;
        status = bufMgr->readPage(file, pageNo, page_ptr);
        if(status != OK)
            returnStatus = status;

        // casts to HdrPage pointer and sets attributes
        headerPage = reinterpret_cast<FileHdrPage*>(page_ptr);
        headerPageNo = pageNo;
        hdrDirtyFlag = false;

        // gets page number of first actual data page
        int firstPageNo = headerPage->firstPage;
        status = bufMgr->readPage(file, firstPageNo, page_ptr);
        if(status != OK)
            returnStatus = status;

        // initializes attributes
        curPage = page_ptr;
        curPageNo = firstPageNo;
        curDirtyFlag = false;

        curRec = NULLRID;
        returnStatus = OK;
        return;

    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   
    // requested page is pinned, try and find record
   if(rid.pageNo == curPageNo) {
       status = curPage->getRecord(rid, rec);
   }
   
   // requested page is not pinned
   else {

       // unpin current page
       status = bufMgr->unPinPage(filePtr, curPageNo, curPage);
       if(status != OK)
           return status;

       // open requested page
       status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
       if(status != OK)
           return status;

       // mark current page no and clear dirty bit
       curPageNo = rid.pageNo;
       curDirtyFlag = false;

       // try and find record
       status = curPage->getRecord(rid, rec);
   }
   
   return status;

}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    while(curPageNo != -1)
    {
        tmpRid = curRec;
        status = curPage->nextRecord(tmpRid,nextRid);

        if(status != OK)
        {
            if (curPageNo == headerPage->lastPage) {
                return FILEEOF;
            }

            status = curPage->getNextPage(nextPageNo);
            if (status != OK){
                return status;
            } 
            
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if(status != OK) return status;
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if(status != OK) return status;

            curPageNo = nextPageNo;
            curDirtyFlag = false;
            status = curPage->firstRecord(nextRid);
        } 

        if(status == OK || status != NORECORDS)
        {
            status = curPage->getRecord(nextRid, rec);
            if(status != OK) {
                return status;
            }
            curRec = nextRid;
            
            if(matchRec(rec))
            {
                outRid = curRec;
                return status;
            }
        }
    }
    return OK;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool

    if ((curPage != NULL) && (curPageNo != headerPage->lastPage)) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) cerr << "error in unpin of data page\n";
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) cerr << "error in readPage \n";
        curDirtyFlag = false;
    }
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }



    //Qiaoyu's part
    if (curPage == NULL) {//Make the last page as the current page
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (unpinstatus != OK && unpinstatus != PAGENOTPINNED){
            return unpinstatus;
        }
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK) {return status;}
        curPageNo = headerPage->lastPage;
    }


    //try to insert
    status = curPage->insertRecord(rec, outRid);

    if (status == NOSPACE) {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        newPage->init(newPageNo);
        if (status != OK){return status;}

        curPage->setNextPage(newPageNo);
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {return status;}
        curDirtyFlag = false;
        curPage = newPage;
        curPageNo = newPageNo;
        status = curPage->insertRecord(rec, outRid);
        if (status != OK) {return status;}
        headerPage->pageCnt += 1;
        headerPage->lastPage = curPageNo;
        hdrDirtyFlag = true;
    }

    curDirtyFlag = true;
    headerPage->recCnt += 1;
    curRec = outRid;

    return status;
}

