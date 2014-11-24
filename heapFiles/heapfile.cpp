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
    
    if (status != OK) // Need to create the file
    {
	status = db.createFile(fileName); // Create the file
	if (status != OK) return status;
	
	status = db.openFile(fileName, file); // Returns the file pointer in the file parameter
	if (status != OK) return status;
	
	status = bufMgr->allocPage(file, hdrPageNo, newPage); // Allocate the header page
	if(status != OK) return status;
	
	hdrPage = (FileHdrPage *)newPage; // Cast to a FileHdrPage type
	
	bufMgr->allocPage(file, newPageNo, newPage); // Allocate a new page
	newPage->init(newPageNo); // Set up a new empty page	
	
	// Initialize values of header page
	strcpy(hdrPage->fileName, fileName.c_str()); // Copy string into char[], limited to MAXNAMESIZE
	hdrPage->firstPage = newPageNo;
	hdrPage->lastPage = newPageNo;
	hdrPage->pageCnt = 1; // Technically newPage exists
	hdrPage->recCnt = 0; // But there aren't any records on it yet
	
	status = bufMgr->unPinPage(file, hdrPageNo, true); // UnPin and mark dirty
	if (status != OK) return status;
	
	status = bufMgr->unPinPage(file, newPageNo, true); // UnPin and mark dirty
	if (status != OK) return status;
	
	status = db.closeFile(file);
	return status; // Finally done.
    }

    return FILEEXISTS;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
	// Gets page number of header page
	returnStatus = filePtr->getFirstPage(headerPageNo);
	if (returnStatus != OK) return;

	// Reads header page
	returnStatus = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
	if (returnStatus != OK) return;

	// Initialize protected data members
	headerPage   = (FileHdrPage*) pagePtr;
	hdrDirtyFlag = false;

	// Read first data page
	curPageNo    = headerPage->firstPage;
	returnStatus = bufMgr->readPage(filePtr, curPageNo, curPage);
	if (returnStatus != OK) return;

	// Finish Initializing protected data members
	curDirtyFlag = false;
	curRec       = NULLRID;
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

const Status HeapFile::getRecord(const RID &  rid, Record & rec)
{	
	Status status;

	// If record is not on the currently pinned page
	if (rid.pageNo != curPageNo) {
		// Unpin current page
		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		if (status != OK) return status;
		
		// Cleanup curPage vars
		curDirtyFlag = false;

		// Read the required page
		status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
		if (status != OK) return status;

		// Pin the reqired page
		curPageNo = rid.pageNo;
	}   

	// Get record
	status = curPage->getRecord(rid, rec);
	if (status != OK) return status;

	// Set current record id
	curRec = rid;

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
        ((type_ == INTEGER && length_ != sizeof(int))
         || (type_ == FLOAT && length_ != sizeof(float))) ||
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

// Out: outRid
const Status HeapFileScan::scanNext(RID& outRid)
{	
	Status 	status = OK;
	RID	tmpRid;
	Record  rec;

	do {
		// If no record yet, grab first record
		if ((curRec.pageNo == -1) && (curRec.slotNo == -1)) {
			status = curPage->firstRecord(tmpRid);
			if (status != OK) return FILEEOF;
		} 
		// Otherwise get next record
		else {
			status = curPage->nextRecord(curRec, tmpRid);
		}

		// If we are at the end of the page
		if (status == ENDOFPAGE) {
			// Unpin current page
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;

			// Getting the next page
			curPage->getNextPage(curPageNo);

			// Read page
			status = bufMgr->readPage(filePtr, curPageNo, curPage);
			if (status != OK) return status;

			// Get first record
			status = curPage->firstRecord(tmpRid);
			if (status != OK) return FILEEOF;
		}

		status = getRecord(rec);
		if (status != OK) return status;

		curRec = tmpRid;
	} while(!matchRec(rec));

	outRid = curRec;
	return status;
}

/*
    
    // There will always be curPage to loop over, this ensures do/while will
    // run at least once before terminating. (As I understand it)
    nextPageNo = curPageNo; 
    
    while(status != FILEEOF)
    {    	    		
    	status = bufMgr->readPage(filePtr, nextPageNo, curPage); // Read curPage into the buffer pool
    	if(status != OK) return status; // Only continue if nothing's gone wrong
    	
    	do // Loop over records on page
    	{
    	    if(curRec.pageNo == -1 and curRec.slotNo == -1) // We haven't scanned anything on this page yet
	    {
	        status = curPage->firstRecord(curRec); // Put the first RID of the page in curRec
	        if(status == NORECORDS) break; // Break out of the loop for this page
	    }
    	
	    status = curPage->getRecord(curRec, rec); // Pulls the actual record data into rec
    	    if(status != OK) return status; // Only continue if nothing's gone wrong
    	
	    if(matchRec(rec)) // Check if the Record matches the predicate filter
	    {
	    	outRid = curRec;
	    	
	    	if(markedRec.pageNo == curRec.pageNo && markedRec.slotNo == curRec.slotNo) // We saw this record last time!
	    	    break; 
	    	
	    	status = curPage->nextRecord(curRec, tmpRid);
	    	if (status != ENDOFPAGE) 
	    	{
	    	    curRec = tmpRid;
	    	}
	    	else
	    	{
	    	    markedRec = curRec;
	    	}
	        return OK;
	    }
	}
	while(curPage->nextRecord(curRec, curRec) != ENDOFPAGE);
	
	// Reset curRec so that the next page will start scanning on the first record
	curRec.pageNo = -1;
	curRec.slotNo = -1;
	
	// Move to the next page
	curPage->getNextPage(nextPageNo);
	if(nextPageNo == -1) return FILEEOF;	
    }
    
    return FILEEOF; // Should never actually reach this point
    // The designed point to break out of this method is the check at the top of the first do/while loop
}
*/

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
  //Do nothing. Heapfile constructor will read the header page and the first
  // data page of the file into the buffer pool
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
    Status	status;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // Ensure that we deal with the last page of the file
    if(curPageNo != headerPage->lastPage) // Not currently on the last page
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // Release the current page
    	if (status != OK) return status;
    	
    	status = bufMgr->readPage(filePtr, headerPage->lastPage, newPage); // Read in last page
    	if (status != OK) return status;
    	
    	// Set curPage to the last page in the file
    	curPage = newPage;
    	curPageNo = headerPage->lastPage;
    	curDirtyFlag = false;
    }
    
    // At this point curPage should always be the last page of the file    

    status = curPage->insertRecord(rec, outRid); // Try to insert on this page

    if (status == NOSPACE) // The page was full
    {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage); // Make new page
        if (status != OK) return status; // Bail, db layer broked        
        newPage->init(newPageNo); // Initialize the page
        
        curPage->setNextPage(newPageNo);
        
        // Unpin and Pin pages appropriately
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // No longer need the current curPage
        if (status != OK) return status; // Bail
        
        // Update curPage info
        curPage = newPage; // Pointer
        curPageNo = newPageNo; // PageNo
        curDirtyFlag = false; // Haven't written yet
       
        status = curPage->insertRecord(rec, outRid); // Should work now
        if (status != OK) return status; // But just in case
    	
        // Update the FileHdrPage
        headerPage->lastPage = curPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
    }
    
    // Do this stuff regardless of space
    curDirtyFlag = true; // New record in the page!
    headerPage->recCnt++;
    curRec = outRid;

    return OK; // Success!
}


