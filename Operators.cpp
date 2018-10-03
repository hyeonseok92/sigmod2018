#include "Operators.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <mutex>
#include "Joiner.hpp"
#include "Config.hpp"
#include "Utils.hpp"

//---------------------------------------------------------------------------
using namespace std;

void Operator::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    if (auto p = parent.lock()) {
        int pending = __sync_sub_and_fetch(&p->pendingAsyncOperator, 1);
        assert(pending>=0);
#ifdef VERBOSE
    cout << "Operator("<< queryIndex << "," << operatorIndex <<")::finishAsyncRun parent's pending: " << pending << endl;
#endif
        if (pending == 0 && startParentAsync) 
            p->createAsyncTasks(ioService);
    } else {
#ifdef VERBOSE
    cout << "Operator("<< queryIndex << "," << operatorIndex <<")::finishAsyncRun has no parent" << endl;
#endif
        // root node
    }
    
}

//---------------------------------------------------------------------------
bool Scan::require(SelectInfo info)
// Require a column and add it to results
{
    if (info.binding!=relationBinding)  {
        return false;
    }
    assert(info.colId<relation.columns.size());
    if (select2ResultColId.find(info)==select2ResultColId.end()) {
//        resultColumns.push_back(relation.columns[info.colId]);
        results.emplace_back(1);
        infos.push_back(info);
		select2ResultColId[info]=results.size()-1;
    }
    return true;
}
//---------------------------------------------------------------------------
unsigned Scan::getResultsSize() {
    return results.size()*relation.size*8; 
}
//---------------------------------------------------------------------------
void Scan::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "Scan("<< queryIndex << "," << operatorIndex <<")::asyncRun, Task" << endl;
#endif
    pendingAsyncOperator = 0;
    for (int i=0; i<infos.size(); i++) {
        results[i].addTuples(0, relation.columns[infos[i].colId], relation.size);
        results[i].fix();
    }
    resultSize=relation.size; 
    finishAsyncRun(ioService, true);
}
//---------------------------------------------------------------------------
bool FilterScan::require(SelectInfo info)
// Require a column and add it to results
{
    if (info.binding!=relationBinding)
        return false;
    assert(info.colId<relation.columns.size());
    if (select2ResultColId.find(info)==select2ResultColId.end()) {
        // Add to results
        inputData.push_back(relation.columns[info.colId]);
//        tmpResults.emplace_back();
        unsigned colId=inputData.size()-1;
        select2ResultColId[info]=colId;
    }
    return true;
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
// Apply filter
{
    auto compareCol=relation.columns[f.filterColumn.colId];
    auto constant=f.constant;
    switch (f.comparison) {
        case FilterInfo::Comparison::Equal:
            return compareCol[i]==constant;
        case FilterInfo::Comparison::Greater:
            return compareCol[i]>constant;
        case FilterInfo::Comparison::Less:
            return compareCol[i]<constant;
    };
    return false;
}
//---------------------------------------------------------------------------
void FilterScan::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 0;
    __sync_synchronize();
    createAsyncTasks(ioService);  
}

//---------------------------------------------------------------------------
void FilterScan::createAsyncTasks(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif  
    //const unsigned partitionSize = L2_SIZE/2;
    //const unsigned taskNum = CNT_PARTITIONS(relation.size*relation.columns.size()*8, partitionSize);
	unsigned cntTask = THREAD_NUM;
    unsigned taskLength = (relation.size+cntTask-1)/cntTask;
    
    if (relation.size/cntTask < minTuplesPerTask) {
        cntTask = (relation.size+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength = minTuplesPerTask;
    }
    
    pendingTask = cntTask;
    
    for (int i=0; i<inputData.size(); i++) {
		results.emplace_back(cntTask);
    }
	for (int i=0; i<cntTask; i++) {
		tmpResults.emplace_back();
		for (int j=0; j<inputData.size(); j++) {
			tmpResults[i].emplace_back();
		}
	}
	
    __sync_synchronize(); 
    // unsigned length = partitionSize/(relation.columns.size()*8); 
    unsigned length = taskLength;
    for (unsigned i=0; i<cntTask; i++) {
        unsigned start = i*length;
        if (i == cntTask-1 && relation.size%length) {
            length = relation.size%length;
        }
        ioService.post(bind(&FilterScan::filterTask, this, &ioService, i, start, length)); 
    }
}
//---------------------------------------------------------------------------
void FilterScan::filterTask(boost::asio::io_service* ioService, int taskIndex, unsigned start, unsigned length) {
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    localResults.reserve(length);
    for (unsigned i=start;i<start+length;++i) {
        bool pass=true;
        for (auto& f : filters) {
            pass&=applyFilter(i,f);
        }
        if (pass) {
            for (unsigned cId=0;cId<inputData.size();++cId)
                localResults[cId].push_back(inputData[cId][i]);
        }
    }

    for (unsigned cId=0;cId<inputData.size();++cId) {
		results[cId].addTuples(taskIndex, localResults[cId].data(), localResults[cId].size());
    }
    //resultSize += localResults[0].size();
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
        for (unsigned cId=0;cId<inputData.size();++cId) {
            results[cId].fix();
        }
        finishAsyncRun(*ioService, true);
    }
}

//---------------------------------------------------------------------------
vector<Column<uint64_t>>& Operator::getResults()
// Get materialized results
{
//    vector<uint64_t*> resultVector;
//    for (auto& c : tmpResults) {
//        resultVector.push_back(c.data());
//    }
    return results;
}
//---------------------------------------------------------------------------
unsigned Operator::getResultsSize() {
    return resultSize*results.size()*8;
}
//---------------------------------------------------------------------------
bool Join::require(SelectInfo info)
// Require a column and add it to results
{
    if (requestedColumns.count(info)==0) {
        bool success=false;
        if(left->require(info)) {
            requestedColumnsLeft.emplace_back(info);
            success=true;
        } else if (right->require(info)) {
            success=true;
            requestedColumnsRight.emplace_back(info);
        }
        if (!success)
            return false;

        requestedColumns.emplace(info);
    }
    return true;
}
//---------------------------------------------------------------------------
void Join::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 2;
    left->require(pInfo.left);
    right->require(pInfo.right);
    __sync_synchronize();
    left->asyncRun(ioService);
    right->asyncRun(ioService);
}

//---------------------------------------------------------------------------
void Join::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif

    if (left->resultSize>right->resultSize) {
        swap(left,right);
        swap(pInfo.left,pInfo.right);
        swap(requestedColumnsLeft,requestedColumnsRight);
    }

    auto& leftInputData=left->getResults();
    auto& rightInputData=right->getResults(); 

    unsigned resColId = 0;
    for (auto& info : requestedColumnsLeft) {
        select2ResultColId[info]=resColId++;
    }
    for (auto& info : requestedColumnsRight) {
        select2ResultColId[info]=resColId++;
    }
    
    if (left->resultSize == 0) { // no reuslts
        finishAsyncRun(ioService, true);
        return;
    }

    leftColId=left->resolve(pInfo.left);
    rightColId=right->resolve(pInfo.right);

    cntPartition = THREAD_NUM;
    for (int i=0; i<requestedColumns.size(); i++) {
        results.emplace_back(cntPartition);
    }

    for (int i=0; i<cntPartition; i++) {
        tmpResults.emplace_back();
    }

    pendingBuilding = cntPartition;
    __sync_synchronize();
    uint64_t length = left->resultSize / cntPartition;
    uint64_t start = 0;
    uint64_t additional = left->resultSize - length * cntPartition;
    for (int i=0; i<cntPartition; i++) {
        uint64_t cur_length = length + (i < additional);
        ioService.post(bind(&Join::buildingTask, this, &ioService, start,
                    cur_length));
        start += cur_length;
    }
}

//---------------------------------------------------------------------------
void Join::buildingTask(boost::asio::io_service* ioService, uint64_t start, uint64_t length) {
    vector<Column<uint64_t>>& inputData = left->getResults();
    Column<uint64_t>& keyColumn = inputData[leftColId];
    
	auto keyIt = keyColumn.begin(start);
	for (unsigned i=start, limit=start+length; i<limit; i++, ++keyIt) {
        hashTable.insert(make_pair(*keyIt, i));
    }
    int remainder = __sync_sub_and_fetch(&pendingBuilding, 1);
    if (UNLIKELY(remainder == 0)) { // gogo probing
        pendingProbing = cntPartition;
        __sync_synchronize();
        uint64_t length = right->resultSize / cntPartition;
        uint64_t start = 0;
        uint64_t additional = right->resultSize - length * cntPartition;
        for (int i=0; i<cntPartition; i++) {
            uint64_t cur_length = length + (i < additional);
            ioService->post(bind(&Join::probingTask, this, ioService, i, start,
                        cur_length));
            start += cur_length;
        }
    }
}

void Join::probingTask(boost::asio::io_service* ioService, int taskId, uint64_t start, uint64_t length) {
    // Resolve the partitioned columns
    vector<vector<uint64_t>>& localResults = tmpResults[taskId];
    vector<Column<uint64_t>>& leftInputData = left->getResults();
    vector<Column<uint64_t>>& rightInputData = right->getResults();
    Column<uint64_t>& keyColumn = rightInputData[rightColId];
    vector<Column<uint64_t>::Iterator> colIt;
    std::vector<Column<uint64_t>> copyLeftData;

    for (auto& info : requestedColumnsLeft) {
        copyLeftData.push_back(leftInputData[left->resolve(info)]);
    }
    for (auto& info : requestedColumnsRight) {
        colIt.push_back(rightInputData[right->resolve(info)].begin(start));
    }
    for (unsigned i=0; i<requestedColumns.size(); i++) {
        localResults.emplace_back();
    }

    // probing
	auto keyIt = keyColumn.begin(start);
	for (unsigned i=start, limit=start+length; i<limit; i++, ++keyIt) {
        auto range=hashTable.equal_range(*keyIt);
        for (auto iter=range.first;iter!=range.second;++iter) {
            unsigned relColId=0;
            for (unsigned cId=0;cId<copyLeftData.size();++cId){
                localResults[relColId++].push_back(*(copyLeftData[cId].begin(iter->second)));
            }
            for (unsigned j=0; j<colIt.size(); j++) {
                localResults[relColId++].push_back(*colIt[j]);
            }
        }
        for (unsigned j=0; j<colIt.size(); j++) {
            ++colIt[j];
        }
    }
    if (localResults[0].size() < 0) 
        goto probing_finish;
    for (unsigned i=0; i<requestedColumns.size(); i++)  {
		results[i].addTuples(taskId, localResults[i].data(), localResults[i].size());
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());

probing_finish:  
    int remainder = __sync_sub_and_fetch(&pendingProbing, 1);
    if (UNLIKELY(remainder == 0)) {
        for (unsigned cId=0;cId<requestedColumns.size();++cId) {
            results[cId].fix();
        }
        finishAsyncRun(*ioService, true); 
    }
}
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo info)
// Require a column and add it to results
{
    if (requiredIUs.count(info))
        return true;
    if(input->require(info)) {
        requiredIUs.emplace(info);
        return true;
    }
    return false;
}
//---------------------------------------------------------------------------
void SelfJoin::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 1;
    input->require(pInfo.left);
    input->require(pInfo.right);
    __sync_synchronize();
    input->asyncRun(ioService);
}
//---------------------------------------------------------------------------
void SelfJoin::selfJoinTask(boost::asio::io_service* ioService, int taskIndex, unsigned start, unsigned length) {
    auto& inputData=input->getResults();
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    auto leftColId=input->resolve(pInfo.left);
    auto rightColId=input->resolve(pInfo.right);

    auto leftColIt=inputData[leftColId].begin(start);
    auto rightColIt=inputData[rightColId].begin(start);

    vector<Column<uint64_t>::Iterator> colIt;
    
    for (unsigned i=0; i<copyData.size(); i++) {
        colIt.push_back(copyData[i]->begin(start));
    }
    for (uint64_t i=start, limit=start+length;i<limit;++i) {
        if (*leftColIt==*rightColIt) {
            for (unsigned cId=0;cId<copyData.size();++cId) {
                localResults[cId].push_back(*(colIt[cId]));
            }
        }
        ++leftColIt;
        ++rightColIt;
        for (unsigned i=0; i<copyData.size(); i++) {
            ++colIt[i];
        }
    }
    //local results가 0일 경우??? ressult, tmp 초기화
    for (int i=0; i<copyData.size(); i++) {
        results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        for (unsigned cId=0;cId<copyData.size();++cId) {
            results[cId].fix();
        }
        finishAsyncRun(*ioService, true);
    }
}
//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif

    if (input->resultSize == 0) {
        finishAsyncRun(ioService, true);
        return;
    }
    
    auto& inputData=input->getResults();
    int cntTask = THREAD_NUM;
    unsigned taskLength = (input->resultSize+cntTask-1)/cntTask;
    
    for (auto& iu : requiredIUs) {
        auto id=input->resolve(iu);
        copyData.emplace_back(&inputData[id]);
        select2ResultColId.emplace(iu,copyData.size()-1);
		results.emplace_back(cntTask);
    }


    for (int i=0; i<cntTask; i++) {
        tmpResults.emplace_back();
        for (int j=0; j<copyData.size(); j++) {
            tmpResults[i].emplace_back();
        }
    } 
    
    if (input->resultSize/cntTask < minTuplesPerTask) {
        cntTask = (input->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength = minTuplesPerTask;
    }
    
    pendingTask = cntTask; 
    __sync_synchronize();
    unsigned length = taskLength;
    for (int i=0; i<cntTask; i++) {
        unsigned start = i*length;
        if (i == cntTask-1 && input->resultSize%length) {
            length = input->resultSize%length;
        }
        ioService.post(bind(&SelfJoin::selfJoinTask, this, &ioService, i, start, length)); 
    }
}
//---------------------------------------------------------------------------
void Checksum::asyncRun(boost::asio::io_service& ioService, int queryIndex) {
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex << ")::asyncRun()" << endl;
#endif
    this->queryIndex = queryIndex;
    pendingAsyncOperator = 1;
    for (auto& sInfo : colInfo) {
        input->require(sInfo);
    }
    __sync_synchronize();
    input->asyncRun(ioService);
//    cout << "Checksum::asyncRun" << endl;
}
//---------------------------------------------------------------------------

void Checksum::checksumTask(boost::asio::io_service* ioService, int taskIndex, unsigned start, unsigned length) {
    auto& inputData=input->getResults();
     
    int sumIndex = 0;
    for (auto& sInfo : colInfo) {
        auto colId = input->resolve(sInfo);
        auto inputColIt = inputData[colId].begin(start);
        uint64_t sum=0;
        for (int i=0; i<length; i++,++inputColIt)
            sum += *inputColIt;
        __sync_fetch_and_add(&checkSums[sumIndex++], sum);
    }
    
    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        finishAsyncRun(*ioService, false);
    }
     
}
//---------------------------------------------------------------------------
void Checksum::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ")::createAsyncTasks" << endl;
#endif
    for (auto& sInfo : colInfo) {
        checkSums.push_back(0);
    }
    if (input->resultSize == 0) {
        finishAsyncRun(ioService, false);
        return;
    }
    
    int cntTask = THREAD_NUM;
    unsigned taskLength = (input->resultSize+cntTask-1)/cntTask;
    
    if (input->resultSize/cntTask < minTuplesPerTask) {
        cntTask = (input->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength = minTuplesPerTask;
    }
#ifdef VERBOSE 
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ") cntTask: " << cntTask << " length: " << taskLength <<  endl;
#endif
    pendingTask = cntTask; 
    __sync_synchronize();
    unsigned length = taskLength;
    for (int i=0; i<cntTask; i++) {
        unsigned start = i*length;
        if (i == cntTask-1 && input->resultSize%length) {
            length = input->resultSize%length;
        }
        ioService.post(bind(&Checksum::checksumTask, this, &ioService, i, start, length)); 
    }
}
void Checksum::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    joiner.asyncResults[queryIndex] = std::move(checkSums);
    int pending = __sync_sub_and_fetch(&joiner.pendingAsyncJoin, 1);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ") finish query index: " << queryIndex << " rest quries: "<< pending << endl;
#endif
    assert(pending >= 0);
    if (pending == 0) {
#ifdef VERBOSE
        cout << "A query set is done. " << endl;
#endif
        unique_lock<mutex> lk(joiner.cvAsyncMt); // guard for missing notification
        joiner.cvAsync.notify_one();
    }
}
//---------------------------------------------------------------------------
void Checksum::printAsyncInfo() {
	cout << "pendingChecksum : " << pendingTask << endl;
	input->printAsyncInfo();
}

void SelfJoin::printAsyncInfo() {
	cout << "pendingSelfJoin : " << pendingTask << endl;
	input->printAsyncInfo();
}
void Join::printAsyncInfo() {
	
}
void FilterScan::printAsyncInfo() {
	cout << "pendingFilterScan : " << pendingTask << endl;
}
void Scan::printAsyncInfo() {
}
