#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap* Table_heap,Transaction* txn,RowId _rid):_table_heap(Table_heap),_txn(txn){

  if(_rid.GetPageId() != INVALID_PAGE_ID){
     _current_row = new Row(_rid);
    ASSERT(Table_heap->GetTuple(_current_row,txn),"TableIterator::TableIterator invalid rowid");
  } else{
    _current_row = new Row(INVALID_ROWID);
  }

}

TableIterator::TableIterator(const TableIterator &other) ://拷贝构造函数不显示调用
 _table_heap(other._table_heap), _txn(other._txn){
  _current_row = new Row(*other._current_row); // 创建一个新的 Row 对象，并复制数据
}

TableIterator::~TableIterator() {
  delete _current_row;
}

bool TableIterator::operator==(const TableIterator &itr) const {//存在问题
  /*首先判断大小是否一致，类型是否相同，再去比较 每一条一个row 里的每一条 field*/

  return(_current_row->GetRowId().Get() == itr._current_row->GetRowId().Get());
/*
  if(_current_row->GetFieldCount() !=itr._current_row->GetFieldCount()){//大小都不一样
     return false;
  }
  uint32_t _size_ = _current_row->GetFieldCount();
  vector<Field*> _Fields1 = _current_row->GetFields();//row 中存储的field 是field指针，field*
  vector<Field*> _Fields2 = itr._current_row->GetFields();
  for(::uint32_t i = 0; i < _size_; i++){//比较每个 row 它们的每个field 中的内容是否相同
     if(_Fields1[i]->CompareEquals(*_Fields2[i]) == 0){//有不相等记录
       LOG(INFO) << "TableIterator::operator== 有不相等记录";
       return false ;
     }
  }
  LOG(INFO) << "TableIterator::operator== 没有不相等记录";
  return true;*/
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !((*this) == itr);// == 反过来
}

const Row &TableIterator::operator*() {
//  LOG(INFO) << "TableIterator::operator*";
  ASSERT(*this != _table_heap->End(), "TableHeap iterator out of range");
  return *_current_row;
}

Row *TableIterator::operator->() {
  LOG(INFO) << "TableIterator::operator->";
  ASSERT(*this != _table_heap->End(), "TableHeap iterator out of range");
  return _current_row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {//实现赋值运算
  if (this != &itr) {
     _table_heap = itr._table_heap;
     _txn = itr._txn;
     delete _current_row; // 删除当前的 _current_row 对象
     _current_row = new Row(*itr._current_row); // 创建一个新的 Row 对象，并复制数据
  }
  return *this;
}

// ++iter

TableIterator &TableIterator::operator++() {
  FindNextValidTuple();
  return *this;
}
/*
TableIterator &TableIterator::operator++() {
  // 如果当前已经是非法的iter，则返回nullptr构成的
  if (_current_row == nullptr || _current_row->GetRowId() == INVALID_ROWID) {
     delete _current_row;
     _current_row = new Row(INVALID_ROWID);
     return *this;
  }
  // 先找本页之后的row
  auto page = reinterpret_cast<TablePage *>(_table_heap->buffer_pool_manager_->FetchPage(_current_row->GetRowId().GetPageId()));
  RowId new_id;
  page->RLatch();
  // 如果能找到则直接读
  if (page->GetNextTupleRid(_current_row->GetRowId(), &new_id)) {
     delete _current_row;
     _current_row = new Row(new_id);
     if (*this != _table_heap->End()) {
       _table_heap->GetTuple(_current_row, nullptr);
     }
     page->RUnlatch();
     _table_heap->buffer_pool_manager_->UnpinPage(_current_row->GetRowId().GetPageId(), false);
  } else {
     // 本页没有合适的，去找下一页直到找到可以用的页
     page_id_t next_page_id = page->GetNextPageId();
     while (next_page_id != INVALID_PAGE_ID) {
       // 还没到最后一页
       auto new_page = reinterpret_cast<TablePage *>(_table_heap->buffer_pool_manager_->FetchPage(next_page_id));
       page->RUnlatch();
       _table_heap->buffer_pool_manager_->UnpinPage(_current_row->GetRowId().GetPageId(), false);
       page = new_page;
       page->RLatch();
       if (page->GetFirstTupleRid(&new_id)) {
         // 如果找到可用的tuple则跳出循环并读这个tuple
         delete _current_row;
         _current_row = new Row(new_id);
         break;
       }
       next_page_id = page->GetNextPageId();
     }
     // 如果next_page_id不是非法的则可以读取tuple,否则需要返回nullptr构成的iter
     if (next_page_id != INVALID_PAGE_ID) {
       _table_heap->GetTuple(_current_row, nullptr);
     } else {
       delete _current_row;
       _current_row = new Row(INVALID_ROWID);
     }
     page->RUnlatch();
     _table_heap->buffer_pool_manager_->UnpinPage(_current_row->GetRowId().GetPageId(), false);
  }
  return *this;
}*/

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator old_iterator(*this);
  ++(*this);
  return old_iterator;
}


bool TableIterator::FindNextValidTuple() {
  RowId* next_rid;
  page_id_t current_page_id = _current_row->GetRowId().GetPageId();
  page_id_t next_page_id = INVALID_PAGE_ID;
  auto page = reinterpret_cast<TablePage*>(_table_heap->buffer_pool_manager_->FetchPage(current_page_id));
  if (page != nullptr) {//如果本页有可用的
     page->RLatch();
     if (page->GetNextTupleRid(_current_row->GetRowId(), next_rid) == true) {
       _current_row->SetRowId(*next_rid);
       _table_heap->GetTuple(_current_row, _txn);
       _table_heap->buffer_pool_manager_->UnpinPage(current_page_id, false);  // Unpin the current page
       page->RUnlatch();
       return true;
     } else {//如果本页没有可用的，直接到下一页去找，直到找到为止
       page->RUnlatch();
       _table_heap->buffer_pool_manager_->UnpinPage(current_page_id, false);  // Unpin the current page
       while ((current_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {
         auto page = reinterpret_cast<TablePage*>(_table_heap->buffer_pool_manager_->FetchPage(current_page_id));
         page->RLatch();
         if(page->GetFirstTupleRid(next_rid)){
           _current_row->SetRowId(*next_rid);
           _table_heap->GetTuple(_current_row,_txn);
          // _table_heap->buffer_pool_manager_->UnpinPage(current_page_id, false);  // Unpin the current page
           page->RUnlatch();
           break ;
         } else{
           page->RUnlatch();
           _table_heap->buffer_pool_manager_->UnpinPage(current_page_id, false);
           //continue
         }
       }
     }
  }else {
       LOG(INFO) << "TableIterator::FindNextValidTuple page is nullptr";
       return false;
  }


  if (current_page_id == INVALID_PAGE_ID) {
     _current_row->SetRowId(RowId(-1, 0));
     return true;
  }

}
