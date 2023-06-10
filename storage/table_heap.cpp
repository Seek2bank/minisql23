#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The transaction performing the insert
   * @return true iff the insert is successful
   */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    if (row.GetSerializedSize(schema_) >= PAGE_SIZE) {
      return false; // 如果元组大小大于等于页大小，返回 false
    }
    //RowId的修改在 table_page内部的Insert tuple函数修改
    page_id_t page_id = first_page_id_;
    page_id_t lase_page_id = -1;
    // 在所有页面中寻找有足够空间插入元组的页面

    while (page_id != INVALID_PAGE_ID){// if page_id = INVALID_PAGE_ID,直接创建新页
      auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
       if(page == nullptr){
         LOG(INFO)<<"TableHeap::InsertTuple page == nullptr";
         return false;// 如果页面无法获取，则返回 false
       }
       page->WLatch();//写锁定

       if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
         page->WUnlatch();//写解锁
         buffer_pool_manager_->UnpinPage(page_id, true);//
         return true;
       }//这一页空闲空间足够，插入成功
       page->WUnlatch();//写解锁
       buffer_pool_manager_->UnpinPage(page_id, false);//这是干什么用的
       lase_page_id = page_id;
       page_id = page->GetNextPageId();
    }
    // If no page has enough space, create a new page and insert the tuple，以下是对新页的操作
    auto new_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(page_id));
    new_page->WLatch();
    new_page->Init(page_id,lase_page_id, nullptr, nullptr);//初始化新的一页，它的前一页是之前链表的最后一页
    new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
    new_page->WUnlatch(); // 写解锁
    buffer_pool_manager_->UnpinPage(page_id, true);

    // 更新前一页的下一页id
    auto prev_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(lase_page_id));
    prev_page->WLatch(); // 获取写锁定
    prev_page->SetNextPageId(page_id);
    prev_page->WUnlatch(); // 写解锁
    buffer_pool_manager_->UnpinPage(lase_page_id, true); // 解除固定，并将页面标记为脏页
    return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
/**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Transaction performing the update
   * @return true is update is successful.
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  //找到所在的页
  if(page == nullptr){
    LOG(INFO)<< "UpdateTuple::page == nullptr 找不到指定页面";
    return false;
  }
  // Create a copy of the old row
  auto old_row = new Row(rid);//交给智能指针管理内存
  if(!GetTuple(old_row,txn)){
    return false;
  }
  // Acquire write latch on the page
  page->WLatch();
  int update_result = page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_);
  //delete old_row;//用完old_row 释放内存？？
  page->WUnlatch();

  if (update_result == 0) {
    //LOG(INFO) << "UpdateTuple: successfully update tuple.";
    // Update the RowId of the new_row
    row.SetRowId(rid);
    // Unpin the page and release the latch
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;  // Update successful
  } else {
    // Handle different update result cases
    switch (update_result) {
         case 1:
           LOG(INFO) << "UpdateTuple: Invalid slot number.";
           buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
           return false;
         case 2:
           LOG(INFO) << "UpdateTuple: Tuple is deleted.";
           buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
           return false;
         case 3:
        //   LOG(INFO) << "UpdateTuple: Not enough space to update tuple.";
        //   LOG(INFO) << "UpdateTuple: Mark this Tuple deleted";
           buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
           page->MarkDelete(rid,txn,lock_manager_, log_manager_);
           if(InsertTuple(row,txn)){
             buffer_pool_manager_->UnpinPage(row.GetRowId().GetPageId(), true);
         //    LOG(INFO) << "UpdateTuple: Insert the updated tuple";
             return true;
           }
           buffer_pool_manager_->UnpinPage(row.GetRowId().GetPageId(), false);
           LOG(INFO) << "UpdateTuple: Fail to update tuple.";
           return false;

         default:
           LOG(INFO) << "UpdateTuple: Unknown update result.";
           break;
    }
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {

  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"UpdateTuple::page == nullptr 找不到指定页面");

 // Step2: Delete the tuple from the page.
    page->WLatch();
    page->ApplyDelete(rid,txn,log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
/**
   * Read a tuple from the table.
   * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
   * @param[in] txn transaction performing the read
   * @return true if the read was successful (i.e. the tuple exists)
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  // Step1: Find the page which contains the tuple.
  if(page == nullptr){
    ASSERT(page == nullptr,"GetTuple::page == nullptr 找不到指定页面") ;
    return false;
  }else{
    bool GetTuple_result = page->GetTuple(row,schema_,txn,lock_manager_);
    return GetTuple_result;
  }

}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  page_id_t page_id =  this->GetFirstPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  page->GetFirstTupleRid(&rid);
  return TableIterator(this,txn,rid); //begin的调用
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, nullptr, INVALID_ROWID);//end 的调用
}

