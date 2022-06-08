#include "storage/table_heap.h"

//wsx_start1

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  //check if some current pages are enough for the new row
  page_id_t this_page_id = first_page_id_;
  for (; this_page_id != INVALID_PAGE_ID; )
  {
    auto this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_page_id));
    if (this_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
    {
      buffer_pool_manager_->UnpinPage(this_page_id, true);//将该页unpin
      return true;
    }
    page_id_t next_page_id = this_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
    if (next_page_id == INVALID_PAGE_ID) break;
    this_page_id = next_page_id;
  }

  //all current pages are not enough for the new row, so we need to create a new page and insert it into the double link list
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  new_page->Init(new_page_id, this_page_id, log_manager_, txn);
  if (new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
  {
    //insert it into the double link list
    if (this_page_id != INVALID_PAGE_ID) {
      auto end_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_page_id));
      end_page->SetNextPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(this_page_id, true);//将该页unpin
    }
    else  first_page_id_ = new_page_id; 
    buffer_pool_manager_->UnpinPage(new_page_id, true);//将该页unpin
    return true;
  }
  buffer_pool_manager_->UnpinPage(new_page_id, false);//将该页unpin
  return false;
}

// bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
//   // std::cout << "TableHeap::InsertTuple first_page_id_: " << first_page_id_ << std::endl;
//   //check if some current pages are enough for the new row
//   page_id_t this_page_id = first_page_id_, next_page_id;
//   TablePage *this_page;
//   for (; this_page_id != INVALID_PAGE_ID; )
//   {
//     this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_page_id));
//     if (this_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
//     {
//       buffer_pool_manager_->UnpinPage(this_page_id, true);//将该页unpin
//       return true;
//     }
//     next_page_id = this_page->GetNextPageId();
//     buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
//     if (next_page_id == INVALID_PAGE_ID) break;
//     this_page_id = next_page_id;
//   }

//   // all current pages are not enough for the new row, so we need to create a new page and insert it into the double link list
//   page_id_t new_page_id;
//   auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
//   new_page->Init(new_page_id, INVALID_PAGE_ID, log_manager_, txn);
//   new_page->SetNextPageId(first_page_id_);
//   if (new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
//   {
//     //insert it into the !!head!! of double link list
//     if (first_page_id_ != INVALID_PAGE_ID) {//当前有page存在
//       this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//       this_page->SetPrevPageId(new_page_id);
//       buffer_pool_manager_->UnpinPage(first_page_id_, true);//将该页unpin
//     }
//     first_page_id_ = new_page_id; 
//     buffer_pool_manager_->UnpinPage(new_page_id, true);//将该页unpin
//     return true;
//   }
//   buffer_pool_manager_->UnpinPage(new_page_id, false);//将该页unpin
//   return false;
// }

//wsx_end1

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

//wsx_start2

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  page_id_t this_page_id = rid.GetPageId();
  auto this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_page_id));
  // If the page could not be found, return false
  if (this_page == nullptr) {
    buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
    return false;
  }

  Row old_row(rid);
  // if (!this_page->GetTuple(&old_row, schema_, txn, lock_manager_))
  // {
  //   buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
  //   return false;
  // }

  int update_ret = this_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (update_ret == 1)
  {
    buffer_pool_manager_->UnpinPage(this_page_id, true);//将该页unpin
    return true;
  } 
  else if (update_ret == 2)//current page is no enough for the new row, so we delete and insert again
  {
    buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
    bool ret_delete = MarkDelete(rid, txn);
    bool ret_insert = InsertTuple(*(const_cast<Row *>(&row)), txn);
    return ret_delete && ret_insert;
  }
  else
  {
    buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
    return false;
  } 
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
   auto this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // Step2: Delete the tuple from the page.
  this_page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);//将该页unpin
}

//wsx_end2

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

//wsx_start3

void TableHeap::FreeHeap() {
  delete schema_;
  delete log_manager_;
  delete lock_manager_;
  delete buffer_pool_manager_;
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId this_rid = row->GetRowId();
  // std::cout << "TableHeap::GetTuple this_rid.GetPageId(): " << this_rid.GetPageId() << std::endl;
  auto this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_rid.GetPageId()));
  buffer_pool_manager_->UnpinPage(this_rid.GetPageId(), false);//将该页unpin
  if (this_page == nullptr) return false;
  return this_page->GetTuple(row, schema_, txn, lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {

  RowId first_rid;
  page_id_t this_page_id = first_page_id_;
  auto this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_page_id));//get first page
  while (!this_page->GetFirstTupleRid(&first_rid))//if false, the page's record is delete all, then change to next page
  {
    this_page_id = this_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
    if (this_page_id == INVALID_PAGE_ID)
    {
      return End();//find next page is invalid, return null
    } 
    this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this_page_id));//get next page
  }
  
  Row *first_row = new Row(first_rid);
  if (this_page->GetTuple(first_row, schema_, txn, lock_manager_))
  {
    buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
    return TableIterator(this, first_row);
  }
  buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
  return End();
}

TableIterator TableHeap::End() {
  // Row end_row(INVALID_ROWID);
  // return TableIterator(this, &end_row);
  Row *end_row = new Row(INVALID_ROWID);
  return TableIterator(this, end_row);
}

//wsx_end3