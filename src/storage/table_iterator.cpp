#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *tableheap, Row *row): tableheap_(tableheap), row_(row) {}

TableIterator::TableIterator(const TableIterator &other) {
  tableheap_ = other.tableheap_;
  row_ = new Row(*other.row_);
}

TableIterator::~TableIterator() {

}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator++() {
  RowId this_rid = row_->GetRowId();
  page_id_t this_page_id = this_rid.GetPageId();
  auto this_page = reinterpret_cast<TablePage *>(tableheap_->buffer_pool_manager_->FetchPage(this_page_id));

  RowId *next_rid = new RowId;
  if (!this_page->GetNextTupleRid(row_->GetRowId(), next_rid))//there is no next record in this page, so need to change to next page
  {
    do
    {
      page_id_t next_page_id = this_page->GetNextPageId();
      buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
      this_page_id = next_page_id;
      if (this_page_id == INVALID_PAGE_ID) //find next page is invalid, so is the end
      {
        row_->SetRowId(INVALID_ROWID);
        return *this;
      }
      this_page = reinterpret_cast<TablePage *>(tableheap_->buffer_pool_manager_->FetchPage(this_page_id));//get next page
    } while (!this_page->GetFirstTupleRid(next_rid));//if false, means this page is deleted all, so go next
  }

  //after get next rid, we need update the row and return
  row_->SetRowId(*next_rid);
  bool updateRow_ret = this_page->GetTuple(row_, tableheap_->schema_, nullptr, tableheap_->lock_manager_);
  ASSERT(updateRow_ret == true, "wsx_tableiterator++ error!");

  delete next_rid;
  buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
  return *this;
}

TableIterator TableIterator::operator++(int) {
  Row old_row(*row_);

  RowId this_rid = row_->GetRowId();
  page_id_t this_page_id = this_rid.GetPageId();
  auto this_page = reinterpret_cast<TablePage *>(tableheap_->buffer_pool_manager_->FetchPage(this_page_id));

  RowId *next_rid = new RowId;
  if (!this_page->GetNextTupleRid(row_->GetRowId(), next_rid))//there is no next record in this page, so need to change to next page
  {
    do
    {
      page_id_t next_page_id = this_page->GetNextPageId();
      buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
      this_page_id = next_page_id;
      if (this_page_id == INVALID_PAGE_ID) //find next page is invalid, so is the end
      {
        row_->SetRowId(INVALID_ROWID);
      }
      this_page = reinterpret_cast<TablePage *>(tableheap_->buffer_pool_manager_->FetchPage(this_page_id));//get next page
    } while (!this_page->GetFirstTupleRid(next_rid));//if false, means this page is deleted all, so go next
  }

  //after get next rid, we need update the row and return
  row_->SetRowId(*next_rid);
  bool updateRow_ret = this_page->GetTuple(row_, tableheap_->schema_, nullptr, tableheap_->lock_manager_);
  ASSERT(updateRow_ret == true, "wsx_tableiterator++ error!");

  delete next_rid;
  buffer_pool_manager_->UnpinPage(this_page_id, false);//将该页unpin
  return TableIterator(tableheap_, &old_row);
}
