#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  table_heap_ = table_heap;
  row = Row(rid);
  txn_ = txn;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row = other.row;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (table_heap_ == itr.table_heap_ && row.GetRowId() == itr.row.GetRowId() && txn_ == itr.txn_) {
    return true;
  }
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return row;
}

Row *TableIterator::operator->() {
  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  row = itr.row;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (table_heap_ == nullptr || *this == table_heap_->End()) {
    return *this;
  }
  RowId next_row_id;
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row.GetRowId().GetPageId()));
  if(page == nullptr) {
    return *this;
  }
  page->RLatch();
  if(page->GetNextTupleRid(row.GetRowId(), &next_row_id)) {
    row.SetRowId(next_row_id);
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
  }
  else{
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next_page_id = page->GetNextPageId();
    while(next_page_id != INVALID_PAGE_ID) {
      auto next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      if(next_page == nullptr) {
        return *this;
      }
      next_page->RLatch();
      if(next_page->GetFirstTupleRid(&next_row_id)) {
        row.SetRowId(next_row_id);
        next_page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
        return *this;
      }
      else {
        next_page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
        next_page_id = next_page->GetNextPageId();
      }
    }
  }
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator temp(*this);
  ++(*this);
  return TableIterator(temp);
 }
