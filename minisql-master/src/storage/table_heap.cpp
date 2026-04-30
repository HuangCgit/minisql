#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
    page_id_t current_page_id = first_page_id_;
    TablePage *page = nullptr;
    while (current_page_id != INVALID_PAGE_ID) {
        page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
        if (page == nullptr) return false;
        page->WLatch();
        if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, true);
            return true;
        }
        page->WUnlatch();
        auto prev_page_id = page->GetTablePageId();
        current_page_id = page->GetNextPageId();
        buffer_pool_manager_->UnpinPage(prev_page_id, false);
        if (current_page_id == INVALID_PAGE_ID) break; // Last page
    }

    // Allocate a new page
    page_id_t new_page_id;
    auto new_page_ptr = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page_ptr == nullptr) return false;
    auto new_page = reinterpret_cast<TablePage *>(new_page_ptr);

    page_id_t prev_page_id = (page == nullptr) ? INVALID_PAGE_ID : page->GetTablePageId();
    new_page->WLatch();
    new_page->Init(new_page_id, prev_page_id, log_manager_, txn);

    if (page != nullptr) { // Link from previous page
        auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
        prev_page->WLatch();
        prev_page->SetNextPageId(new_page_id);
        prev_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page_id, true);
    } else { // First page
        first_page_id_ = new_page_id;
    }

    bool success = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_page_id, success);
    return success;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
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
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page == nullptr) {
    return false;
  }
  Row old_row(rid);
  page->WLatch();
  int result = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(result == 0) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); // No changes
    return false;
  }
  else if(result == 1) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true); // In-place update
    return true;
  }
  else { // result == 2, not enough space, need to move
    // The page->UpdateTuple call didn't modify the page.
    // We need to mark the old tuple as deleted and insert the new one.
    if (!page->MarkDelete(rid, txn, lock_manager_, log_manager_)) {
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return false;
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return InsertTuple(row, txn);
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
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
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page == nullptr) {
    return false;
  }
  page->RLatch();
  if(page->GetTuple(row, schema_, txn, lock_manager_))
  {    
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return true;
  }
  else
  {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  page_id_t page_id = first_page_id_;
  if(page_id == INVALID_PAGE_ID) {
    return End();
  }
  while(page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(page == nullptr) {
      return End();
    }
    RowId first_rid;
    if(page->GetFirstTupleRid(&first_rid)) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      return TableIterator(this, first_rid, txn);
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = page->GetNextPageId();
  }
  return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr); }
