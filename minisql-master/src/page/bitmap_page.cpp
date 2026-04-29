#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= GetMaxSupportedSize()) {
    return false;
  }
  // Find the first free page by circularly searching from next_free_page_
  for (uint32_t i = 0; i < GetMaxSupportedSize(); ++i) {
    uint32_t current_offset = (next_free_page_ + i) % GetMaxSupportedSize();
    if (IsPageFree(current_offset)) {
      page_offset = current_offset;
      // Mark the page as used (set the bit to 1)
      uint32_t byte_index = page_offset / 8;
      uint8_t bit_index = page_offset % 8;
      bytes[byte_index] |= (1 << (7 - bit_index));
      page_allocated_++;
      // Update the hint for the next allocation
      next_free_page_ = (page_offset + 1) % GetMaxSupportedSize();
      return true;
    }
  }
  // Should not be reached if page_allocated_ is consistent with the bitmap
  return false; 
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if(page_offset >= GetMaxSupportedSize() || IsPageFree(page_offset)) {
      return false;
    }
    bytes[page_offset / 8] &= ~(1 << (7 - page_offset % 8));
    page_allocated_--;
    if(page_offset < next_free_page_) {
      next_free_page_ = page_offset;
    }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (byte_index >= MAX_CHARS) {
    return false;
  }
  // The original code had an operator precedence bug. It should be (A & B) == 0.
  return (bytes[byte_index] & (1 << (7 - bit_index))) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;