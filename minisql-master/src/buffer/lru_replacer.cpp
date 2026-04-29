#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_frames.empty()) {
    return false;
  }
  unordered_set<frame_id_t>::iterator it= lru_frames.begin();
for(int i=0; it != lru_frames.end(); it++,i++) {
  if(i == lru_frames.size() - 1) {
    *frame_id = *it;
    lru_frames.erase(it);
    return true;
  }
}
return false;
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_frames.find(frame_id) == lru_frames.end()) {
    return;
  }
  lru_frames.erase(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(lru_frames.size() < capacity_) {
    lru_frames.insert(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_frames.size();
}