#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  bool flag;
  if (this->len > 0) {
    *frame_id = this->lrulist.back();
    this->lrumap.erase(*frame_id);
    this->lrulist.pop_back();
    this->len--;
    flag = true;
  }
  else {
      frame_id = nullptr;
      flag = false;
  }
  return flag;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (this->lrumap.find(frame_id) != this->lrumap.end()) {
    this->lrulist.erase(this->lrumap[frame_id]);
    this->lrumap.erase(frame_id);
    this->len--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (this->lrumap.find(frame_id) == this->lrumap.end()) {
    this->lrulist.push_front(frame_id);
    this->lrumap.insert(std::make_pair(frame_id, this->lrulist.begin()));
    this->len++;
  }
}

size_t LRUReplacer::Size() { 
  return this->len; 
}
