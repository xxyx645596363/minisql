#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

//wsx_start

page_id_t DiskManager::AllocatePage() {
  auto * metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  
  if (metaPage->num_allocated_pages_ >= MAX_VALID_PAGE_ID)//No more free pages
  {
    return INVALID_PAGE_ID;
  }
  else
  {
    if (metaPage->num_allocated_pages_ == metaPage->num_extents_ * BITMAP_SIZE) metaPage->num_extents_++;//expand the number of extents if every extent which is used before is full

    metaPage->num_allocated_pages_++;//The total number of page ++

    for (uint32_t i = 0; i < metaPage->num_extents_; i++)
    {
      if (metaPage->extent_used_page_[i] >= BITMAP_SIZE) continue;//No free pages of this extent

      metaPage->extent_used_page_[i]++;//The number of pages of this extent++

      char page_data[PAGE_SIZE];
      uint32_t i_page;

      ReadPhysicalPage(1 + i * (BITMAP_SIZE + 1), page_data);//read bit map page data
      auto * mapPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
      if ( mapPage->AllocatePage(i_page) == false ) return INVALID_PAGE_ID;//allocate failed
      else
      {
        WritePhysicalPage(1 + i * (BITMAP_SIZE + 1), page_data);//write bit map page data
        WritePhysicalPage(0, meta_data_);
        return i * BITMAP_SIZE + i_page;
      }
    }
  }
  return INVALID_PAGE_ID;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t i_extent = logical_page_id / BITMAP_SIZE;
  uint32_t pi_bitmap = i_extent * (BITMAP_SIZE + 1) + 1; //the Physica id of bitmap page
  char bitmap_data[PAGE_SIZE];
  ReadPhysicalPage(pi_bitmap, bitmap_data);
  auto *mapPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);

  if (mapPage->DeAllocatePage(logical_page_id % BITMAP_SIZE))
  {
    WritePhysicalPage(pi_bitmap, bitmap_data);
    auto * metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_);//update the data of bit map page
    metaPage->num_allocated_pages_--;
    metaPage->extent_used_page_[i_extent]--;
    WritePhysicalPage(0, meta_data_);
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t i_extent = logical_page_id / 3;
  uint32_t pi_bitmap = i_extent * (BITMAP_SIZE + 1) + 1; //the Physica id of bitmap page
  char *bitmap_data = (char *)malloc(PAGE_SIZE);
  ReadPhysicalPage(pi_bitmap, bitmap_data);
  auto *mapPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
  if (mapPage->IsPageFree(logical_page_id % BITMAP_SIZE)) return true;
  else return false;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return 2 + logical_page_id / 3 + logical_page_id;
}

//wsx_end

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}