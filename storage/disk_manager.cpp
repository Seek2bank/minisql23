#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
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

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  uint32_t meta_copy[PAGE_SIZE / 4];
  memcpy(meta_copy, meta_data_, PAGE_SIZE);
  uint32_t id = 0;
  while (meta_copy[id + 2] == BITMAP_SIZE) {
    id++;
  }
  // Search the extent for the bitmap
  char bitmap_info[PAGE_SIZE];
  page_id_t physical_id = id * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(physical_id, bitmap_info);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_info);
  uint32_t bitmap_id = bitmap->next_free_page_;
  bitmap->AllocatePage(bitmap_id);
  DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  //  meta_page->extent_used_page_[id]++;
  //  meta_page->num_allocated_pages_++;
  //  meta_page->num_extents_++;
  // Update meta_data
  if (id >= meta_copy[1]) {
    meta_copy[1]++;
  }
  meta_copy[2 + id]++;
  meta_copy[0]++;
  memcpy(meta_data_, meta_copy, PAGE_SIZE);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(physical_id, bitmap_info);
  return size_t(bitmap_id + id * BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t meta_copy[PAGE_SIZE / 4];
  memcpy(meta_copy, meta_data_, PAGE_SIZE);
  uint32_t id = logical_page_id / BITMAP_SIZE;
  // Search the extent for the bitmap
  char bitmap_info[PAGE_SIZE];
  page_id_t physical_id = MapPageId(logical_page_id) / (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(physical_id, bitmap_info);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_info);
  uint32_t bitmap_id = logical_page_id % BITMAP_SIZE;
  bitmap->DeAllocatePage(bitmap_id);
  DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  //  meta_page->extent_used_page_[id]--;
  //  meta_page->num_allocated_pages_--;
  //  meta_page->num_extents_--;
  if (meta_copy[2+id]==0) {
    meta_copy[1]--;
  }
  meta_copy[2+id]--;
  meta_copy[0]--;
  memcpy(meta_data_,meta_copy,PAGE_SIZE);
  // Update meta_data
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(physical_id, bitmap_info);
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  //return false;
  uint32_t meta_copy[PAGE_SIZE];
  memcpy(meta_copy, meta_data_, PAGE_SIZE);
  char bitmap_info[PAGE_SIZE];
  ReadPhysicalPage(1 + MapPageId(logical_page_id)* (BITMAP_SIZE + 1), bitmap_info);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_info);
  return bitmap->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  int id = logical_page_id/BITMAP_SIZE;
  return logical_page_id + 2 + id;
}

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

