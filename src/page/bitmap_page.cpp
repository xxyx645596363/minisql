#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  uint32_t MaxSupportedSize = GetMaxSupportedSize();
  if (page_allocated_ >= MaxSupportedSize)//Bitmap pages have no free pages left to allocate
  {
    return false;
  }
  else
  {
    page_offset = next_free_page_;
    bytes[next_free_page_ / 8] |= ( 1 << (next_free_page_ % 8) );//the bit of page allocated turned from 0 to 1

    page_allocated_++;//the number of pages allocated + 1
    next_free_page_++;
    if (page_allocated_ < MaxSupportedSize)//if no more free page, then no need to update the next free page because it doesn't exsit
    {
      for (int i = next_free_page_; ;i = (i + 1) % MaxSupportedSize)//loop for finding the next free page
      {
        if ( !( bytes[i / 8] & ( 1 << ( i % 8 ) ) ) )//check if the page is free by checking if its bit is 0 
        {
          next_free_page_ = i;//update the new free page
          break;
        }
      }
    }

    return true;
  }
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t MaxSupportedSize = GetMaxSupportedSize();
  int i_byte = page_offset / 8;
  int i_bit = 1 << ( page_offset % 8 );
  if ( !( bytes[i_byte] & i_bit ) ) return false;//if the page is already free, return false;
  else
  {
    bytes[i_byte] &= ~i_bit;//update the bit of the page freed to be 0

    page_allocated_--;//the number of pages allocated - 1

    if (page_allocated_ + 1 == MaxSupportedSize)//if the original bitmap is full, after free a page, the next_free_page must be updated to this page
    {
      next_free_page_ = page_offset;
    }

    return true;
  }
  
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  int i_byte = page_offset / 8;
  int i_bit = 1 << ( page_offset % 8 );
  if ( !( bytes[i_byte] & i_bit ) ) return true;
  else return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;