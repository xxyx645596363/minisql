#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);//write magic_num
  buf += sizeof(uint32_t);//update the buf
  
  MACH_WRITE_UINT32(buf, table_id_);//write table_id_
  buf += sizeof(uint32_t);

  // MACH_WRITE_STRING(buf, table_name_);//write the name of the table
  // buf += sizeof(std::string);
  MACH_WRITE_UINT32(buf, table_name_.length());//write magic_num
  buf += sizeof(uint32_t);//update the buf
  MACH_WRITE_STRING(buf, table_name_);//write the name of the table
  buf += table_name_.length();


  MACH_WRITE_UINT32(buf, root_page_id_);//write the root page id
  buf += sizeof(uint32_t);

  buf += schema_->SerializeTo(buf);

  MACH_WRITE_UINT32(buf, prim_idx_);//write prim_idx_
  buf += sizeof(uint32_t);

  return GetSerializedSize();
}

uint32_t TableMetadata::GetSerializedSize() const {
  return static_cast<uint32_t>( sizeof(uint32_t)*4 + \
  table_name_.length() + sizeof(int) +\
  schema_->GetSerializedSize() );
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  table_id_t table_id;
  // std::string table_name;
  page_id_t root_page_id;
  Schema *schema=nullptr;
  uint32_t prim_idx;

  ASSERT(MACH_READ_FROM(uint32_t, buf) == TABLE_METADATA_MAGIC_NUM, "Wrong for MAGIC_NUM.");//check magic_num
  buf += sizeof(uint32_t);//update the buf


  table_id = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);

  // table_name = buf;
  // buf += sizeof(std::string);
  uint32_t name_len = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);
  std::string table_name(buf, name_len);
  buf += name_len;
  root_page_id = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);

  buf += Schema::DeserializeFrom(buf, schema, heap);
  prim_idx = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);

  table_meta = ALLOC_P(heap,TableMetadata)(table_id, table_name, root_page_id, schema, prim_idx);

  return static_cast<uint32_t>( sizeof(uint32_t)*4 + \
  sizeof(std::string) +  schema->GetSerializedSize() );
}


/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap, uint32_t prim_idx) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema, prim_idx);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema, uint32_t prim_idx)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema), prim_idx_(prim_idx) {}
