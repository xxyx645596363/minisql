#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  return 0;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return 0;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  return 0;
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
