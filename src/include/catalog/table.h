#ifndef MINISQL_TABLE_H
#define MINISQL_TABLE_H

#include <memory>
#include <unordered_map>
#include "glog/logging.h"
#include "record/schema.h"
#include "storage/table_heap.h"

class TableMetadata {
  friend class TableInfo;

public:
  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap);

  static TableMetadata *Create(table_id_t table_id, std::string table_name,
                               page_id_t root_page_id, TableSchema *schema, MemHeap *heap, uint32_t prim_idx);

  inline table_id_t GetTableId() const { return table_id_; }

  inline std::string GetTableName() const { return table_name_; }

  inline uint32_t GetFirstPageId() const { return root_page_id_; }

  inline Schema *GetSchema() const { return schema_; }

  inline uint32_t GetPrimIdx() const { return prim_idx_; }

private:
  TableMetadata() = delete;

  TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema, uint32_t prim_idx);

private:
  static constexpr uint32_t TABLE_METADATA_MAGIC_NUM = 344528;
  table_id_t table_id_;
  std::string table_name_;
  page_id_t root_page_id_;
  Schema *schema_;
  uint32_t prim_idx_;
};

/**
 * The TableInfo class maintains metadata about a table.
 */
class TableInfo {
public:
  static TableInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableInfo));
    return new(buf)TableInfo();
  }

  ~TableInfo() {
    delete heap_;
  }

  void Init(TableMetadata *table_meta, TableHeap *table_heap) {
    table_meta_ = table_meta;
    table_heap_ = table_heap;
  }

  void TableSerialize(char * &buf){
    table_meta_->SerializeTo(buf);
  }

  inline TableHeap *GetTableHeap() const { return table_heap_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline table_id_t GetTableId() const { return table_meta_->table_id_; }

  inline std::string GetTableName() const { return table_meta_->table_name_; }

  inline Schema *GetSchema() const { return table_meta_->schema_; }

  inline page_id_t GetRootPageId() const { return table_meta_->root_page_id_; }

  inline uint32_t GetPrimIdx() const { return table_meta_->GetPrimIdx(); }

  std::unordered_map<uint32_t, uint32_t> primmap;
  uint32_t prim_idx = 0;
  std::unordered_map<std::string, uint32_t> uniquemap;
  uint32_t unique_idx = 0;

private:
  explicit TableInfo() : heap_(new SimpleMemHeap()) {};

private:
  TableMetadata *table_meta_;
  TableHeap *table_heap_;
  MemHeap *heap_; /** store all objects allocated in table_meta and table heap */
};

#endif //MINISQL_TABLE_H
