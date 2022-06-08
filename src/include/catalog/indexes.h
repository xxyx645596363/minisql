#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"
using BP_TREE_INDEX = BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>>;

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

  uint32_t GetColIndex(uint32_t i) { return key_map_[i]; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
                           index_id_ = index_id;
                           index_name_ = index_name;
                           table_id_ = table_id;
                           key_map_ = key_map;
                         }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};
 
/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager);

  void IndexSerialize(char * &buf){
    meta_data_->SerializeTo(buf);
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

  uint32_t GetColIndex(uint32_t i) { return meta_data_->GetColIndex(i); }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    // //ASSERT(false, "Not Implemented yet.");
    // void * buf = heap_->Allocate(sizeof(BPlusTreeIndex<GenericKey<4>, RowId, GenericComparator<4>>));
    // index = new(buf)BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>>(meta_data_->index_id_,key_schema_, buffer_pool_manager);
    BP_TREE_INDEX * index = ALLOC_P(heap_, BP_TREE_INDEX)(meta_data_->index_id_,key_schema_, buffer_pool_manager);
    TableHeap * table_heap;
    table_heap = table_info_->GetTableHeap();
    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter) {
        // std::cout << "Index *CreateIndex for flag1\n";
        
        const Row &row = *iter;
        std::vector<Field> new_fields;
        uint32_t idx;
        for(auto iter2 = meta_data_->key_map_.begin();iter2!=meta_data_->key_map_.end();iter2++){
          idx = *iter2;
          new_fields.emplace_back(*(row.GetField(idx)));
        }
        Row new_row(new_fields);
        // std::cout << "Index *CreateIndex for flag2\n";
        index->InsertEntry(new_row, row.GetRowId(), nullptr);
        // std::cout << "Index *CreateIndex for flag3\n";
    }
    return index;
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};



#endif //MINISQL_INDEXES_H
