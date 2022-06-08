#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);//write magic_num
  buf += sizeof(uint32_t);//update the buf
  
  MACH_WRITE_UINT32(buf, index_id_);//write magic_num
  buf += sizeof(uint32_t);//update the buf

  MACH_WRITE_UINT32(buf, index_name_.length());//write magic_num
  buf += sizeof(uint32_t);//update the buf
  MACH_WRITE_STRING(buf, index_name_);//write the name of the table
  buf += index_name_.length();

  MACH_WRITE_UINT32(buf, table_id_);//write magic_num
  buf += sizeof(uint32_t);//update the buf

  MACH_WRITE_TO(int, buf, key_map_.size());//the number of column
  buf += sizeof(int);

  for (long unsigned int i = 0; i < key_map_.size(); i++)
  {
    MACH_WRITE_UINT32(buf, key_map_[i]);//write magic_num
    buf += sizeof(uint32_t);//update the buf
  }
  return static_cast<uint32_t>( sizeof(uint32_t)*4 + \
  index_name_.length() +  sizeof(int) +\
  key_map_.size() * sizeof(uint32_t) );
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return static_cast<uint32_t>( sizeof(uint32_t)*4 + \
  index_name_.length() + sizeof(int) +\
  key_map_.size() * sizeof(uint32_t) );
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  index_id_t index_id;
  // std::string index_name;
  table_id_t table_id;
  int n_key_map;
  std::vector<uint32_t> key_map;

  ASSERT(MACH_READ_FROM(uint32_t, buf) == INDEX_METADATA_MAGIC_NUM, "Wrong for MAGIC_NUM.");//check magic_num
  buf += sizeof(uint32_t);//update the buf

  index_id = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);

  // index_name = MACH_READ_FROM(std::string, buf);//read the name of the field
  // buf += sizeof(std::string);
  uint32_t name_len = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);
  std::string index_name(buf, name_len);
  buf += name_len;

  table_id = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);


  n_key_map = MACH_READ_FROM(int, buf);
  buf += sizeof(int);

  for (int i = 0; i < n_key_map; i++)
  {
    uint32_t new_key_map;
    new_key_map = MACH_READ_FROM(uint32_t, buf);
    buf += sizeof(uint32_t);
    key_map.push_back(new_key_map);
  }

  index_meta = ALLOC_P(heap,IndexMetadata)(index_id,index_name,table_id,key_map);
  
  return static_cast<uint32_t>( sizeof(uint32_t)*4 + \
  name_len +  sizeof(int) +\
  n_key_map * sizeof(uint32_t) );
}

void IndexInfo::Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    // 传入事先创建好的IndexMetadata和从CatalogManager中获取到的TableInfo，
    meta_data_ = meta_data;
    table_info_ = table_info;
    // Step2: mapping index key to key schema
    // 创建索引本身的key_schema_和Index对象。
    // 这里的key_schema_可以通过Schema::ShallowCopySchema来创建，
    // 且key_schema_中包含的列与TableSchema中的列共享同一份存储。
    TableSchema * tableschema = table_info_->GetSchema();

    // uint32_t columncount = tableschema->GetColumnCount();
    vector<uint32_t> attrs;
    // for(uint32_t i=0; i<columncount; i++)
    // attrs.emplace_back((uint32_t)i);
    for(auto iter = meta_data->key_map_.begin();iter != meta_data->key_map_.end(); iter++)
      attrs.emplace_back(*(iter));
    //static Schema *ShallowCopySchema(const Schema *table_schema, const std::vector<uint32_t> &attrs, MemHeap *heap)
    //按照attrs里标记的顺序，把table_schema的列进行浅拷贝并返回
    
    key_schema_ = Schema::ShallowCopySchema(tableschema, attrs, heap_);

    // std::cout << "IndexInfo::Init flag1\n";

    //这里attr应该是（0，1，2，3...）但是到多少呢？
    // Step3: call CreateIndex to create the index
    index_ = CreateIndex(buffer_pool_manager);
    return;
}
