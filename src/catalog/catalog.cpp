#include "catalog/catalog.h"
//I'm coding on VSC hhh
void CatalogMeta::SerializeTo(char *buf) const {
  map<table_id_t, page_id_t>::const_iterator iter;  
  int table_meta_pages_size, index_meta_pages_size;
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);//write magic_num
  buf += sizeof(uint32_t);//update the buf

  table_meta_pages_size = table_meta_pages_.size();
  MACH_WRITE_INT32(buf, table_meta_pages_size);//write table_id_
  buf += sizeof(int);
  for(iter = table_meta_pages_.begin(); iter != table_meta_pages_.end(); iter++){
    MACH_WRITE_UINT32(buf, iter->first);
    buf += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf, iter->second);
    buf += sizeof(uint32_t);
    //cout<<iter->first<<' '<<iter->second<<endl;  
  }

  index_meta_pages_size = index_meta_pages_.size();
  MACH_WRITE_INT32(buf, index_meta_pages_size);//write table_id_
  buf += sizeof(int);

  for(iter = index_meta_pages_.begin(); iter != index_meta_pages_.end(); iter++){
    MACH_WRITE_UINT32(buf, iter->first);
    buf += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf, iter->second);
    buf += sizeof(uint32_t);
  }    
  return;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  std::map<table_id_t, page_id_t> table_meta_pages;
  std::map<index_id_t, page_id_t> index_meta_pages;
  int table_meta_pages_size, index_meta_pages_size;
  table_id_t table_id;
  index_id_t index_id;
  page_id_t page_id;
  CatalogMeta * CatalogMeta_;
  CatalogMeta_ = ALLOC_P(heap, CatalogMeta)();
  
  ASSERT(MACH_READ_FROM(uint32_t, buf) == CATALOG_METADATA_MAGIC_NUM, "Wrong for MAGIC_NUM.");//check magic_num
  buf += sizeof(uint32_t);//update the buf

  table_meta_pages_size = MACH_READ_FROM(int32_t, buf);
  buf += sizeof(int);

  for(int i=0; i<table_meta_pages_size; i++){
    table_id = MACH_READ_FROM(uint32_t, buf);
    buf += sizeof(uint32_t);
    page_id = MACH_READ_FROM(uint32_t, buf);
    buf += sizeof(uint32_t);
    CatalogMeta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id, page_id));  
  }

  index_meta_pages_size = MACH_READ_FROM(int32_t, buf);
  buf += sizeof(int);

  for(int i=0; i<index_meta_pages_size; i++){
    index_id = MACH_READ_FROM(uint32_t, buf);
    buf += sizeof(uint32_t);
    page_id = MACH_READ_FROM(uint32_t, buf);
    buf += sizeof(uint32_t);
    CatalogMeta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id, page_id));  
  }
  
  return CatalogMeta_;
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  
  //new CatalogManager(bpm_, nullptr, nullptr, init)
  //初次创建时（init = true）初始化元数据；
  if(init == true){
    catalog_meta_=CatalogMeta::NewInstance(heap_);
  }
  // 并在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，
  //                                          -这里的“加载”是不是说的就是反序列化
  // 构建TableInfo和IndexInfo信息置于内存中。
  else{
    char * buf = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    catalog_meta_ = CatalogMeta::DeserializeFrom(buf, heap_);
    for(auto iter = catalog_meta_->table_meta_pages_.begin(); iter != catalog_meta_->table_meta_pages_.end(); iter++)
      LoadTable(iter->first, iter->second);

    for(auto iter = catalog_meta_->index_meta_pages_.begin(); iter != catalog_meta_->index_meta_pages_.end(); iter++)
      LoadIndex(iter->first, iter->second);
  }
}

CatalogManager::~CatalogManager() {
  char * buf ;//= buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  // char * buf_for_catalog_meta = buf;
  // buf += catalog_meta_->GetSerializedSize();
  // reserve space, put catalogmeta at first
  table_id_t table_id;
  page_id_t page_id;
  TableInfo * table_info;
  for(auto iter = tables_.begin(); iter!=tables_.end();iter++){
    table_id = iter->first;
    table_info = iter->second;
    buf = buffer_pool_manager_->NewPage(page_id)->GetData();
    table_info->TableSerialize(buf);
    buffer_pool_manager_->UnpinPage(page_id, true);
    catalog_meta_->table_meta_pages_[table_id]=page_id;
    // // catalog_meta_->table_meta_pages_.emplace(make_pair(table_id, page_id));
  
  }

  index_id_t index_id;
  IndexInfo * index_info;
  for(auto iter = indexes_.begin(); iter!=indexes_.end();iter++){
    index_id = iter->first;
    index_info = iter->second;
    buf = buffer_pool_manager_->NewPage(page_id)->GetData();
    index_info->IndexSerialize(buf);
    buffer_pool_manager_->UnpinPage(page_id,true);
    (catalog_meta_->index_meta_pages_).insert(make_pair(index_id, page_id));
  }

  buf = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info, uint32_t prim_idx) {
  if(GetTable(table_name, table_info) != DB_TABLE_NOT_EXIST){
    table_info = nullptr;
    return DB_TABLE_ALREADY_EXIST;
  }
  // next_table_id_ = catalog_meta_->GetNextTableId();
  table_id_t table_id = next_table_id_++; //tables_.size();
  table_names_[table_name] = table_id;

  TableHeap * table_heap = TableHeap::Create(buffer_pool_manager_, schema, nullptr, log_manager_, lock_manager_, heap_);
  TableMetadata * table_meta = TableMetadata::Create(table_id, table_name, \
  table_heap->GetFirstPageId(), schema, heap_, prim_idx);
  table_info = TableInfo::Create(heap_);
  table_info->Init(table_meta, table_heap);

  tables_[table_id] = table_info;

  std::vector<std::string> primary_key;
  primary_key.emplace_back(schema->GetColumn(prim_idx)->GetName());
  IndexInfo * primary_index = nullptr;
  CreateIndex(table_name, "prim_index", primary_key, nullptr, primary_index);
// //serialize to catalog meta
//     page_id_t table_meta_page_id;
//     char * buf = buffer_pool_manager_->NewPage(table_meta_page_id)->GetData();
//     table_info->TableSerialize(buf);
//     buffer_pool_manager_->UnpinPage(table_meta_page_id, true);
//     catalog_meta_->table_meta_pages_[table_id]=table_meta_page_id;

//     buf = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
//     catalog_meta_->SerializeTo(buf);
//     if(FlushCatalogMetaPage()==DB_FAILED) return DB_FAILED;
//     buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  table_info = nullptr;
  auto iter1 = table_names_.find(table_name);
  if (iter1 == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = iter1->second;
  auto iter2 = tables_.find(table_id);
  if (iter2 == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }

  table_info = iter2->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto iter = tables_.begin(); iter != tables_.end(); iter++){
    tables.emplace_back(iter->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // next_index_id_ = catalog_meta_->GetNextIndexId();
  TableInfo * table_info;
  if(GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST){
    return DB_TABLE_NOT_EXIST;
  }
  // auto iterr = index_names_.find(table_name)->second;
  // cout<<"We now have how many indexes? "<<iterr.size()<<endl;
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end()){
    cout << "当前索引已存在！！！！！\n";
    return DB_INDEX_ALREADY_EXIST;
  }
  index_id_t index_id = next_index_id_++;
  
  Schema * table_schema = table_info->GetSchema();
  std::vector<uint32_t> key_map;
  uint32_t column_id;
  for(auto i = index_keys.begin(); i != index_keys.end(); i++){
    std::string name = *i;
    // cout<<"key_column name="<<name<<endl;
    if(table_schema->GetColumnIndex(name, column_id) == DB_COLUMN_NAME_NOT_EXIST){
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.emplace_back(column_id);
  }
  //把index_key里面说的column换算成对应的列号放到key_map里面；
  // printf("key_map.size = %ld\n",key_map.size());
  // for(auto iter = key_map.begin();iter != key_map.end(); iter++)
  // printf("key map[] = %d\n",*(iter));
  index_names_[table_name][index_name] = index_id;
  //将索引名字和id的对应关系插入
  

  IndexMetadata * meta_data = IndexMetadata::Create(index_id, index_name, \
  table_info->GetTableId(), key_map, heap_);
  index_info = IndexInfo::Create(heap_);
  // std::cout << "CatalogManager::CreateIndex flag1\n";
  index_info -> Init(meta_data, table_info, buffer_pool_manager_);
  // printf("schema key num = %d\n", index_info->GetIndexKeySchema()->GetColumnCount());
  // printf("schema key size = %d\n", index_info->GetIndexKeySchema()->GetSerializedSize());
  indexes_[index_id] = index_info;

// //serialize to catalog meta
//     page_id_t index_meta_page_id;
//     char * buf = buffer_pool_manager_->NewPage(index_meta_page_id)->GetData();
//     index_info->IndexSerialize(buf);
//     buffer_pool_manager_->UnpinPage(index_meta_page_id,true);
//     // (catalog_meta_->index_meta_pages_).insert(make_pair(index_id, index_meta_page_id));
//     catalog_meta_->index_meta_pages_[index_id]=index_meta_page_id;

//     buf = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
//     catalog_meta_->SerializeTo(buf);
//     if(FlushCatalogMetaPage()==DB_FAILED) return DB_FAILED;
//     buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // index_id_t index_id = (((index_names_.find(table_name))->second).find(index_name))->second;
  // index_info = (indexes_.find(index_id))->second;
  index_info = nullptr;
  auto iter1 = index_names_.find(table_name);
  if(iter1 == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto iter2 = iter1->second.find(index_name);
  if(iter2 == iter1->second.end()){
    return DB_INDEX_NOT_FOUND;
  }
  auto iter3 = indexes_.find(iter2->second);
  if(iter3 == indexes_.end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_info = iter3->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto iter1 = index_names_.find(table_name);
  if(iter1 == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  IndexInfo * index_info;
  for(auto iter = iter1->second.begin(); iter != iter1->second.end(); iter++){
    if(GetIndex(table_name, iter->first, index_info) != DB_SUCCESS){
      return DB_FAILED;
    }
    indexes.emplace_back(index_info);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto iter1 = table_names_.find(table_name);
  if(iter1 == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = iter1->second;
  table_names_.erase(iter1);
  auto iter2 = tables_.find(table_id);
  if(iter2 == tables_.end())
    return DB_TABLE_NOT_EXIST;
  // TableInfo * table_info = iter2->second;
  tables_.erase(iter2);
  // delete table_info;

  
  string index_name;
  auto iter3 = index_names_.find(table_name)->second;
  for(auto iter4 = iter3.begin(); iter4 != iter3.end(); iter4++){
    index_name = iter4->first;
    DropIndex(table_name, index_name);
  }
  index_names_.erase(table_name);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto iter1 = index_names_.find(table_name);
  if(iter1 == index_names_.end())
    return DB_INDEX_NOT_FOUND;
  auto iter2 = iter1->second.find(index_name);
  if(iter2 == iter1->second.end())
    return DB_INDEX_NOT_FOUND;
  index_id_t index_id = iter2->second;
  iter1->second.erase(iter2);
  // index_names_.erase(iter1);

  auto iter3 = indexes_.find(index_id);
  if(iter3 == indexes_.end())
    return DB_INDEX_NOT_FOUND;
  indexes_.erase(iter3);
  // IndexInfo * index_info = iter3->second;
  // delete index_info;
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID))
    return DB_SUCCESS;
  else
    return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  TableInfo * table_info = TableInfo::Create(heap_);
  TableMetadata * table_meta;
  char * buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  buf += TableMetadata::DeserializeFrom(buf, table_meta, table_info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);
  string table_name = table_meta->GetTableName();
  table_names_[table_name] = table_id;

  TableHeap * table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), nullptr, nullptr, table_info->GetMemHeap());
  table_info->Init(table_meta, table_heap);//这里的table_heap怎么办？
  tables_[table_id] = table_info;

  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  IndexInfo * index_info = IndexInfo::Create(heap_);
  IndexMetadata * index_meta;
  char * buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  buf += IndexMetadata::DeserializeFrom(buf, index_meta, index_info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);
 
  string table_name;
  for(auto iter = table_names_.begin(); iter != table_names_.end(); iter++){
    if(iter->second == index_meta->GetTableId()){
      table_name = iter->first;
    }
  }

  string index_name = index_meta->GetIndexName();
  index_names_[table_name][index_name] = index_id;
  
  index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
  indexes_[index_id] = index_info;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }

  table_info = iter->second;
  return DB_SUCCESS;
}

