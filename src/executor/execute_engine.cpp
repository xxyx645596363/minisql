#include "executor/execute_engine.h"
#include "glog/logging.h"

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  //获取新数据库的名称
  pSyntaxNode ast_son = ast->child_;
  if (ast_son->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string new_dbs_name = ast_son->val_;

  //判断当前数据库名称是否存在
  if (dbs_.find(new_dbs_name) != dbs_.end())//若存在
  {
    std::cout << "Database Already Exists!\n";
    return DB_FAILED;
  }

  //创建新数据库
  DBStorageEngine *new_dbs = new DBStorageEngine(new_dbs_name, true);

  //将新的数据库指针加入到ExecuteEngine的unordered_map——dbs_中
  dbs_.emplace(new_dbs_name, new_dbs);
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  //获取要删除的数据库的名称
  pSyntaxNode ast_son = ast->child_;
  if (ast_son->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string deleted_dbs_name = ast_son->val_;

  //判断是否存在要删除的数据库
  if (dbs_.find(deleted_dbs_name) == dbs_.end())//若不存在，直接返回
  {
    std::cout << "Database Not Exist!\n";
    return DB_NOT_EXIST;
  }

  //根据要删除的数据库名称获取并释放数据库指针，并从unordered_map中删除对应数据库
  DBStorageEngine *deleted_dbs = dbs_.at(deleted_dbs_name);
  delete deleted_dbs;
  dbs_.erase(deleted_dbs_name);
  return DB_SUCCESS;
}

void RecoverMultiCheck(DBStorageEngine *dbs)
{
  TableInfo * ti;
  dbs->catalog_mgr_->GetTable("account", ti);
  TableHeap *th = ti->GetTableHeap();
  for (auto iter = th->Begin(nullptr); iter != th->End(); ++iter)
  {
    ti->primmap.emplace((*iter).GetField(0)->GetIntVal(), ti->primmap.size());
    ti->uniquemap.emplace(string((*iter).GetField(1)->GetCharVal(), (*iter).GetField(1)->GetLength()), ti->uniquemap.size());
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.size())//不是刚开始程序
  {
    //遍历输出所有数据库的名称
    for (auto iter = dbs_.begin(); iter != dbs_.end(); iter++)
    {
      std::cout << iter->first << std:: endl;
    }
    return DB_SUCCESS;
  }
  ifstream db_name_f("db_name.txt", ios::in);
  if (!db_name_f.is_open()) {
    cout << "Open file da_name wrong!" << endl;
    return DB_FAILED;
  }
  
  while (1)
  {
    string db_name;
    getline(db_name_f, db_name);
    if (db_name_f.eof()) break;
    cout << db_name << endl;
    [[maybe_unused]] DBStorageEngine *ori_dbs = new DBStorageEngine(db_name, false);
    dbs_.emplace(db_name, ori_dbs);

    if (db_name == "db0") RecoverMultiCheck(ori_dbs);
  }

  db_name_f.close();
  return DB_SUCCESS;
  
}


dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  //获取要进入数据库的名称
  pSyntaxNode ast_son = ast->child_;
  if (ast_son->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string used_dbs_name = ast_son->val_;

  //判断当前数据库是否存在
  if (dbs_.find(used_dbs_name) == dbs_.end())//若不存在，直接返回
  {
    std::cout << "dbs not exist!" << std::endl;
    return DB_NOT_EXIST;
  }

  //更新current_db_
  current_db_ = used_dbs_name;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取当前数据库中的表
  std::vector<TableInfo *> now_tables;
  now_dbs->catalog_mgr_->GetTables(now_tables);

  //遍历输出所有表的名称
  for (auto iter = now_tables.begin(); iter != now_tables.end(); iter++)
  {
    std::cout << (*iter)->GetTableName() << std::endl;
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取创建表的名称
  if (ast->child_->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string new_table_name = ast->child_->val_;

  //获取列信息
  //获取主键
  if (ast->child_->next_->type_ != kNodeColumnDefinitionList) return DB_FAILED;//检查语义
  // set<char * prim_set;
  // std::vector<string> prim_vec;
  pSyntaxNode prim_node = ast->child_->next_->child_;
  while ((prim_node->type_ != kNodeColumnList || prim_node->val_[0] != 'p') && (prim_node != nullptr)) prim_node = prim_node->next_;//定位存储主键的子树
  if (prim_node == nullptr || prim_node->type_ != kNodeColumnList) return DB_FAILED;
  std::string prim_name = prim_node->child_->val_;
  // cout << "ExecuteCreateTable prim_name: " << prim_name << endl;
  // for (pSyntaxNode node = prim_node->child_; node != nullptr; node = node->next_)
  // {
  //   prim_set.insert(node->val_);
  //   prim_vec.push_back(std::string(node->val_));
  // }  
  //获取列的其他信息，存进vector
  std::vector<Column *> columns;
  uint32_t index = 0, prim_idx;
  pSyntaxNode col_node;
  for (col_node = ast->child_->next_->child_; col_node->type_ == kNodeColumnDefinition; col_node = col_node->next_, index++)
  {
    // std::cout << "ExecuteCreateTable_for index: " << index << std::endl;
    //获取列的name
    char *col_name = col_node->child_->val_;
    //获取列的type和length
    char *col_type = col_node->child_->next_->val_;
    // std::cout << "ExecuteCreateTable_for col_name col_type: " << col_name << " " << col_type << std::endl;
    TypeId col_typeid;
    string col_len_str;
    uint32_t col_len;
    // cout << "ExecuteCreateTable_for col_type[0] col_type[0] == 'i': " << col_type[0] << " " << (col_type[0] == 'i') << endl;
    switch (col_type[0])
    {
    case 'i'://type为int
      col_typeid = kTypeInt;
      // std::cout << "ExecuteCreateTable_for int_type\n";
      break;
    case 'f'://type为float
      col_typeid = kTypeFloat;
      break;
    case 'c'://type为char
      col_typeid = kTypeChar;
      //获取字符串长度，注意长度用字符串组成，如"20"
      col_len_str = col_node->child_->next_->child_->val_;
      //若为负数或小数，则返回失败
      if (col_len_str.find('-') != string::npos || col_len_str.find('.') != string::npos) return DB_FAILED;
      //string转char*转int
      col_len = atoi(col_len_str.c_str());
      break;
    default:
      break;
    }
    //获取列的nullable和unique,记录主键index
    bool nullable = true, unique = false;
    // std::cout << "ExecuteCreateTable_for init nullable unique: " << nullable << " " << unique << std::endl;
    if (prim_name == string(col_name))// 为主键
    {
      // cout << "prim_name == string(col_name)\n";
      nullable = false;
      unique = true;
      prim_idx = index;
    } 
    if (col_node->val_ != nullptr && !strcmp(col_node->val_, "unique"))//判断为unique
    {
      // cout << "ExecuteCreateTable_for col_node->val_ == unique\n";
      unique = true;
    }
    // std::cout << "ExecuteCreateTable_for nullable unique: " << nullable << " " << unique << std::endl;

    //创建column并插入到vector中
    Column *new_column;
    if (col_typeid != kTypeChar)
    {
      new_column = new Column(col_name, col_typeid, index, nullable, unique);
    }
    else
    {
      new_column = new Column(col_name, col_typeid, col_len, index, nullable, unique);
    }
    columns.push_back(new_column);
  }
  if (col_node->type_ != kNodeColumnList) return DB_FAILED;//检察语义

  //用column的vector数组创建schema
  Schema *new_Schema = new Schema(columns);

  //创建空的TableInfo用于存储创建table函数的返回值
  TableInfo *new_tableinfo;

  //调用CatalogManager的CreateTable函数为数据库创建新的表
  dberr_t createtable_ret = now_dbs->catalog_mgr_->CreateTable(new_table_name, new_Schema, nullptr, new_tableinfo, prim_idx);
  
  //  在创建表的时候就创建了
  // 为表的主键自动创建索引：
  // IndexInfo *index_info;//创建一个IndexInfo用于引用返回
  // dberr_t createindex_ret = now_dbs->catalog_mgr_->CreateIndex(new_table_name, "primary_key", prim_vec, nullptr, index_info);
  
  //返回创建结果
  return createtable_ret;
}


dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取要删除的表的名称
  if (ast->child_->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string droptable_name = ast->child_->val_;

  //调用CatalogManager的DropTable函数
  dberr_t droptable_ret = now_dbs->catalog_mgr_->DropTable(droptable_name);
  //返回删除结果
  return droptable_ret;
}


void printIndexInfo(IndexInfo * index, const std::string table_name)
{
  std::cout << "Index_name: " << index->GetIndexName() << std::endl;
  std::cout << "Table_name: " << table_name << std::endl;
  std::cout << "Attributes: ";
  IndexSchema * indexschema = index->GetIndexKeySchema();
  for (uint32_t i = 0; i < indexschema->GetColumnCount(); i++)
  {
    std::cout << indexschema->GetColumn(i)->GetName() << " ";
  }std::cout << std::endl;
  std::cout << "..........................................\n";
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //调用函数遍历每个table，输出索引信息
  std::vector<TableInfo *> tables;
  now_dbs->catalog_mgr_->GetTables(tables);//获取所有table
  for (long unsigned int i = 0; i < tables.size(); i++)
  {
    std::string table_name = tables[i]->GetTableName();//获取table的名字
    std::vector<IndexInfo *> indexes;//vector for indexes in this table;
    now_dbs->catalog_mgr_->GetTableIndexes(table_name, indexes);
    cout << "一共有 " << indexes.size() << " 个索引如下：\n";
    for (long unsigned int j = 0; j < indexes.size(); j++)
    {
      printIndexInfo(indexes[j], table_name);
    }
  }  
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取创建索引的名称：
  pSyntaxNode ast_son1 = ast->child_, ast_son2 = ast_son1->next_, ast_son3 = ast_son2->next_;
  if (ast_son1->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string new_index_name = ast_son1->val_;

  //获取创建索引的表的名称：
  if (ast_son2->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string table_name = ast_son2->val_;
  TableInfo * table;
  if (now_dbs->catalog_mgr_->GetTable(table_name, table) != DB_SUCCESS)
  {
    cout << "获取表失败\n";
    return DB_FAILED;
  }
  Schema *schema = table->GetSchema();

  //获取索引的attribute,以vector的结果
  std::vector<std::string> index_keys;
  if (ast_son3->type_ != kNodeColumnList || strcmp(ast_son3->val_, "index keys")) return DB_FAILED;//检查语义
  for (pSyntaxNode key_node = ast_son3->child_; key_node != nullptr; key_node = key_node->next_)
  {
    if (key_node->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
    index_keys.push_back(key_node->val_);
    uint32_t col_idx;
    if (schema->GetColumnIndex(key_node->val_, col_idx) != DB_SUCCESS)
    {
      cout << "没有找到当前索引对应的列\n";
      return DB_FAILED;
    }
    if (!schema->GetColumn(col_idx)->IsUnique())
    {
      cout << "创建的索引必须unique\n";
      return DB_FAILED;
    }
  }
  // cout << "ExecuteCreateIndex flag1\n"; 

  //调用catalog的CreateIndex函数创建索引：
  IndexInfo *index_info;//创建一个IndexInfo用于引用返回
  dberr_t createindex_ret = now_dbs->catalog_mgr_->CreateIndex(table_name, new_index_name, index_keys, nullptr, index_info);
  return createindex_ret;
}


dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //获取删除的索引名称
  pSyntaxNode ast_son = ast->child_;
  if (ast_son->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string drop_index_name = ast_son->val_;
  cout << "删除索引名称：" << ast_son->val_ << endl;

  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //调用函数遍历每个table，删除索引
  std::vector<TableInfo *> tables;
  dberr_t dropindex_ret = DB_INDEX_NOT_FOUND;
  now_dbs->catalog_mgr_->GetTables(tables);//获取所有table
  for (long unsigned int i = 0; i < tables.size(); i++)
  {
    std::string table_name = tables[i]->GetTableName();//获取table的名字
    if (now_dbs->catalog_mgr_->DropIndex(table_name, drop_index_name) == DB_SUCCESS) dropindex_ret = DB_SUCCESS;//一旦有一个table删除成功，则返回成功
  }
  
  return dropindex_ret;
}


void printRow(const Row row, const std::vector<std::string> col_names, const bool allCol, const Schema *schema)
{
  select_record++;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++)
  {
    string col_name = schema->GetColumn(i)->GetName();
    if ( allCol || find(col_names.begin(), col_names.end(), col_name) != col_names.end() )//当前列属于要输出的列
    {
      if (row.GetField(i)->IsNull())
        cout << setw(20) << setiosflags(ios::left) << "null";  
      else
      {
        switch (row.GetField(i)->GetType())
        {
        case kTypeInt:
          cout << setw(20) << setiosflags(ios::left) << row.GetField(i)->GetIntVal();
          break;
        case kTypeFloat:
          cout << setw(20) << setiosflags(ios::left) << row.GetField(i)->GetFloatVal();
          break;
        case kTypeChar:
          cout << setw(20) << setiosflags(ios::left) << string(row.GetField(i)->GetCharVal(), row.GetField(i)->GetLength());
          break;
        default:
          break;
        }
      } 
    }
  }cout << endl;
}

void printRowWithRid(const RowId &rid, TableHeap *table_heap, const std::vector<std::string> &col_names, const bool allCol, const Schema *schema)
{
  //获取row并输出:
  Row row(rid);
  bool gettuple_ret = table_heap->GetTuple(&row, nullptr);
  if (!gettuple_ret)  std::cout << std::endl;
  else
  {
    printRow(row, col_names, allCol, schema);
  }
}

void printRowWithpair(const Mapping_Type &keypair, TableHeap *table_heap, const std::vector<std::string> &col_names, const bool allCol, const Schema *schema)
{
  //获取rowid:
  RowId rid = keypair.second;
  
  printRowWithRid(rid, table_heap, col_names, allCol, schema);
  // //获取row并输出:
  // Row row(rid);
  // bool gettuple_ret = table_heap->GetTuple(&row, nullptr);
  // if (!gettuple_ret)  std::cout << std::endl;
  // else
  // {
  //   printRow(row, col_names, allCol, schema);
  // }
}

SelectCondition *getConditionByCompare(pSyntaxNode compare_node, Schema *schema)
{
  // cout << "ExecuteSelect_getSelectCondition_getConditionByCompare start\n";
  if (compare_node->type_ != kNodeCompareOperator) return nullptr;
  // cout << "ExecuteSelect_getSelectCondition_getConditionByCompare flag1\n";
  
  SelectCondition *new_condition = new SelectCondition;
  //获取判断类型：
  string compare_type = compare_node->val_;
  // cout << "ExecuteSelect_getSelectCondition_getConditionByCompare compare_type: " << compare_type << endl;
  if (compare_type == "=") new_condition->type_ = 0;
  else if (compare_type == "<>") new_condition->type_ = 1;
  else if (compare_type == "<") new_condition->type_ = 2;
  else if (compare_type == ">") new_condition->type_ = 3;
  else if (compare_type == "<=") new_condition->type_ = 4;
  else if (compare_type == ">=") new_condition->type_ = 5;
  else return nullptr;
  // cout << "ExecuteSelect_getSelectCondition_getConditionByCompare new_condition->type_: " << new_condition->type_ << endl;
  //获取判断属性名,根据名称获取在schema中的idx,从而获取对应column的type：
  new_condition->attri_name = compare_node->child_->val_;
  uint32_t col_idx;
  if (schema->GetColumnIndex(new_condition->attri_name, col_idx) != DB_SUCCESS)
  {
    cout << "No Such Column!!!\n";
    return nullptr;
  }
  new_condition->type_id_ = schema->GetColumn(col_idx)->GetType();
  //获取判断值：
  pSyntaxNode value_node = compare_node->child_->next_;
  if (value_node->type_ == kNodeNumber)
  {
    if (new_condition->type_id_ == kTypeInt)
    {
      // new_condition->type_id_ = kTypeInt;
      new_condition->value_.int_ = atoi(value_node->val_);
    }
    else if (new_condition->type_id_ == kTypeFloat)
    {
      new_condition->value_.float_ = atof(value_node->val_);
      // new_condition->type_id_ = kTypeFloat;
    }
    
  }
  else if (value_node->type_ == kNodeString)
  {
    new_condition->value_.chars_ = value_node->val_;
    // new_condition->type_id_ = kTypeChar;
  }
  else return nullptr;
  // cout << "ExecuteSelect_getSelectCondition_getConditionByCompare new_condition-attri_name value_: " << new_condition->attri_name << " " << new_condition->value_.int_ << endl;
  return new_condition;
}

dberr_t getSelectCondition(vector<SelectCondition *> &select_conditions, pSyntaxNode condition_node, Schema *schema)
{
  // cout << "ExecuteSelect_getSelectCondition start\n";
  if (condition_node->child_->type_ == kNodeCompareOperator)//where后只有一个条件
  {
    select_conditions.push_back(getConditionByCompare(condition_node->child_, schema));
    // cout << "ExecuteSelect_getSelectCondition DB_SUCCESS\n";
    return DB_SUCCESS;
  }
  else if (condition_node->child_->type_ == kNodeConnector)//where后为and
  {
    // cout << "发现where后面是and\n";
    select_conditions.push_back(getConditionByCompare(condition_node->child_->child_, schema));
    select_conditions.push_back(getConditionByCompare(condition_node->child_->child_->next_, schema));
    return DB_SUCCESS;
  }
  else return DB_FAILED;
}

bool checkCondition(vector<SelectCondition *> &select_conditions, const Row row, Schema *schema)
{
  for (uint32_t i = 0; i < select_conditions.size(); i++)
  {
    string col_name = select_conditions[i]->attri_name;
    uint32_t ind;
    dberr_t getcolindex_ret = schema->GetColumnIndex(col_name, ind);
    if (getcolindex_ret == DB_COLUMN_NAME_NOT_EXIST) continue;//在schema未找到这个条件中的column，直接跳过这个条件
    Field *field = row.GetField(ind);
    Field *comfield;
    //将进行比较的值变为field形式用于调用函数进行比较：
    // cout << "checkCondition select_conditions[i]->type_id_==float: " << (select_conditions[i]->type_id_ == kTypeFloat) << endl;
    // cout << "checkCondition select_conditions[i]->attri_name " << (select_conditions[i]->attri_name) << endl;
    if (select_conditions[i]->type_id_ == kTypeInt)
    {
      comfield = new Field(kTypeInt, select_conditions[i]->value_.int_);
    }
    else if (select_conditions[i]->type_id_ == kTypeFloat)
    {
      comfield = new Field(kTypeFloat, select_conditions[i]->value_.float_);
    }
    else
    {
      comfield = new Field(kTypeChar, select_conditions[i]->value_.chars_, strlen(select_conditions[i]->value_.chars_), true);
    }
    //根据条件类型调用不同函数进行比较：
    // cout << "checkCondition select_conditions[i]->type_: " << select_conditions[i]->type_ << endl;
    // cout << "checkCondition : " << select_conditions[0]->value_.float_ << endl;
    // cout << "checkCondition comare two datas: " << field->GetData() << " " << comfield->GetData() << endl;
    switch (select_conditions[i]->type_)
    {
    case 0://=
      if (field->CompareEquals(*comfield) != CmpBool::kTrue) return false;
      break;
    case 1://!=
      if (field->CompareEquals(*comfield) == CmpBool::kTrue) return false;
      break;
    case 2://<
      if (field->CompareLessThan(*comfield) != CmpBool::kTrue) return false;
      break;
    case 3://>
      if (field->CompareGreaterThan(*comfield) != CmpBool::kTrue) return false;
      break;
    case 4://<=
      if (field->CompareLessThanEquals(*comfield) != CmpBool::kTrue) return false;
      break;
    case 5://>=
      if (field->CompareGreaterThanEquals(*comfield) != CmpBool::kTrue) return false;
      break;
    default:
      return false;
      break;
    }
    
    delete comfield;
  }
  return true;
}

bool checkIndexSameWithCondition(IndexInfo *index, const SelectCondition *condition)
{
  if (index->GetIndexKeySchema()->GetColumnCount() != 1) return false;//首先须为单属性索引
  return condition->attri_name == index->GetIndexKeySchema()->GetColumn(0)->GetName();

}

dberr_t selectWithIndex(SelectCondition *condition, IndexInfo *indexinfo, const std::vector<std::string> col_names, const bool allCol)
{
  //利用condition的attribute构造一个"row",将row序列化为keytype用于索引:
  //首先构造用于构造row的field_vector,因为只考虑单属性索引，索引这里的vector里面只有一个：
  vector<Field> fields;
  switch (condition->type_id_)
  {
  case kTypeInt:
    fields.push_back(Field(kTypeInt, condition->value_.int_));
    cout << "selectWithIndex " << condition->value_.int_ << endl;
    break;
  case kTypeFloat:
    fields.push_back(Field(kTypeFloat, condition->value_.float_));
    break;
  case kTypeChar:
    fields.push_back(Field(kTypeChar, condition->value_.chars_, strlen(condition->value_.chars_), true));
    break;
  default:
    return DB_FAILED;
    break;
  }

  //获取schema
  Schema *schema = indexinfo->GetIndexKeySchema();

  //构建row和GenericKey,默认用64的大小,从而获取相应的迭代器:
  Row key_row(fields);
  
  GenericKey<64> genekey;
  genekey.SerializeFromKey(key_row, schema);
  
  auto key_iter = reinterpret_cast<BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>> *>(indexinfo->GetIndex())->GetBeginIterator(genekey);
  auto begin_iter = reinterpret_cast<BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>> *>(indexinfo->GetIndex())->GetBeginIterator();
  auto end_iter = reinterpret_cast<BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>> *>(indexinfo->GetIndex())->GetEndIterator();

  //获取table_heap:
  TableHeap *table_heap = indexinfo->GetTableInfo()->GetTableHeap();  

  //根据条件不同执行结果
  //void printRow(const Row row, const std::vector<std::string> col_names, const bool allCol, const Schema *schema)
  //void printRowWithpair(const Mapping_Type keypair, TableHeap *table_heap, const std::vector<std::string> col_names, const bool allCol, const Schema *schema)
  Mapping_Type keypair;
  Schema * printschema = indexinfo->GetTableInfo()->GetSchema();
  switch (condition->type_)
  {
  case 0://=
    //获取rowid:
    keypair = *key_iter;//Mapping_Type std::pair<KeyType, ValueType>
    if (keypair.first == genekey)
      printRowWithpair(keypair, table_heap, col_names, allCol, printschema);
    break;
  case 1://!=
    for (auto iter = begin_iter; ; ++iter)
    {
      //获取rowid:
      keypair = *iter;//Mapping_Type std::pair<KeyType, ValueType>
      if (iter == key_iter && keypair.first == genekey) continue;//跳过等于的row
      printRowWithpair(keypair, table_heap, col_names, allCol, printschema);
      if (iter == end_iter) break;
    }
    break;
  case 2://<
    for (auto iter = begin_iter; iter != key_iter; ++iter)
    {
      //获取rowid:
      keypair = *iter;//Mapping_Type std::pair<KeyType, ValueType>
      printRowWithpair(keypair, table_heap, col_names, allCol, printschema);
      if (iter == end_iter) break;
    }
    break;
  case 3://>
    for (auto iter = key_iter; ; ++iter)
    {
      //获取rowid:
      keypair = *iter;//Mapping_Type std::pair<KeyType, ValueType>
      if (keypair.first == genekey)
      {
        if (iter == end_iter) break;
        continue;
      }
      printRowWithpair(keypair, table_heap, col_names, allCol, printschema);
      if (iter == end_iter) break;
    }
    break;
  case 4://<=
    for (auto iter = begin_iter; ; ++iter)
    {
      //获取rowid:
      keypair = *iter;//Mapping_Type std::pair<KeyType, ValueType>
      if (iter == key_iter)
      {
        if (keypair.first == genekey)
        {
          ++key_iter;
        }
        else
        {
          break;
        }
      }
      printRowWithpair(keypair, table_heap, col_names, allCol, printschema);
      if (iter == end_iter) break;
    }
    break;
  case 5://>=
    for (auto iter = key_iter; ; ++iter)
    {
      //获取rowid:
      keypair = *iter;//Mapping_Type std::pair<KeyType, ValueType>
      printRowWithpair(keypair, table_heap, col_names, allCol, printschema);
      if (iter == end_iter) break;
    }
    break;
  default:
    return DB_FAILED;
    break;
  }
  return DB_SUCCESS;
}
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  select_record = 0;
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取表：
  pSyntaxNode table_node = ast->child_->next_;
  if (table_node->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string table_name = table_node->val_;
  TableInfo *table; 
  if (now_dbs->catalog_mgr_->GetTable(table_name, table) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  // cout << "ExecuteSelect flag1\n";
  //获取查询的列的名称
  bool allCol = false;
  std::vector<std::string> col_names;
  if (ast->child_->type_ == kNodeColumnList)
  {
    for (pSyntaxNode col_node = ast->child_->child_; col_node != nullptr; col_node = col_node->next_)
    {
      if (col_node->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
      col_names.push_back(std::string(col_node->val_));
      cout << setw(20) << setiosflags(ios::left) << col_node->val_;
    }cout << endl;
  }
  else if (ast->child_->type_ == kNodeAllColumns) allCol = true;
  else return DB_FAILED;//检查语义

  //判断条件:
  pSyntaxNode condition_node = table_node->next_;
  Schema *schema = table->GetSchema();//获取模式
  TableHeap *table_heap = table->GetTableHeap();//获取堆表
  if (condition_node == nullptr)//无条件，输出所有列
  {
    for (auto row_iter = table_heap->Begin(nullptr); row_iter != table_heap->End(); ++row_iter)
    {
      printRow(*row_iter, col_names, allCol, schema);
    }cout << "................................................................................\n";
  }
  else if (condition_node->type_ == kNodeConditions)//存在where
  {
    // cout << "ExecuteSelect condition_node->type_ == kNodeConditions\n";
    vector<SelectCondition *> select_conditions;
    // cout << "!!!!!!!!!!!!!!!!!!!!!" << endl;
    getSelectCondition(select_conditions, condition_node, schema);
    // cout << "......................" << endl;
    // cout << "ExecuteSelect size select_conditions[0]->type is float: " << select_conditions.size() << " " << (select_conditions[0]->type_id_ == kTypeFloat) << endl;
    if (select_conditions.size() == 2)//多条件查询，直接遍历
    {
      for (auto row_iter = table_heap->Begin(nullptr); row_iter != table_heap->End(); ++row_iter)
      {
        // cout << "multiselect\n";
        if (checkCondition(select_conditions, *row_iter, schema))
          printRow(*row_iter, col_names, allCol, schema);
      }cout << "................................................................................\n";
    }
    else if (select_conditions.size() == 1)//单条件查询，尝试利用index
    {
      //获取表的索引：
      std::vector<IndexInfo *> indexes;//vector for indexes in this table;
      dberr_t getindexes_ret = now_dbs->catalog_mgr_->GetTableIndexes(table_name, indexes);
      if (getindexes_ret != DB_SUCCESS)
      {
        cout << "ExecuteSelect getindexes_ret != DB_SUCCESS\n";
        return DB_FAILED;
      }
      //遍历索引，检查是否有索引和where中条件相吻合：
      for (uint32_t i = 0; i < indexes.size(); i++)
      {
        if (checkIndexSameWithCondition(indexes[i], select_conditions[0]))//索引和where中条件相吻合
        {
          cout << "可利用索引: " << indexes[i]->GetIndexName() << "进行优化查询\n";
          dberr_t select_ret = selectWithIndex(select_conditions[0], indexes[i], col_names, allCol);
          cout << "一共查到 " << select_record << "条记录!\n";
          return select_ret;
        }
      }
      //没有索引,直接遍历
      int i = 1;
      for (auto row_iter = table_heap->Begin(nullptr); row_iter != table_heap->End(); ++row_iter, i++)
      {
        if (checkCondition(select_conditions, *row_iter, schema))
        {
          // cout << "get a record\n";
          printRow(*row_iter, col_names, allCol, schema);
        }
        // cout << i << endl;
      }cout << "................................................................................\n";
    }
    for (uint32_t i = 0; i < select_conditions.size(); i++)
    {
      delete select_conditions[i];
    }
  }
  else return DB_FAILED;//检查语义
  cout << "一共查到 " << select_record << "条记录!\n";
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取表名和表：
  pSyntaxNode table_node = ast->child_;
  if (table_node->type_ != kNodeIdentifier) return DB_FAILED;
  std::string table_name = table_node->val_;

  //循环获取插入的每个column的值并创建field数组：
  std::vector<Field> fields;
  TableInfo *ins_table; 
  dberr_t getTable_ret = now_dbs->catalog_mgr_->GetTable(table_name, ins_table);//获取表
  if (getTable_ret == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  TableHeap *table_heap = ins_table->GetTableHeap();//获取堆表
  Schema *schema = ins_table->GetSchema();
  uint32_t idx = 0;
  for (pSyntaxNode val_node = table_node->next_->child_; val_node != nullptr; val_node = val_node->next_)
  {
    Field *newfield;
    switch (schema->GetColumn(idx)->GetType())
    {
    case kTypeInt:
      if (ins_table->primmap.find(atoi(val_node->val_)) != ins_table->primmap.end())
      {
        cout << "PRIMARY KEY约束冲突\n";
        return DB_FAILED;
      }
      newfield = new Field(kTypeInt, atoi(val_node->val_));
      ins_table->primmap.emplace(atoi(val_node->val_), ins_table->prim_idx++);
      fields.push_back(*newfield);
      break;
    case kTypeFloat:
      newfield = new  Field(kTypeFloat, float(atof(val_node->val_)));
      fields.push_back(*newfield);
      break;
    case kTypeChar:
      if (ins_table->uniquemap.find(string(val_node->val_)) != ins_table->uniquemap.end())
      {
        cout << "UNIQUE约束冲突\n";
        return DB_FAILED;
      }
      newfield = new Field(kTypeChar, val_node->val_, strlen(val_node->val_), true);
      ins_table->uniquemap.emplace(string(val_node->val_), ins_table->unique_idx++);
      fields.push_back(*newfield);
      break;
    default:
      return DB_FAILED;
      break;
    }
    delete newfield;
    idx++;
  }

  //根据field数组创建row,并插入到堆表
  Row *ins_row = new Row(fields);
  if (!(table_heap->InsertTuple(*ins_row, nullptr)))
  {
    delete ins_row;
    return DB_FAILED;
  } 

  //获取rowid,插入索引的B+树：
  vector<IndexInfo *> indexes;
  if (now_dbs->catalog_mgr_->GetTableIndexes(table_name, indexes) != DB_SUCCESS) return DB_FAILED;
  for (uint32_t i = 0; i < indexes.size(); i++)
  {
    auto index = indexes[i]->GetIndex();
    vector<Field> index_fields;
    index_fields.push_back(fields[indexes[i]->GetColIndex(0)]);
    Row key_row(index_fields);
    if (index->InsertEntry(key_row, ins_row->GetRowId(), nullptr) != DB_SUCCESS) return DB_FAILED;
    else
    {
      // cout << "记录插入索引： " << index->index_id_ << endl;
    }
  }
  delete ins_row;
  return DB_SUCCESS;
}


bool checkDeleteRow(const Row &row, const std::string name, const char *val, Schema *schema)
{
  //获取删除条件中属性名name对应的col_idx:
  uint32_t col_idx;
  dberr_t getcolidx_ret = schema->GetColumnIndex(name, col_idx);
  if (getcolidx_ret == DB_COLUMN_NAME_NOT_EXIST) return false;

  //根据idx获取row中的field:
  Field *condition_field = row.GetField(col_idx);

  switch (condition_field->GetType())
  {
  case kTypeInt:
    return atoi(val) == condition_field->GetIntVal();
    break;
  case kTypeFloat:
    return abs(atof(val) - condition_field->GetFloatVal()) <= 1e-6;
    break;
  case kTypeChar:
    if (!strncmp(val, condition_field->GetCharVal(), strlen(val))) return true;
    return false;
  default:
    return false;
    break;
  }
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取表的名称：
  pSyntaxNode table_node = ast->child_;
  if (table_node->type_ != kNodeIdentifier) return DB_FAILED;
  std::string table_name = table_node->val_;
  TableInfo *del_table; 
  dberr_t getTable_ret = now_dbs->catalog_mgr_->GetTable(table_name, del_table);//获取表
  if (getTable_ret == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  Schema *schema = del_table->GetSchema();

  //获取删除条件：
  bool allDelete = false;
  vector<SelectCondition *> del_conditions;
  pSyntaxNode condition_node = table_node->next_;
  if (condition_node == nullptr) allDelete = true;
  else if (condition_node->type_ == kNodeConditions)
  {
    getSelectCondition(del_conditions, condition_node, schema);
    
  }
  else return DB_FAILED;
  // bool allDelete = false;
  // pSyntaxNode condition_node = table_node->next_;
  // std::string condition_name;
  // char *del_val;
  // if (condition_node == nullptr) allDelete = true;
  // else if (!strcmp(condition_node->child_->val_, "="))//偷个懒，根据验收流程默认条件为=,且只有一个条件
  // {
  //   condition_name = condition_node->child_->child_->val_;//名称
  //   //获取值：
  //   del_val = condition_node->child_->child_->next_->val_;
  // }
  // else return DB_FAILED;

  //遍历删除：
  TableHeap *table_heap = del_table->GetTableHeap();//获取堆表
  vector<IndexInfo *> indexes;
  dberr_t getindexes_ret = now_dbs->catalog_mgr_->GetTableIndexes(table_name, indexes);
  if (getindexes_ret != DB_SUCCESS) return DB_FAILED;
  for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter)//遍历堆表
  {
    // if (allDelete || checkDeleteRow(*iter, condition_name, del_val, schema))//若全部删除或row满足删除条件，则删除
    if (allDelete || checkCondition(del_conditions, *iter, schema))//若全部删除或row满足删除条件，则删除
    {
      del_table->primmap.erase((*iter).GetField(0)->GetIntVal());
      del_table->uniquemap.erase(string((*iter).GetField(1)->GetCharVal(), (*iter).GetField(1)->GetLength()));
      //调用堆表删除
      table_heap->ApplyDelete((*iter).GetRowId(), nullptr);
      //遍历索引删除
      for (uint32_t i = 0; i < indexes.size(); i++)
      {
        vector<Field> index_fields;
        index_fields.push_back(*((*iter).GetField(indexes[i]->GetColIndex(0))));
        Row key_row(index_fields);
        indexes[i]->GetIndex()->RemoveEntry(key_row, nullptr);
      }
    }
  }
  return DB_SUCCESS;
}


size_t GetUpdateItem(vector<UpdateItem> &items, pSyntaxNode udnode)
{
  size_t itemnum = 0;
  for (pSyntaxNode node = udnode->child_; node != nullptr; node = node->next_)
  {
    itemnum++;

    if (node->type_ != kNodeUpdateValue)
    {
      cout << "更新值语法树错误\n";
      return -1;
    }

    UpdateItem newitem;
    newitem.name = node->child_->val_;
    char *udval = node->child_->next_->val_;
    switch (node->child_->next_->type_)
    {
    case kNodeNumber:
      if (strstr(udval, ".") != NULL)// 找到了小数点
      {
        newitem.type_ = kTypeFloat;
        newitem.value_.float_ = atof(udval);
      }
      else
      {
        newitem.type_ = kTypeInt;
        newitem.value_.int_ = atoi(udval);
      }
      break;
    case kNodeString:
      newitem.type_ = kTypeChar;
      newitem.value_.chars_ = udval;
      break;
    default:
      return -1;
    }
    items.push_back(newitem);
  }
  return itemnum;
}

Row *GetNewRow(const Row &oldrow, vector<UpdateItem> &updateitems, Schema * schema)
{
  vector<Field> newFields;
  for (uint32_t i = 0; i < oldrow.GetFieldCount(); i++)
  {
    string name = schema->GetColumn(i)->GetName();
    bool update_flag = false;
    for (uint32_t j = 0; j < updateitems.size(); j++)
    {
      if (updateitems[j].name == name)// 当前field为要更新的field
      {
        update_flag = true;
        switch (schema->GetColumn(i)->GetType())
        {
        case kTypeInt:
          newFields.push_back(Field(kTypeInt, updateitems[j].value_.int_));
          break;
        case kTypeFloat:
          newFields.push_back(Field(kTypeFloat, updateitems[j].value_.float_));
          break;
        case kTypeChar:
          newFields.push_back(Field(kTypeChar, updateitems[j].value_.chars_, strlen(updateitems[j].value_.chars_), true));
          break;
        default:
          cout << "updateitems出错！\n";
          return nullptr;
        }
        break;
      }
    }

    if (!update_flag)// 当前field不用更新，直接加入新的row
      newFields.push_back(*(oldrow.GetField(i)));
  }
  if (schema->GetColumnCount() != newFields.size())
  {
    cout << "updateitems数量不对\n";
    return nullptr;
  }
  Row *newrow = new Row(newFields);
  return newrow;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  if (!current_db_.length())//当前无数据库
  {
    std::cout << "No current dbs!" << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取表的名称：
  pSyntaxNode table_node = ast->child_;
  if (table_node->type_ != kNodeIdentifier) return DB_FAILED;
  std::string table_name = table_node->val_;
  TableInfo *ud_table; 
  dberr_t getTable_ret = now_dbs->catalog_mgr_->GetTable(table_name, ud_table);//获取表
  if (getTable_ret == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  Schema *schema = ud_table->GetSchema();

  vector<UpdateItem> updateitems;
  if (GetUpdateItem(updateitems, table_node->next_) != updateitems.size())
  {
    cout << "更新记录语法树出错！\n";
    return DB_FAILED;
  }

  //获取更新条件：
  bool allUpdate = false;
  vector<SelectCondition *> ud_conditions;
  pSyntaxNode condition_node = table_node->next_->next_;
  if (condition_node == nullptr) allUpdate = true;
  else if (condition_node->type_ == kNodeConditions)
  {
    getSelectCondition(ud_conditions, condition_node, schema);
    
  }
  else return DB_FAILED;
  // bool allUpdate = false;
  // pSyntaxNode condition_node = table_node->next_->next_;
  // std::string condition_name;
  // char *update_val;
  // if (condition_node == nullptr) allUpdate = true;
  // else if (!strcmp(condition_node->child_->val_, "="))//偷个懒，根据验收流程默认条件为=,且只有一个条件
  // {
  //   condition_name = condition_node->child_->child_->val_;//名称
  //   //获取值：
  //   update_val = condition_node->child_->child_->next_->val_;
  //   // cout << "update " << condition_name << "=" << update_val<< endl;
  // }
  // else return DB_FAILED;

  //遍历更新：
  TableHeap *table_heap = ud_table->GetTableHeap();//获取堆表
  vector<IndexInfo *> indexes;
  dberr_t getindexes_ret = now_dbs->catalog_mgr_->GetTableIndexes(table_name, indexes);//获取索引
  if (getindexes_ret != DB_SUCCESS) return DB_FAILED;
  for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter)//遍历堆表
  {
    if (allUpdate || checkCondition(ud_conditions, *iter, schema))//若全部更新或row满足更新条件，则更新（此处判断条件的函数和上面公用）
    {
      ud_table->primmap.erase((*iter).GetField(0)->GetIntVal());
      ud_table->uniquemap.erase(string((*iter).GetField(1)->GetCharVal(), (*iter).GetField(1)->GetLength()));
      // cout << "满足更新条件\n";
      auto newrow = GetNewRow(*iter, updateitems, schema);
      ud_table->primmap.emplace(newrow->GetField(0)->GetIntVal(), ud_table->primmap.size());
      ud_table->uniquemap.emplace(string(newrow->GetField(1)->GetCharVal(), (*iter).GetField(1)->GetLength()), ud_table->primmap.size());
      //调用堆表更新
      if (!table_heap->UpdateTuple(*newrow, (*iter).GetRowId(), nullptr))
      {
        cout << "更新堆表失败！\n";
        return DB_FAILED;
      }
      //B+树更新,遍历索引,先删除再插入
      for (uint32_t i = 0; i < indexes.size(); i++)
      {
        vector<Field> index_fields;
        index_fields.push_back(*(((*iter).GetField(indexes[i]->GetColIndex(0)))));
        Row key_row(index_fields);
        if (indexes[i]->GetIndex()->RemoveEntry(key_row, nullptr) != DB_SUCCESS)
        {
          cout << "更新删除索引记录失败\n";
          delete newrow;
          return DB_FAILED;
        } 

        index_fields.clear();
        index_fields.push_back(*(newrow->GetField(indexes[i]->GetColIndex(0))));
        Row new_key_row(index_fields);
        new_key_row.SetRowId(newrow->GetRowId());
        if (indexes[i]->GetIndex()->InsertEntry(new_key_row, newrow->GetRowId(), nullptr) != DB_SUCCESS)
        {
          cout << "更新插入索引记录失败\n";
          delete newrow;
          return DB_FAILED;
        }
      }
      delete newrow;
    }
  }

  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  pSyntaxNode fileast = ast->child_;
  //fstream f;
  string ff=fileast->val_;
  ifstream f(ff);
  //f.open(fileast->val_, ios::in);  // open file in normal way
  if (!f.is_open()) {
    cout << "Open file wrong!" << endl;
    return DB_FAILED;
  }
  string line;
  // ExecuteEngine engine;
  const int buf_size = 1024;
  char cmd[buf_size];
  int i = 0;
  while (++i) {  // File openning is not finished yet

    cout << "第i行: " << i << endl;
    getline(f, line);
    if(f.eof()) break;
    strcpy(cmd, line.c_str());
    for(int i=0; i<(int )line.size(); i++){
      //printf("%d ",cmd[i]);
      if(cmd[i]==';') {
        cmd[i+1]=0;
        //break;
      }
    }
  
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      return DB_FAILED;
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
      return DB_FAILED;
    } else {
      #ifdef ENABLE_PARSER_DEBUG
            printf("[INFO] Sql syntax parse ok!\n");
            SyntaxTreePrinter printer(MinisqlGetParserRootNode());
            printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id]);
      #endif
    }
    ExecuteContext context;
    (*this).Execute(MinisqlGetParserRootNode(), &context);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
  }
  f.close();
  return DB_SUCCESS;
}

// dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
// #ifdef ENABLE_EXECUTE_DEBUG
//   LOG(INFO) << "ExecuteExecfile" << std::endl;
// #endif
//   pSyntaxNode ast_son = ast->child_;
//   if (ast_son->type_ != kNodeString) return DB_FAILED;//检查语义
//   std::string file_name = ast_son->val_, file_path = "../../build/";
//   file_name = file_path + file_name;
//   ifstream fin;
//   fin.open(file_name.c_str(), ios::in);
  
//   //强行copy main：
//   const int buf_size = 1024;
//   char cmd[buf_size];
//   while (1) {
//     fin.getline(cmd, buf_size);
//     if (fin.eof()) break;//读到文件结束，break

//     // create buffer for sql input
//     YY_BUFFER_STATE bp = yy_scan_string(cmd);
//     if (bp == nullptr) {
//       LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
//       exit(1);
//     }
//     yy_switch_to_buffer(bp);

//     // init parser module
//     MinisqlParserInit();

//     // parse
//     yyparse();

//     // parse result handle
//     if (MinisqlParserGetError()) {
//       // error
//       printf("%s\n", MinisqlParserGetErrorMessage());
//     } else {
// //#ifdef ENABLE_PARSER_DEBUG
//       // printf("[INFO] Sql syntax parse ok!\n");
//       // SyntaxTreePrinter printer(MinisqlGetParserRootNode());
//       // printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
// //#endif
//     }

//     ExecuteContext context;
//     this->Execute(MinisqlGetParserRootNode(), &context);

//     // clean memory after parse
//     MinisqlParserFinish();
//     yy_delete_buffer(bp);
//     yylex_destroy();

//     // quit condition
//     if (context.flag_quit_) {
//       printf("bye!\n");
//       break;
//     }

//   }
//   return DB_SUCCESS;
// }

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;

  ofstream db_name_f("db_name.txt", ios::out);
  if (!db_name_f.is_open()) {
    cout << "Open file da_name wrong!" << endl;
    return DB_FAILED;
  }
  
  for (auto iter = dbs_.begin(); iter != dbs_.end(); iter++)
  {
    db_name_f << iter->first << endl;
  }

  db_name_f.close();

  return DB_SUCCESS;
}
