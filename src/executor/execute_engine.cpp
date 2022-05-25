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
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
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
    return DB_FAILED;
  
  //创建新数据库
  DBStorageEngine *new_dbs = new DBStorageEngine(new_dbs_name);

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
    return DB_NOT_EXIST;

  //根据要删除的数据库名称获取并释放数据库指针，并从unordered_map中删除对应数据库
  DBStorageEngine *deleted_dbs = dbs_.at(deleted_dbs_name);
  delete deleted_dbs;
  dbs_.erase(deleted_dbs_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  //遍历输出所有数据库的名称
  for (auto iter = dbs_.begin(); iter != dbs_.end(); iter++)
  {
    std::cout << iter->first << std:: endl;
  }
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
  if (current_db_.equals(""))//当前无数据库
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
    std::cout << iter->GetTableName() << std::endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  //根据当前所在数据库名称获取当前数据库
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //获取创建表的名称
  if (ast->child_->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string new_table_name = ast->child_->val_;

  //获取列信息
  //获取主键列，存到set中
  if (ast->child_->next_->type_ != kNodeColumnDefinitionList) return DB_FAILED;//检查语义
  std::set<char *> prim_set;
  pSyntaxNode prim_node = ast->child_->next_->child_;
  while ((prim_node->type_ != kNodeColumnList || prim_node->val_[0] != 'p') && (prim_node != nullptr)) prim_node = col_node->next_;//定位存储主键的子树
  if (prim_node == nullptr || prim_node->type_ != kNodeColumnList) return DB_FAILED;
  for (pSyntaxNode node = prim_node->child_; node != nullptr; node = node->next_)
  {
    prim_set.insert(node->val_);
  }  
  //获取列的其他信息，存进vector
  std::vector<Column *> columns;
  uint32_t index = 0;
  pSyntaxNode col_node;
  for (col_node = ast->child_->next_->child_; col_node->type_ == kNodeColumnDefinition; col_node = col_node->next_, index++)
  {
    //获取列的name
    char *col_nume = col_node->child_->val_;
    //获取列的type和length
    char *col_type = col_node->child_->next_->val_;
    TypeId col_typeid;
    std:string col_len_str;
    uint32_t col_len;
    switch (col_type[0])
    {
    case 'i'://type为int
      col_typeid = kTypeInt;
      col_len = sizeof(int);
      break;
    case 'f'://type为float
      col_typeid = kTypeFloat;
      col_len = sizeof(float);
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
    //获取列的nullable
    bool nullable = true;
    if (prim_set.find(col_name) != prim_set.end()) nullable = false;//若在set中找到该列name，则为主键，非空
    //获取列的unique
    bool unique = false;
    if (col_node->val_[0] == 'u')//判断为unique
      unique = true;
    //创建column并插入到vector中
    Column *new_column = new Column(col_name,  col_typeid, col_len, index, nullable, unique);
    columns.push_back(new_column);
  }
  if (col_node->type_ != kNodeColumnList) return DB_FAILED;//检察语义

  //用column的vector数组创建schema
  Schema *new_Schema = new Schema(columns);

  //创建空的TableInfo用于存储创建table函数的返回值
  TableInfo *new_tableinfo = new TableInfo;

  //调用CatalogManager的CreateTable函数为数据库创建新的表
  dberr_t createtable_ret = now_dbs->catalog_mgr_->CreateTable(new_table_name, new_Schema, nullptr, new_tableinfo);
  //返回创建结果
  return createtable_ret;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  //获取当前数据库
  DBStorageEngine *now_dbs = dbs_.at(current_db_);
  //获取要删除的表的名称
  if (ast->child_->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string droptable_name = ast->child_->val_;

  //调用CatalogManager的DropTable函数
  dberr_t droptable_ret = now_dbs->catalog_mgr_->DropTable(droptable_name);
  //返回删除结果
  return droptable_ret;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  //获取创建索引的名称：
  pSyntaxNode ast_son1 = ast->child_, ast_son2 = ast_son1->next_, ast_son3 = ast_son2->next_;
  if (ast_son1->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string new_index_name = ast_son1->val_;

  //获取创建索引的表的名称：
  if (ast_son2->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
  std::string table_name = ast_son2->val_;

  //获取索引的attribute,以vector的结果
  std::vector<std::string> index_keys;
  if (ast_son3->type_ != kNodeColumnList || strcmp(ast_son3->val_, "index keys")) return DB_FAILED;//检查语义
  for (pSyntaxNode key_node = ast_son3->child_; key_node != nullptr; key_node = key_node->next_)
  {
    if (key_node->type_ != kNodeIdentifier) return DB_FAILED;//检查语义
    index_keys.push_back(key_node->val_);
  }

  //获取当前数据库
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //调用catalog的CreateIndex函数创建索引：
  IndexInfo *index_info = new IndexInfo();//创建一个IndexInfo用于引用返回
  dberr_t createindex_ret = now_dbs->catalog_mgr_->CreateIndex(table_name, new_index_name, index_keys, nullptr, index_info);
  return createindex_ret;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //获取删除的索引名称
  pSyntaxNode ast_son = ast->child_;
  if (ast_son->type_ != kNodeIdentifier) reutrn DB_FAILED;//检查语义
  std::string drop_index_name = ast_son->val_;

  //获取当前数据库
  DBStorageEngine *now_dbs = dbs_.at(current_db_);

  //调用函数遍历每个table，删除索引
  std::vector<TableInfo *> tables;
  dberr_t dropindex_ret = DB_INDEX_NOT_FOUND, gettables_ret = now_dbs->catalog_mgr_->GetTables(tables);//获取所有table
  for (int i = 0; i < tables.size(); i++)
  {
    std::string table_name = tables[i]->GetTableName();//获取table的名字
    if (now_dbs->catalog_mgr_->DropIndex(table_name, drop_index_name) == DB_SUCCESS) dropindex_ret = DB_SUCCESS;//一旦有一个table删除成功，则返回成功
  }
  
  return dropindex_ret;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
