#include <vector>
#include <unordered_map>
#include <iostream>
using namespace std;

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i),
            Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
    };
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }
  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }

  Fields *new_field = new Fields{
          Field(TypeId::kTypeInt, 233),
          Field(TypeId::kTypeChar, const_cast<char *>("characters"), 11, true),
          Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
  };
  Row new_row(*new_field);
  RowId upd_rowid(3, 23);
  table_heap->UpdateTuple(new_row, upd_rowid, nullptr);
  RowId del_rowid(5, 20);
  table_heap->ApplyDelete(del_rowid, nullptr);
  bool delete_success = true;
  
  TableIterator it = table_heap->Begin(nullptr);
  // std::cout << "out table_heap_test *it.GetRowId().GetPageId(): " << (*it).GetRowId().GetPageId() << endl;
  // std::cout << "out table_heap_test it->GetRowId().GetPageId(): " << it->GetRowId().GetPageId() << endl;
  // TableIterator it = table_heap->End();
  for(;it!=table_heap->End();it++){
    // std::cout << "table_heap_test for_iter\n";
    if(it->GetRowId().GetPageId() == 5 && it->GetRowId().GetSlotNum() == 20) delete_success = false;
    if(it->GetRowId().GetPageId() == 3 && it->GetRowId().GetSlotNum() == 23){
      for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
        ASSERT_EQ(CmpBool::kTrue, (*it).GetField(j)->CompareEquals(*new_row.GetField(j)));
      }
    }
    Row row(it->GetRowId());
  // std::cout << "table_heap_test it->GetRowId().GetSlotNum(): " << it->GetRowId().GetSlotNum() << std::endl;
    table_heap->GetTuple(&row, nullptr);
    
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, (*it).GetField(j)->CompareEquals(*row.GetField(j)));
    }
    // std::cout << "table_heap_test for_iter_end\n";
  }
  ASSERT_EQ(delete_success, true);
}
