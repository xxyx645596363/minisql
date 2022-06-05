#include "record/schema.h"
#include<iostream>

//wsx_start

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  //write the data of schema into buf
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);//magic_num
  buf += sizeof(uint32_t);//update the buf
  
  MACH_WRITE_TO(int, buf, columns_.size());//the number of column
  buf += sizeof(int);

  for (long unsigned int i = 0; i < columns_.size(); i++)
  {
    buf += columns_[i]->SerializeTo(buf);
  }

  return GetSerializedSize();
}

uint32_t Schema::GetSerializedSize() const {
  return static_cast<uint32_t>( sizeof(uint32_t) + sizeof(int) + columns_.size() * columns_[0]->GetSerializedSize() );
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in schema deserialize." << std::endl;
  }

  int n_column;
  std::vector<Column *> columns;

  ASSERT(MACH_READ_FROM(uint32_t, buf) == SCHEMA_MAGIC_NUM, "Wrong for MAGIC_NUM.");//check magic_num
  buf += sizeof(uint32_t);//update the buf

  n_column = MACH_READ_FROM(int, buf);
  buf += sizeof(int);

  for (int i = 0; i < n_column; i++)
  {
    Column * new_column;
    buf += Column::DeserializeFrom(buf, new_column, heap);
    std::cout<<new_column->GetName()<<std::endl;
    columns.push_back(new_column);
  }
  
  schema = ALLOC_P(heap, Schema)(columns);

  return static_cast<uint32_t>( sizeof(uint32_t) + sizeof(int) + schema->GetColumnCount() * schema->GetColumn(0)->GetSerializedSize() );
}

//wsx_end