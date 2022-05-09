#include "record/row.h"

//wsx_start

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  //get the data of rid
  int64_t rid_data = rid_.Get();
  //write the data of rid
  MACH_WRITE_TO(int64_t, buf, rid_data);
  buf += sizeof(int64_t);

  //write the number of field
  int64_t field_num = fields_.size();
  MACH_WRITE_TO(int64_t, buf, field_num);
  buf += sizeof(int64_t);

  //get and write the null_bitmap
  int64_t null_bitmap = 0;
  for (int64_t i = 0; i < field_num; i++)
  {
    if (fields_[i] == nullptr)
    {
      null_bitmap |= (1 << i);
    }
  }
  MACH_WRITE_TO(int64_t, buf, null_bitmap);
  buf += sizeof(int64_t);

  //write the data of field by using its function
  for (int64_t i = 0; i < field_num; i++)
  {
    buf += fields_[i]->SerializeTo(buf);
  }
  return GetSerializedSize(schema);
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  //read the data of rid
  int64_t rid_data = MACH_READ_FROM(int64_t, buf);
  buf += sizeof(int64_t);
  rid_ = RowId(rid_data);
  
  //read the data of header of the row
  int64_t field_num = MACH_READ_FROM(int64_t, buf);
  buf += sizeof(int64_t);
  int64_t null_bitmap = MACH_READ_FROM(int64_t, buf);
  buf += sizeof(int64_t);

  for (int64_t i = 0; i < field_num; i++)
  {
    bool is_null = null_bitmap & (1 << i);
    const Column *col = schema->GetColumn(i);
    Field *fie;
    buf += Field::DeserializeFrom(buf, col->GetType(), &fie, is_null, heap_);
    fields_.push_back(fie);
  }
  return GetSerializedSize(schema);
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t buf_add = 3 * sizeof(int64_t);
  int64_t field_num = fields_.size();
  for (int64_t i = 0; i < field_num; i++)
  {
    buf_add += fields_[i]->GetSerializedSize();
  }
  return buf_add;
}

//wsx_end