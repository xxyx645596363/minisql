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
    if (fields_[i] != nullptr)
    {
      buf += fields[i]->SerializeTo(buf);
    }
  }
  return GetSerializedSize();
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  int64_t rid_data = MACH_READ_FROM(int64_t, buf);
  
  return 0;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  return 0;
}

//wsx_end