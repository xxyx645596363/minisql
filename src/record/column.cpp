#include "record/column.h"
#include <iostream>
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

//wsx_start

uint32_t Column::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);//write magic_num
  buf += sizeof(uint32_t);//update the buf

  MACH_WRITE_UINT32(buf, name_.length());//序列化name_的长度
  buf += sizeof(uint32_t);
  MACH_WRITE_STRING(buf, name_);//write the name of the field
  buf += name_.length();

  MACH_WRITE_INT32(buf, type_);//write TypeId
  buf += sizeof(int);

  MACH_WRITE_UINT32(buf, len_);//write the length
  buf += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf, table_ind_);//write column position in table
  buf += sizeof(uint32_t);

  MACH_WRITE_TO(bool, buf, nullable_);//write whether the column can be null
  buf += sizeof(bool);

  MACH_WRITE_TO(bool, buf, unique_);//write whether the column is unique
  buf += sizeof(bool);

  return GetSerializedSize();
}

uint32_t Column::GetSerializedSize() const {
  return static_cast<uint32_t>( 4 * sizeof(uint32_t) + name_.length() + sizeof(int) + 2 * sizeof(bool) );
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // if (column != nullptr) {
  //   LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  // }

  uint32_t column_len, column_index, name_len;
  TypeId column_type;
  int column_type_int;
  bool column_nullable, column_unique;
std::cout<<MACH_READ_FROM(uint32_t, buf)<<std::endl;
  ASSERT(MACH_READ_FROM(uint32_t, buf) == COLUMN_MAGIC_NUM, "Wrong for MAGIC_NUM.");//check magic_num
  buf += sizeof(uint32_t);//update the buf

  name_len = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);
  std::string column_name(buf, name_len);//read the name of the field
  buf += name_len;

  column_type_int = MACH_READ_FROM(int, buf);//read the type
  buf += sizeof(int);
  switch (column_type_int)
  {
  case kTypeInvalid:
    column_type = kTypeInvalid;
    break;
  case kTypeInt:
    column_type = kTypeInt;
    break;
  case kTypeFloat:
    column_type = kTypeFloat;
    break;
  default: 
    column_type = kTypeChar;
    break;
  }

  column_len = MACH_READ_FROM(uint32_t, buf);//write the length 
  buf += sizeof(uint32_t);

  column_index = MACH_READ_FROM(uint32_t, buf);
  buf += sizeof(uint32_t);//update the buf

  column_nullable = MACH_READ_FROM(bool, buf);
  buf += sizeof(bool);

  column_unique = MACH_READ_FROM(bool, buf);
  buf += sizeof(bool);

  if (column_type == TypeId::kTypeChar)
  {
    column = ALLOC_P(heap, Column)(column_name, column_type, column_len, column_index, column_nullable, column_unique);
  }
  else
  {
    column = ALLOC_P(heap, Column)(column_name, column_type, column_index, column_nullable, column_unique);
  }

  return static_cast<uint32_t>( 3 * sizeof(uint32_t) + sizeof(std::string) + sizeof(int) + 2 * sizeof(bool) );
}

//wsx_end