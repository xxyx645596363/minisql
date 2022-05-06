#include "record/column.h"

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

  MACH_WRITE_STRING(buf, name_);//write the name of the field
  buf += sizeof(std::string);

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
  return static_cast<uint32_t>( 3 * sizeof(uint32_t) + sizeof(std::string) + sizeof(int) + 2 * sizeof(bool) );
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }

  uint32_t column_len, column_index;
  int column_type;
  std::string column_name;
  bool column_nullable, column_unique;

  ASSERT(MACH_READ_FROM(uint32_t, buf) == COLUMN_MAGIC_NUM, "Wrong for MAGIC_NUM.");//check magic_num
  buf += sizeof(uint32_t);//update the buf

  column_name = MACH_READ_FROM(std::string, buf);//read the name of the field
  buf += sizeof(std::string);

  column_type = MACH_READ_FROM(int, buf);//read the type
  buf += sizeof(int);

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

  return GetSerializedSize();
}

//wsx_end