#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }//这个column构造函数用于构造 int 和 float类型的列
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),//std::move() 是为了将字符串 column_name 的所有权从构造函数的参数转移到类的成员变量 name_ 中
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}//这个column构造函数用于构造 char类型的列，使用断言检查列类型是否为 TypeId::kTypeChar，如果不是，则触发断言错误。

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
      char* buff_1_Byte = buf;
      MACH_WRITE_UINT32(buff_1_Byte,COLUMN_MAGIC_NUM);//在buffer开头存入魔数
      buff_1_Byte+=4;

      MACH_WRITE_UINT32(buff_1_Byte,name_.length());//获取字符的数量,也就是字符串所用字节数
      buff_1_Byte+=4;//先保存name的长度，再保存name的值，便于反序列化提取

      MACH_WRITE_STRING(buff_1_Byte,name_);//将name存入buffer
      buff_1_Byte += MACH_STR_SERIALIZED_SIZE(name_);//buffer后移

      MACH_WRITE_TO(TypeId,buff_1_Byte,type_);//枚举类型处理
      buff_1_Byte += sizeof(TypeId);

      MACH_WRITE_UINT32(buff_1_Byte,len_);//char类型默认为0
      buff_1_Byte += 4;

      MACH_WRITE_UINT32(buff_1_Byte,table_ind_);// column position in table
      buff_1_Byte += 4;

      *(buff_1_Byte++) = nullable_;//1字节
      *(buff_1_Byte++) = unique_;//1 字节
      return static_cast<uint32_t>(buff_1_Byte - buf);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
      return (4* sizeof(uint32_t) + 2* sizeof(bool ) + MACH_STR_SERIALIZED_SIZE(name_) + sizeof(TypeId));
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
 // if (column != nullptr) {
 //   LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
 // }
  char* buff_1_byte = buf;
  ASSERT(MACH_READ_UINT32(buff_1_byte) == COLUMN_MAGIC_NUM,"Invalid magic number during deserialization of Column.");
  buff_1_byte += 4;

  uint32_t name_length = MACH_READ_UINT32(buff_1_byte);
  buff_1_byte += 4;
  std::string name = std::string(reinterpret_cast<const char *>(buff_1_byte),name_length);
  buff_1_byte += MACH_STR_SERIALIZED_SIZE(name);//buffer后移

  TypeId type_id = MACH_READ_FROM(TypeId,buff_1_byte);//处理type枚举类型
  buff_1_byte += sizeof(TypeId);

  uint32_t len_ = MACH_READ_UINT32(buff_1_byte);
  buff_1_byte += 4;
  uint32_t table_ind_ = MACH_READ_UINT32(buff_1_byte);  // column position in table
  buff_1_byte += 4;
  bool nullable_ = *(buff_1_byte++);   // whether the column can be null
  bool unique_ = *(buff_1_byte++);     // whether the column is unique


  if(type_id == TypeId::kTypeChar){
   // delete column;//原先的 column 如果申请了空间要释放掉
    column = new Column(std::move(name),type_id,len_,table_ind_,nullable_,unique_);
  }else{
   // delete column;//原先的 column 如果申请了空间要释放掉
    column = new Column(std::move(name),type_id,table_ind_,nullable_,unique_);
  }

  return static_cast<uint32_t>(buff_1_byte - buf);
}
