#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  char* buff_1_Byte = buf;
  uint32_t serializedSize_tmp = 0;
  MACH_WRITE_UINT32(buff_1_Byte,SCHEMA_MAGIC_NUM);//在buffer开头存入魔数
  buff_1_Byte += sizeof(uint32_t);

  // Serialize the number of columns
  MACH_WRITE_UINT32(buff_1_Byte, static_cast<uint32_t>(columns_.size()));
  buff_1_Byte += sizeof(uint32_t);

  // Serialize each column
  for (const auto &column : columns_) {
    serializedSize_tmp = column->SerializeTo(buff_1_Byte);
    buff_1_Byte += serializedSize_tmp;
  }

  return buff_1_Byte - buf;

}

uint32_t Schema::GetSerializedSize() const {
  uint32_t serializedSize = 0;


  // Size of the number of columns
  serializedSize += 2*sizeof(uint32_t);

  // Size of each column
  for (const auto &column : columns_) {
    serializedSize += column->GetSerializedSize();
  }

  return serializedSize;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  char* buff_1_Byte = buf;

 // ASSERT(schema != nullptr,"Pointer to column is not null in schema deserialize.");

  uint32_t deserializedSize = 0;
  ASSERT(MACH_READ_UINT32(buff_1_Byte) == SCHEMA_MAGIC_NUM,"Invalid magic number during deserialization of Schema.");
  buff_1_Byte += sizeof (uint32_t);

  // Deserialize the number of columns
  uint32_t columnCount = MACH_READ_UINT32(buff_1_Byte);
  deserializedSize += sizeof(uint32_t);

  // Deserialize each column
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < columnCount; ++i) {
    Column *column = nullptr;
    deserializedSize = Column::DeserializeFrom(buf + deserializedSize, column);
    buff_1_Byte += deserializedSize;
    columns.push_back(column);
  }

  // Create a new schema object

  //delete schema;//如果传进来的schema分配过内存，要先释放，解决内存泄漏的问题,但是delete也可能带来问题，如果原来的schema还有用呢？
  schema = new Schema(columns, true);
  return buff_1_Byte - buf;
}