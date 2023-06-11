#ifndef MINISQL_COLUMN_H
#define MINISQL_COLUMN_H

#include <string>

#include "common/macros.h"
#include "record/types.h"

class Column {
  friend class Schema;

 public:
  Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique);

  Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique);

  Column(const Column *other);

  std::string GetName() const { return name_; }

  uint32_t GetLength() const { return len_; }

  void SetTableInd(uint32_t ind) { table_ind_ = ind; }

  uint32_t GetTableInd() const { return table_ind_; }

  bool IsNullable() const { return nullable_; }

  bool IsUnique() const {return unique_; }

  TypeId GetType() const { return type_; }//返回Typeid

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, Column *&column);//使得指针的值能够被修改，column是一个指针

 private:
  static constexpr uint32_t COLUMN_MAGIC_NUM = 210928;//在序列化时被写入到字节流的头部并在反序列化中被读出以验证我们在反序列化时生成的对象是否符合预期。
  std::string name_;
  TypeId type_;
  uint32_t len_{0};  // for char type this is the maximum byte length of the string data,
  // otherwise is the fixed size
  uint32_t table_ind_{0};  // column position in table
  bool nullable_{false};   // whether the column can be null
  bool unique_{false};     // whether the column is unique
};

#endif  // MINISQL_COLUMN_H
