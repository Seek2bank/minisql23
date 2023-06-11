#include "storage/table_heap.h"
#include <unordered_map>
#include <vector>
#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  std::vector<Row> Row_test;
  std::vector<Row> Row_test2;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  //测试insert Tuple
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));

    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
      Row_test.push_back(row);
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);

  TableIterator begin = table_heap->Begin(nullptr);
  TableIterator end = table_heap->End();
int i = 0;
  for (TableIterator iter = begin; iter != end; iter++) {
    const Row& row = *iter;
    Row row1(RowId(row.GetRowId()));
    table_heap->GetTuple(&row1, nullptr);
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      //  ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(*Row_test[i].GetField(j)));
      if (row.GetField(j)->CompareEquals(*row1.GetField(j)) != CmpBool::kTrue) {
        std::cout << "Mismatch at row " << i << ", column " << j << std::endl;
        std::cout << "Expected: " << row.GetField(j)->toString() << std::endl;
        std::cout << "Actual: " << row1.GetField(j)->toString()<< std::endl;
      }
    }
    i++;
  }

  for(int i = 0; i < Row_test.size(); i++){
    Row row(RowId(Row_test[i].GetRowId()));
    table_heap->GetTuple(&row, nullptr);
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      if (row.GetField(j)->CompareEquals(*Row_test[i].GetField(j)) != CmpBool::kTrue) {
        std::cout << "Mismatch at row " << i << ", column " << j << std::endl;
        std::cout << "Expected: " << Row_test[i].GetField(j)->toString() << std::endl;
        std::cout << "Actual: " << row.GetField(j)->toString()<< std::endl;
      }
    }
  }
  //测试getTuple
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));

    }
    // free spaces
    delete row_kv.second;
  }

  //测试updateTuple
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);

    //修改row_test =  Row_test[i].GetRowId() 这条记录
    if(i == 40){//删除这条Tuple再插入,插入失败
      table_heap->MarkDelete(Row_test[i].GetRowId(), nullptr);
      table_heap->ApplyDelete(Row_test[i].GetRowId(), nullptr);
      ASSERT_FALSE(table_heap->UpdateTuple(row,Row_test[i].GetRowId() , nullptr));
    }else{
      ASSERT_TRUE(table_heap->UpdateTuple(row,Row_test[i].GetRowId() , nullptr));
    }
    delete[] characters;
  }



  ASSERT_EQ(size, 0);
}


