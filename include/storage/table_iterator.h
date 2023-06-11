#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
 explicit  TableIterator(TableHeap* Table_heap,Transaction* txn, RowId _rid);

 explicit  TableIterator(TableHeap* Table_heap,Transaction* txn);

TableIterator(const TableIterator &other);//拷贝构造函数不显式调用

 virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

TableIterator operator++(int);

  bool FindNextValidTuple();

private:
  // add your own private member variables here
 TableHeap* _table_heap;
 Transaction* _txn;
 Row* _current_row; // 新增成员变量，用于存储当前行的指针,记得需要维护
};

#endif  // MINISQL_TABLE_ITERATOR_H
