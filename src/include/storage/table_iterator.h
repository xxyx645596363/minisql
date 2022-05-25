#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(TableHeap *tableheap, Row *row) {
    return this->row_->GetRowId() == itr.row_->GetRowId();
  }

  explicit TableIterator(const TableIterator &other) {
    return !(this->row_->GetRowId() == itr.row_->GetRowId());
  } 

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  TableHeap *tableheap_;
  Row *row_;
};

#endif //MINISQL_TABLE_ITERATOR_H
