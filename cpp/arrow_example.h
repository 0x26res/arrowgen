#ifndef ARROWGENTEST_ARROW_EXAMPLE_H
#define ARROWGENTEST_ARROW_EXAMPLE_H

#endif // ARROWGENTEST_ARROW_EXAMPLE_H

#include <arrow/api.h>
#include <ostream>

// While we want to use columnar data structures to build efficient operations,
// we often receive data in a row-wise fashion from other systems. In the
// following, we want give a brief introduction into the classes provided by
// Apache Arrow by showing how to transform row-wise data into a columnar table.
//
// The data in this example is stored in the following struct:
struct data_row {
  int64_t id;
  double cost;
  std::vector<double> cost_components;

  bool operator==(const data_row &rhs) const;
  bool operator!=(const data_row &rhs) const;

  friend std::ostream &operator<<(std::ostream &os, const data_row &row);
};

arrow::Status VectorToColumnarTable(const std::vector<struct data_row> &rows,
                                    std::shared_ptr<arrow::Table> *table);

arrow::Status ColumnarTableToVector(const std::shared_ptr<arrow::Table> &table,
                                    std::vector<struct data_row> *rows);

struct nested_repeated {
  std::vector<data_row> rows;

  bool operator==(const nested_repeated &rhs) const;
  bool operator!=(const nested_repeated &rhs) const;

  friend std::ostream &operator<<(std::ostream &os,
                                  const nested_repeated &repeated);
};

arrow::Status
VectorToColumnarTable(const std::vector<struct nested_repeated> &rows,
                      std::shared_ptr<arrow::Table> *table);

arrow::Status ColumnarTableToVector(const std::shared_ptr<arrow::Table> &table,
                                    std::vector<struct nested_repeated> &rows);