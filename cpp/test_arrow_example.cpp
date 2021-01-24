#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>

#include "arrow_example.h"

BOOST_AUTO_TEST_SUITE(TestArrowExample)

BOOST_AUTO_TEST_CASE(test_simple_example) {
  std::vector<data_row> rows = {
      {1, 1.0, {1.0}}, {2, 2.0, {1.0, 2.0}}, {3, 3.0, {1.0, 2.0, 3.0}}};

  std::shared_ptr<arrow::Table> table;
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(), VectorToColumnarTable(rows, &table));

  std::vector<data_row> expected_rows;
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(),
                      ColumnarTableToVector(table, &expected_rows));

  BOOST_REQUIRE_EQUAL(rows.size(), expected_rows.size());
}

BOOST_AUTO_TEST_CASE(test_nested_example) {
  std::vector<data_row> rows = {
      {1, 1.0, {1.0}}, {2, 2.0, {1.0, 2.0}}, {3, 3.0, {1.0, 2.0, 3.0}}};
  std::vector<nested_repeated> messages = {{rows}, {rows}, {rows}};

  std::shared_ptr<arrow::Table> table;
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(),
                      VectorToColumnarTable(messages, &table));

  BOOST_REQUIRE_EQUAL(messages.size(), table->num_rows());

  std::vector<nested_repeated> read_messages;
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(),
                      ColumnarTableToVector(table, read_messages));

  for (size_t index = 0; index < messages.size(); ++index) {
    BOOST_REQUIRE_EQUAL(messages[index], read_messages[index]);
  }
}

BOOST_AUTO_TEST_SUITE_END()
