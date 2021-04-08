#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>

#include <arrow/api.h>

BOOST_AUTO_TEST_SUITE(ArrowLearningTest)

BOOST_AUTO_TEST_CASE(CanYouHaveHeterogenousChunk) {

  arrow::Int32Builder builder1;
  builder1.Append(1);
  builder1.Append(2);
  builder1.Append(3);


  auto array1 = builder1.Finish().ValueOrDie();

  arrow::Int32Builder builder2;
  builder2.Append(4);
  builder2.Append(5);

  auto array2 = builder2.Finish().ValueOrDie();
  arrow::ArrayVector arrays12({array1, array2});
  auto chunk12 =
      arrow::ChunkedArray::Make(arrays12, arrow::int32()).ValueOrDie();

  arrow::ArrayVector arrays21({array2, array1});
  auto chunk21 =
      arrow::ChunkedArray::Make(arrays12, arrow::int32()).ValueOrDie();

  auto schema = arrow::schema({
      arrow::field("col1", arrow::int32()),
      arrow::field("col2", arrow::int32()),
  });

  std::shared_ptr<arrow::Table> table =
      arrow::Table::Make(schema, {chunk12, chunk21});

  BOOST_REQUIRE_EQUAL(5, table->num_rows());

  BOOST_REQUIRE_EQUAL(2, table->column(0)->num_chunks());
  BOOST_REQUIRE_EQUAL(3, table->column(0)->chunk(0)->length());
  BOOST_REQUIRE_EQUAL(2, table->column(0)->chunk(1)->length());

  BOOST_REQUIRE_EQUAL(2, table->column(1)->num_chunks());
  BOOST_REQUIRE_EQUAL(3, table->column(1)->chunk(0)->length());
  BOOST_REQUIRE_EQUAL(2, table->column(1)->chunk(1)->length());
}

BOOST_AUTO_TEST_SUITE_END()
