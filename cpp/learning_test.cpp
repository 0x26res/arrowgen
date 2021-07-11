#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>

#include <memory.h>

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
  auto chunk12 = arrow::ChunkedArray::Make(arrays12, arrow::int32()).ValueOrDie();

  arrow::ArrayVector arrays21({array2, array1});
  auto chunk21 = arrow::ChunkedArray::Make(arrays12, arrow::int32()).ValueOrDie();

  auto schema = arrow::schema({
      arrow::field("col1", arrow::int32()),
      arrow::field("col2", arrow::int32()),
  });

  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {chunk12, chunk21});

  BOOST_REQUIRE_EQUAL(5, table->num_rows());

  BOOST_REQUIRE_EQUAL(2, table->column(0)->num_chunks());
  BOOST_REQUIRE_EQUAL(3, table->column(0)->chunk(0)->length());
  BOOST_REQUIRE_EQUAL(2, table->column(0)->chunk(1)->length());

  BOOST_REQUIRE_EQUAL(2, table->column(1)->num_chunks());
  BOOST_REQUIRE_EQUAL(3, table->column(1)->chunk(0)->length());
  BOOST_REQUIRE_EQUAL(2, table->column(1)->chunk(1)->length());
}

/** TODO: illustrate the fact that it guesses the dtype wrong */
BOOST_AUTO_TEST_CASE(IsThereABugWithArrays) {
  const arrow::FieldVector fields = {arrow::field("return_code", arrow::int32()),
                                     arrow::field("message", arrow::utf8())};

  const std::shared_ptr<arrow::DataType> struct_data_type = arrow::struct_(fields);
  const std::shared_ptr<arrow::DataType> list_of_struct_data_type = arrow::list(struct_data_type);

  const std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("search_results", list_of_struct_data_type)});

  arrow::MemoryPool *pool = arrow::default_memory_pool();

  std::shared_ptr<arrow::Int32Builder> return_code_builder = std::make_shared<arrow::Int32Builder>(pool);
  std::shared_ptr<arrow::StringBuilder> message_builder = std::make_shared<arrow::StringBuilder>(pool);
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> struct_fields_builders = {return_code_builder, message_builder};

  std::shared_ptr<arrow::StructBuilder> struct_builder =
      std::make_shared<arrow::StructBuilder>(struct_data_type, pool, struct_fields_builders);
  std::shared_ptr<arrow::ListBuilder> list_builder(
      std::make_shared<arrow::ListBuilder>(pool, struct_builder, list_of_struct_data_type));

  BOOST_REQUIRE(list_builder->type()->Equals(list_of_struct_data_type));

  // This should not be allowed:
  std::shared_ptr<arrow::ListBuilder> list_builder_using_struct_dtype(
      std::make_shared<arrow::ListBuilder>(pool, struct_builder, struct_data_type));

  std::shared_ptr<arrow::DataType> wrong_data_type =
      std::make_shared<arrow::ListType>(arrow::field("return_code", struct_data_type));

  BOOST_REQUIRE(!list_builder_using_struct_dtype->type()->Equals(list_of_struct_data_type));
  BOOST_REQUIRE(list_builder_using_struct_dtype->type()->Equals(wrong_data_type));
}

BOOST_AUTO_TEST_SUITE_END()
