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

/** TODO: illustrate the fact that it guesses the dtype wrong */
BOOST_AUTO_TEST_CASE(IsThereABugWithArrays) {
  const arrow::FieldVector fields = {
      arrow::field("return_code", arrow::int32()),
      arrow::field("message", arrow::utf8())};

  const std::shared_ptr<arrow::DataType> structDataType =
      arrow::struct_(fields);
  const std::shared_ptr<arrow::DataType> listDataType =
      arrow::list(structDataType);

  const std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("search_results", listDataType)});

  arrow::MemoryPool *pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::Int32Builder> return_code_builder =
      std::make_shared<arrow::Int32Builder>(pool);
  std::shared_ptr<arrow::StringBuilder> message_builder =
      std::make_shared<arrow::StringBuilder>(pool);

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> fieldBuilder = {
      return_code_builder, message_builder};
  std::shared_ptr<arrow::StructBuilder> search_results_struct_builder =
      std::make_shared<arrow::StructBuilder>(structDataType, pool,
                                             fieldBuilder);
  std::shared_ptr<arrow::ListBuilder> search_results_list_builder_(
      std::make_shared<arrow::ListBuilder>(pool, search_results_struct_builder,
                                           listDataType));
  //
  //  std::shared_ptr<arrow::Array> return_code_array;
  //  return_code_builder->Finish(&return_code_array);
  //
  std::shared_ptr<arrow::Array> array;
  search_results_list_builder_->Finish(&array);

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.push_back(array);

  static std::shared_ptr<arrow::Table> table =
      arrow::Table::Make(schema, arrays);

  std::cout << "Required schema:     " << *schema << std::endl;
  std::cout << "Table schema:        " << *table->schema() << std::endl;
  std::cout << "Col 0 array dtype:   " << *table->column(0)->type()
            << std::endl;
  std::cout << "Col 0 builder dtype: " << *search_results_list_builder_->type()
            << std::endl;
}

BOOST_AUTO_TEST_SUITE_END()
