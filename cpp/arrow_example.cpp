// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow_example.h"

#include <cstdint>
#include <iostream>
#include <vector>

#include <arrow/api.h>

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;

// Transforming a vector of structs into a columnar Table.
//
// The final representation should be an `arrow::Table` which in turn
// is made up of an `arrow::Schema` and a list of
// `arrow::ChunkedArray` instances. As the first step, we will iterate
// over the data and build up the arrays incrementally.  For this
// task, we provide `arrow::ArrayBuilder` classes that help in the
// construction of the final `arrow::Array` instances.
//
// For each type, Arrow has a specially typed builder class. For the primitive
// values `id` and `cost` we can use the respective `arrow::Int64Builder` and
// `arrow::DoubleBuilder`. For the `cost_components` vector, we need to have two
// builders, a top-level `arrow::ListBuilder` that builds the array of offsets
// and a nested `arrow::DoubleBuilder` that constructs the underlying values
// array that is referenced by the offsets in the former array.
arrow::Status VectorToColumnarTable(const std::vector<struct data_row> &rows,
                                    std::shared_ptr<arrow::Table> *table) {
  // The builders are more efficient using
  // arrow::jemalloc::MemoryPool::default_pool() as this can increase the size
  // of the underlying memory regions in-place. At the moment, arrow::jemalloc
  // is only supported on Unix systems, not Windows.
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  Int64Builder id_builder(pool);
  DoubleBuilder cost_builder(pool);
  ListBuilder components_builder(pool, std::make_shared<DoubleBuilder>(pool));
  // The following builder is owned by components_builder.
  DoubleBuilder &cost_components_builder =
      *(static_cast<DoubleBuilder *>(components_builder.value_builder()));

  // Now we can loop over our existing data and insert it into the builders. The
  // `Append` calls here may fail (e.g. we cannot allocate enough additional
  // memory). Thus we need to check their return values. For more information on
  // these values, check the documentation about `arrow::Status`.
  for (const data_row &row : rows) {
    ARROW_RETURN_NOT_OK(id_builder.Append(row.id));
    ARROW_RETURN_NOT_OK(cost_builder.Append(row.cost));

    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    ARROW_RETURN_NOT_OK(components_builder.Append());
    // Store the actual values. The final nullptr argument tells the underyling
    // builder that all added values are valid, i.e. non-null.
    ARROW_RETURN_NOT_OK(cost_components_builder.AppendValues(
        row.cost_components.data(), row.cost_components.size()));
  }

  // At the end, we finalise the arrays, declare the (type) schema and combine
  // them into a single `arrow::Table`:
  std::shared_ptr<arrow::Array> id_array;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
  std::shared_ptr<arrow::Array> cost_array;
  ARROW_RETURN_NOT_OK(cost_builder.Finish(&cost_array));
  // No need to invoke cost_components_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::Array> cost_components_array;
  ARROW_RETURN_NOT_OK(components_builder.Finish(&cost_components_array));

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int64()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};

  auto schema = std::make_shared<arrow::Schema>(schema_vector);

  // The final `table` variable is the one we then can pass on to other
  // functions that can consume Apache Arrow memory structures. This object has
  // ownership of all referenced data, thus we don't have to care about
  // undefined references once we leave the scope of the function building the
  // table and its underlying arrays.
  *table =
      arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});

  return arrow::Status::OK();
}

arrow::Status ColumnarTableToVector(const std::shared_ptr<arrow::Table> &table,
                                    std::vector<struct data_row> *rows) {
  // To convert an Arrow table back into the same row-wise representation as in
  // the above section, we first will check that the table conforms to our
  // expected schema and then will build up the vector of rows incrementally.
  //
  // For the check if the table is as expected, we can utilise solely its
  // schema.
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int64()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);

  if (!expected_schema->Equals(*table->schema())) {
    // The table doesn't have the expected schema thus we cannot directly
    // convert it to our target representation.
    return arrow::Status::Invalid("Schemas are not matching!");
  }

  // As we have ensured that the table has the expected structure, we can unpack
  // the underlying arrays. For the primitive columns `id` and `cost` we can use
  // the high level functions to get the values whereas for the nested column
  // `cost_components` we need to access the C-pointer to the data to copy its
  // contents into the resulting `std::vector<double>`. Here we need to be care
  // to also add the offset to the pointer. This offset is needed to enable
  // zero-copy slicing operations. While this could be adjusted automatically
  // for double arrays, this cannot be done for the accompanying bitmap as often
  // the slicing border would be inside a byte.

  auto ids =
      std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  auto costs =
      std::static_pointer_cast<arrow::DoubleArray>(table->column(1)->chunk(0));
  auto cost_components =
      std::static_pointer_cast<arrow::ListArray>(table->column(2)->chunk(0));
  auto cost_components_values =
      std::static_pointer_cast<arrow::DoubleArray>(cost_components->values());
  // To enable zero-copy slices, the native values pointer might need to account
  // for this slicing offset. This is not needed for the higher level functions
  // like Value(…) that already account for this offset internally.
  const double *ccv_ptr = cost_components_values->data()->GetValues<double>(1);

  for (int64_t i = 0; i < table->num_rows(); i++) {
    // Another simplification in this example is that we assume that there are
    // no null entries, e.g. each row is fill with valid values.
    int64_t id = ids->Value(i);
    double cost = costs->Value(i);
    const double *first = ccv_ptr + cost_components->value_offset(i);
    const double *last = ccv_ptr + cost_components->value_offset(i + 1);
    std::vector<double> components_vec(first, last);
    rows->push_back({id, cost, components_vec});
  }

  return arrow::Status::OK();
}

arrow::Status
VectorToColumnarTable(const std::vector<struct nested_repeated> &rows,
                      std::shared_ptr<arrow::Table> *table) {
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  std::shared_ptr<arrow::DataType> row_data_type = arrow::struct_({
      arrow::field("id", arrow::int64()),
      arrow::field("double", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64())),
  });

  std::shared_ptr<Int64Builder> id_builder =
      std::make_shared<Int64Builder>(pool);
  std::shared_ptr<DoubleBuilder> cost_builder =
      std::make_shared<DoubleBuilder>(pool);
  std::shared_ptr<ListBuilder> components_builder =
      std::make_shared<ListBuilder>(pool,
                                    std::make_shared<DoubleBuilder>(pool));
  DoubleBuilder *cost_components_builder =
      (static_cast<DoubleBuilder *>(components_builder->value_builder()));
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders = {
      id_builder, cost_builder, components_builder};

  std::shared_ptr<arrow::StructBuilder> rows_builder =
      std::make_shared<arrow::StructBuilder>(row_data_type, pool, builders);
  arrow::ListBuilder rows_list_builder(pool, rows_builder, row_data_type);

  for (const nested_repeated &message : rows) {
    ARROW_RETURN_NOT_OK(rows_list_builder.Append());
    for (const data_row &row : message.rows) {
      ARROW_RETURN_NOT_OK(rows_builder->Append());
      ARROW_RETURN_NOT_OK(id_builder->Append(row.id));
      ARROW_RETURN_NOT_OK(cost_builder->Append(row.cost));
      ARROW_RETURN_NOT_OK(components_builder->Append());
      for (double cost_component : row.cost_components) {
        ARROW_RETURN_NOT_OK(cost_components_builder->Append(cost_component));
      }
    }
  }

  std::shared_ptr<arrow::Array> struct_array;
  ARROW_RETURN_NOT_OK(rows_list_builder.Finish(&struct_array));

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      std::make_shared<arrow::Field>("rows", arrow::list(row_data_type)),
  };

  std::shared_ptr<arrow::Schema> schema =
      std::make_shared<arrow::Schema>(schema_vector);

  *table = arrow::Table::Make(schema, {struct_array});

  return arrow::Status::OK();
}

arrow::Status ColumnarTableToVector(const std::shared_ptr<arrow::Table> &table,
                                    std::vector<struct nested_repeated> *rows) {
  // WIP
  std::vector<std::shared_ptr<arrow::Field>> row_fields = {
      arrow::field("id", arrow::int64()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  std::shared_ptr<arrow::Field> rows_field =
      arrow::field("rows", arrow::struct_(row_fields));
  std::vector<std::shared_ptr<arrow::Field>> message_fields;
  message_fields.push_back(rows_field);
  auto expected_schema = std::make_shared<arrow::Schema>(message_fields);

  if (!expected_schema->Equals(*table->schema())) {
    // The table doesn't have the expected schema thus we cannot directly
    // convert it to our target representation.
    return arrow::Status::Invalid("Schemas are not matching!");
  }

  std::shared_ptr<arrow::ListArray> rows_list_array_ =
      std::static_pointer_cast<arrow::ListArray>(
          table->GetColumnByName("rows")->chunk(0));

  // TODO: use GetArrayView to get the struct array
  std::shared_ptr<arrow::ArrayData> rows_struct_array_ =
      rows_list_array_->data();

  return arrow::Status::OK();
}