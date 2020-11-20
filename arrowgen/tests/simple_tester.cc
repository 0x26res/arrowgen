#include "messages/simple.appender.h"


#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);

messages::DataRow createDataRow(int id, double cost, std::vector<double> const &cost_components) {
  messages::DataRow results;
  results.set_id(id);
  results.set_cost(cost);
  for (double cost_component : cost_components) {
    results.add_cost_components(cost_component);
  }
  return results;
}

messages::SearchResult createSearchResult(messages::ReturnCode return_code, std::string const &message) {
  messages::SearchResult results;
  results.set_return_code(return_code);
  results.set_message(message);
  return results;
}

template<class T>
bool compare(T const &l, T const &r) {
  std::string lstring = l.SerializeAsString();
  std::string rstring = r.SerializeAsString();
  return lstring == rstring;
}

template<class T, class R>
arrow::Status compare(std::shared_ptr<arrow::Table> table, std::vector<T> const& data) {
  R reader(table);
  for (T const &message : data) {
    T actual;
    ARROW_RETURN_NOT_OK(reader.readNext(actual));
    assert(compare(message, actual));
  }
  assert(reader.end());
  return arrow::Status::OK();
}

template<class T, class A, class R>
arrow::Status testDataType(std::vector<T> const data) {
  A appender;
  for (T const &message : data) {
    ARROW_RETURN_NOT_OK(appender.append(message));
  }
  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(appender.build(&table));
  assert(table->num_rows() == data.size());

  arrow::Status status = ::compare<T, R>(table, data);
  ARROW_RETURN_NOT_OK(status);
  std::cout << *table->schema() << std::endl;
  std::cout << table->num_rows() << " * " << table->num_columns() << std::endl;

  std::shared_ptr<arrow::Table> table2 = arrow::ConcatenateTables({table, table}).ValueOrDie();
  std::vector<T> data2(data);
  for (T const& message : data) {
    data2.push_back(message);
  }
  arrow::Status status2 = ::compare<T, R>(table2, data2);
  ARROW_RETURN_NOT_OK(status2);
  return arrow::Status::OK();
}

arrow::Status testDataRow() {
  std::vector<messages::DataRow> messages = {
      ::createDataRow(1, 1.0, {0.5, 0.5}),
      ::createDataRow(2, 2.0, {0.5, 0.5, 1.0}),
  };
  return testDataType<messages::DataRow, messages::DataRowAppender, messages::DataRowReader>(messages);
}

arrow::Status testSearchResult() {
  std::vector<messages::SearchResult> messages = {
      ::createSearchResult(messages::ReturnCode::OK, "200"),
      ::createSearchResult(messages::ReturnCode::ERROR, "403"),
  };
  return testDataType<messages::SearchResult, messages::SearchResultAppender, messages::SearchResultReader>(messages);
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  EXIT_ON_FAILURE(testDataRow());
  EXIT_ON_FAILURE(testSearchResult());
  return EXIT_SUCCESS;
}

