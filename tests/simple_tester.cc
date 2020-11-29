#include "messages/simple.appender.h"
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <google/protobuf/util/json_util.h>
#include <sstream>
#include <stdexcept>

#define EXIT_ON_FAILURE(expr)                                                  \
  do {                                                                         \
    arrow::Status status_ = (expr);                                            \
    if (!status_.ok()) {                                                       \
      std::cerr << status_.message() << std::endl;                             \
      return EXIT_FAILURE;                                                     \
    }                                                                          \
  } while (0);

template <class T> void compareProto(T const &l, T const &r) {
  std::string lstring, rstring;
  google::protobuf::util::Status lstatus =
      google::protobuf::util::MessageToJsonString(l, &lstring);
  google::protobuf::util::Status rstatus =
      google::protobuf::util::MessageToJsonString(r, &rstring);
  if (!lstatus.ok()) {
    throw std::runtime_error(lstatus.error_message().as_string());
  } else if (!rstatus.ok()) {
    throw std::runtime_error(rstatus.error_message().as_string());
  } else if (lstring != rstring) {
    throw std::runtime_error("\n" + lstring + '\n' + rstring);
  } else {
    std::cout << "OK " << lstring << std::endl;
  }
}

template <class T, class R>
arrow::Status compare(std::shared_ptr<arrow::Table> table,
                      std::vector<T> const &data) {
  R reader(table);
  for (T const &message : data) {
    T actual;
    ARROW_RETURN_NOT_OK(reader.readNext(actual));
    compareProto(message, actual);
  }
  assert(reader.end());
  return arrow::Status::OK();
}

template <class T> std::vector<T> loadJson(std::string const &fileName) {
  std::vector<T> messages;
  std::ifstream infile(fileName);
  assert(infile.good());
  std::string line;
  while (std::getline(infile, line)) {
    boost::trim(line);
    if (!line.empty()) {
      T message;
      google::protobuf::util::Status status =
          google::protobuf::util::JsonStringToMessage(line, &message);
      if (status != google::protobuf::util::Status::OK) {
        throw std::runtime_error("[" + line + ']' +
                                 status.error_message().as_string());
      } else {
        messages.push_back(message);
      }
    }
  }
  return messages;
}

template <class T, class A, class R>
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

  std::shared_ptr<arrow::Table> table2 =
      arrow::ConcatenateTables({table, table}).ValueOrDie();
  std::vector<T> data2(data);
  for (T const &message : data) {
    data2.push_back(message);
  }
  arrow::Status status2 = ::compare<T, R>(table2, data2);
  ARROW_RETURN_NOT_OK(status2);
  return arrow::Status::OK();
}

arrow::Status testDataRow() {
  std::vector<messages::DataRow> messages =
      loadJson<messages::DataRow>("messages/DataRow.jsonl");
  return testDataType<messages::DataRow, messages::DataRowAppender,
                      messages::DataRowReader>(messages);
}

arrow::Status testTestMessage() {
  std::vector<messages::TestMessage> messages =
      loadJson<messages::TestMessage>("messages/TestMessage.jsonl");
  return testDataType<messages::TestMessage, messages::TestMessageAppender,
                      messages::TestMessageReader>(messages);
}

arrow::Status testSearchResult() {
  std::vector<messages::SearchResult> messages =
      loadJson<messages::SearchResult>("messages/SearchResult.jsonl");
  return testDataType<messages::SearchResult, messages::SearchResultAppender,
                      messages::SearchResultReader>(messages);
  return arrow::Status::OK();
}

arrow::Status testNestedMessage() {
  std::vector<messages::NestedMessage> messages =
      loadJson<messages::NestedMessage>("messages/NestedMessage.jsonl");
  return testDataType<messages::NestedMessage, messages::NestedMessageAppender,
                      messages::NestedMessageReader>(messages);
  return arrow::Status::OK();
}

int main(int argc, char **argv) {
  EXIT_ON_FAILURE(testDataRow());
  EXIT_ON_FAILURE(testSearchResult());
  EXIT_ON_FAILURE(testTestMessage());
  EXIT_ON_FAILURE(testNestedMessage());
  return EXIT_SUCCESS;
}
