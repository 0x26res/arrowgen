#define BOOST_TEST_MODULE ArrowGenTest

#include <fstream>
#include <iostream>
#include <stdexcept>

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include <google/protobuf/util/json_util.h>

#include <simple.arrow.h>
#include <simple.pb.h>

namespace {
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
  if (!infile.good()) {
    throw std::runtime_error("Could not find file " + fileName);
  }
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
  // Test with binary protocol
  for (T const &message : data) {
    std::string value;
    message.SerializeToString(&value);
    ARROW_RETURN_NOT_OK(appender.append(value.c_str(), value.size()));
  }
  return arrow::Status::OK();
}
} // namespace

BOOST_AUTO_TEST_SUITE(ReadFromDataTestSuite)

BOOST_AUTO_TEST_CASE(test_TestMessage) {
  std::vector<messages::TestMessage> messages =
      loadJson<messages::TestMessage>("data/TestMessage.jsonl");
  arrow::Status status =
      testDataType<messages::TestMessage, messages::TestMessageAppender,
                   messages::TestMessageReader>(messages);
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(), status);
}

BOOST_AUTO_TEST_CASE(test_DataRow) {
  std::vector<messages::DataRow> messages =
      loadJson<messages::DataRow>("data/DataRow.jsonl");
  arrow::Status status =
      testDataType<messages::DataRow, messages::DataRowAppender,
                   messages::DataRowReader>(messages);
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(), status);
}

BOOST_AUTO_TEST_CASE(test_SearchResult) {
  std::vector<messages::SearchResult> messages =
      loadJson<messages::SearchResult>("data/SearchResult.jsonl");
  arrow::Status status =
      testDataType<messages::SearchResult, messages::SearchResultAppender,
                   messages::SearchResultReader>(messages);
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(), status);
}

BOOST_AUTO_TEST_CASE(test_NestedMessage) {
  std::vector<messages::NestedMessage> messages =
      loadJson<messages::NestedMessage>("data/NestedMessage.jsonl");
  arrow::Status status =
      testDataType<messages::NestedMessage, messages::NestedMessageAppender,
                   messages::NestedMessageReader>(messages);
  BOOST_REQUIRE_EQUAL(arrow::Status::OK(), status);
}

BOOST_AUTO_TEST_CASE(test_build_works) {
  messages::TestMessage test_message;
  std::cout << test_message.int32_value() << std::endl;
}

BOOST_AUTO_TEST_SUITE_END()
