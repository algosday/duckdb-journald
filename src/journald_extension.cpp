

#define DUCKDB_EXTENSION_MAIN
#include "journald_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <cstddef>
#include <cstdint>
#include <systemd/sd-journal.h>

namespace duckdb {

static const string TIME_FIELD = "time";
struct BB : public TableFunctionData {};
struct SDJournalData : public TableFunctionData {
  sd_journal *j;
  vector<string> names;
  bool reverse = false;

  explicit SDJournalData(int flags = SD_JOURNAL_LOCAL_ONLY, string dir = "") {
    int ret = 0;
    if (!dir.empty()) {
      ret = sd_journal_open_directory(&j, dir.c_str(), flags);
    } else {
      ret = sd_journal_open(&j, flags);
    }
    if (ret < 0) {
      throw InternalException("Failed to open journal: %s\n", strerror(-ret));
    }

    const char *field;
    SD_JOURNAL_FOREACH_FIELD(j, field) {
      string name(field);
      if (name != TIME_FIELD) {
        names.push_back(name);
      }
    };
  }

  ~SDJournalData() {
    sd_journal_close(j);
    j = nullptr;
  }
  // idx_t MaxThreads() const override { return 1; }
};
struct SDJournalGlobalState : public GlobalTableFunctionState {
  vector<string> names;
  bool seek = false;
  uint64_t time_le = 0, time_gt = 0;
  explicit SDJournalGlobalState(vector<string> &&names_) : names(names_) {}
  void applyFilters(sd_journal *j, const vector<column_t> &column_ids,
                    TableFilterSet &set) {
    for (auto &entry : set.filters) {
      auto idx = entry.first;
      auto &filter = entry.second;
      if (filter->filter_type == TableFilterType::CONJUNCTION_AND) {
        auto &child_filter = filter->Cast<ConjunctionAndFilter>();
        for (auto &child : child_filter.child_filters) {
          if (child->filter_type == TableFilterType::CONSTANT_COMPARISON) {
            auto &constant_filter = child->Cast<ConstantFilter>();
            if (constant_filter.comparison_type ==
                ExpressionType::COMPARE_EQUAL) {
              auto &field = names.at(column_ids[idx]);
              auto &value = StringValue::Get(
                  constant_filter.constant.DefaultCastAs(LogicalType::VARCHAR));
              auto match = field + "=" + value;
              sd_journal_add_match(j, match.c_str(), match.size());
            } else if (constant_filter.comparison_type ==
                       ExpressionType::COMPARE_GREATERTHAN) {
              if (column_ids[idx] == names.size())
                time_gt = TimestampValue::Get(constant_filter.constant).value;
            } else if (constant_filter.comparison_type ==
                       ExpressionType::COMPARE_LESSTHAN) {
              if (column_ids[idx] == names.size())
                time_le = TimestampValue::Get(constant_filter.constant).value;
            }
          }
        }
      }
    }
  }
};

static unique_ptr<FunctionData> SDJournalBind(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {

  int flags = 0;
  auto &local_only = input.named_parameters["local_only"];
  if (local_only.IsNull() || BooleanValue::Get(local_only)) {
    flags |= SD_JOURNAL_LOCAL_ONLY;
  }
  auto &runtime_only = input.named_parameters["runtime_only"];
  if (!runtime_only.IsNull() && BooleanValue::Get(runtime_only)) {
    flags |= SD_JOURNAL_RUNTIME_ONLY;
  }

  auto &system = input.named_parameters["system"];
  if (!system.IsNull() && BooleanValue::Get(system)) {
    flags |= SD_JOURNAL_SYSTEM;
  }

  auto &current_user = input.named_parameters["current_user"];
  if (!current_user.IsNull() && BooleanValue::Get(current_user)) {
    flags |= SD_JOURNAL_CURRENT_USER;
  }

  unique_ptr<SDJournalData> data;
  if (input.inputs.size() > 0) {
    auto dir = StringValue::Get(input.inputs.at(0));
    data = make_uniq<SDJournalData>(flags, dir);

  } else {
    data = make_uniq<SDJournalData>(flags);
  }

  auto &reverse = input.named_parameters["reverse"];
  if (!reverse.IsNull()) {
    data->reverse = BooleanValue::Get(reverse);
  }
  names = data->names;
  return_types = std::vector<LogicalType>(names.size(), LogicalType::VARCHAR);

  names.push_back(TIME_FIELD);
  return_types.push_back(LogicalType::TIMESTAMP);

  return data;
}

static unique_ptr<GlobalTableFunctionState>
SDJournalInitGlobalState(ClientContext &context,
                         TableFunctionInitInput &input) {
  auto &data = input.bind_data->Cast<SDJournalData>();

  vector<string> selected_names;
  for (auto id : input.column_ids) {
    if (id < data.names.size()) {
      selected_names.push_back(data.names[id]);
    } else {
      selected_names.push_back(TIME_FIELD);
    }
  }

  auto state = make_uniq<SDJournalGlobalState>(std::move(selected_names));

  if (input.filters != nullptr) {
    state->applyFilters(data.j, input.column_ids, *input.filters);
  }
  return state;
}
static void SDJournalScan(ClientContext &context, TableFunctionInput &input,
                          DataChunk &output) {
  auto &data = input.bind_data->Cast<SDJournalData>();
  auto &state = input.global_state->Cast<SDJournalGlobalState>();
  int ret = 0;
  idx_t i = 0;
  if (!state.seek) {
    if (data.reverse) {
      if (state.time_le != 0) {
        ret = sd_journal_seek_realtime_usec(data.j, state.time_le);
      } else {
        ret = sd_journal_seek_tail(data.j);
      }

    } else if (state.time_gt != 0) {
      ret = sd_journal_seek_realtime_usec(data.j, state.time_gt);
    }
    if (ret < 0)
      throw InternalException("Failed to seek journal data: %s\n",
                              strerror(-ret));
    state.seek = true;
  }

  for (i = 0; i < STANDARD_VECTOR_SIZE; i++) {
    if (data.reverse) {
      ret = sd_journal_previous(data.j);
    } else {
      ret = sd_journal_next(data.j);
    }

    if (ret <= 0) {
      break;
    }
    uint64_t usec = 0;
    ret = sd_journal_get_realtime_usec(data.j, &usec);
    if (ret < 0) {
      throw InternalException("Failed to get realtime_usec: %s\n",
                              strerror(-ret));
    }

    if (data.reverse && state.time_gt != 0 && usec < state.time_gt) {
      break;
    } else if (state.time_le != 0 && usec > state.time_le) {
      break;
    }

    // SD_JOURNAL_FOREACH_DATA()
    for (idx_t j = 0; j < state.names.size(); j++) {
      auto &name = state.names[j];
      if (name == TIME_FIELD) {
        if (usec == 0) {
          output.SetValue(j, i, Value(LogicalType::TIMESTAMP));
          continue;
        }
        output.SetValue(j, i, Value::TIMESTAMP(timestamp_t(usec)));
        continue;
      }
      const void *value = nullptr;
      size_t len = 0;
      ret = sd_journal_get_data(data.j, name.c_str(), &value, &len);
      if (ret < 0) {
        output.SetValue(j, i, Value(LogicalType::VARCHAR));
        continue;
      }
      string strval((const char *)value, len);
      output.SetValue(j, i, strval.substr(name.size() + 1));
    }
  }

  output.SetCardinality(i);
}

static TableFunction createSDJournalTableFunction(vector<LogicalType> args) {
  auto func = TableFunction("journal", args, SDJournalScan, SDJournalBind,
                            SDJournalInitGlobalState, nullptr);
  func.filter_pushdown = true;
  func.projection_pushdown = true;
  func.named_parameters["reverse"] = LogicalType::BOOLEAN;
  func.named_parameters["local_only"] = LogicalType::BOOLEAN;
  func.named_parameters["runtime_only"] = LogicalType::BOOLEAN;
  func.named_parameters["system"] = LogicalType::BOOLEAN;
  func.named_parameters["current_user"] = LogicalType::BOOLEAN;
  return func;
}

static void LoadInternal(DatabaseInstance &instance) {
  TableFunctionSet set("journal");
  set.AddFunction(createSDJournalTableFunction({}));
  set.AddFunction(createSDJournalTableFunction({LogicalTypeId::VARCHAR}));
}

void JournaldExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string JournaldExtension::Name() { return "journald"; }

std::string JournaldExtension::Version() const {
#ifdef EXT_VERSION_journald
  return EXT_VERSION_journald;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void journald_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::JournaldExtension>();
}

DUCKDB_EXTENSION_API const char *journald_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
