#ifndef AGGREGATE_H
#define AGGREGATE_H

#include <arrow/api.h>

#include <group_by.h>
#include <print.h>
#include <util.h>
#include <cassert>

#if USE_TBB
#include <tbb/tbb.h>
#endif


// This helper is equivalent to runtime dispatch like switch((E) t) {case Args[0]:...; case Args[1]:...;...}
// except it predefines all the case branches so it is easier to support multiple instances of runtime dispatch
// over the same type & values. Example:
//   using aggregate_dispatcher = runtime_dispatcher<aggregate_task_type, count, sum, min, max, average>;
//   aggregate_dispatcher::call<template body>(task->type, c, gb, &ptask);
template<typename E, E T, E... Args>
struct runtime_dispatcher {
    template<template<E> typename F, typename... A>
    static auto call(E t, A... args) -> decltype(F<T>()(args...)) {
        if(t==T)
          return F<T>()(args...);
        else if constexpr(sizeof...(Args))
          return runtime_dispatcher<E, Args...>::template call<F>(t, args...);
        assert(false);
    }
};

//++++++++++++++++++++++++++++++
// AGGREGATE
//++++++++++++++++++++++++++++++

enum aggregate_task_type { count, sum, min, max, average /*TODO median*/ };
using aggregate_dispatcher = runtime_dispatcher<aggregate_task_type, count, sum, min, max, average>;

struct aggregate_task {
  aggregate_task_type type;
  int from_column;
};

template <typename T>
struct partial_aggregate_task {
  aggregate_task *task;
  // mutable buffers?
  std::vector<T> partial;
  std::vector<T> partial1;
};

template<aggregate_task_type a, typename T>
void aggregate_internal(partial_aggregate_task<T>* p_task, int row_number, int value, int c) {
    if constexpr(a == sum) {
        p_task->partial[row_number] += value;
    }
    if constexpr(a == count) {
        p_task->partial1[row_number] += c;
    }
    if constexpr(a == min) {
        if (value < p_task->partial[row_number]) {
            p_task->partial[row_number] = value;
        }
    }
    if constexpr(a == max) {
        if (value > p_task->partial[row_number]) {
            p_task->partial[row_number] = value;
        }
    }
    if constexpr(a == average) {
        p_task->partial[row_number] += value;
        p_task->partial1[row_number] += c;
    }
}

template <typename T, typename T2>
struct aggregate_sequential {
    template<aggregate_task_type a>
    struct body {
        void operator()(std::shared_ptr<T2> c, group *gb, partial_aggregate_task<T>* p_task, int chunk_number) {
            auto cv = c->raw_values();
            if (gb) {
                for (int j = 0; j < c->length(); j++) {
                    aggregate_internal<a>(p_task, gb->redirection[chunk_number][j], cv[j], 1);
                }
            } else {
                for (int j = 0; j < c->length(); j++) {
                    aggregate_internal<a>(p_task, 0, cv[j], 1);
                }
            }
        }
    };
};

template <typename T, typename T4>
struct aggregate_finalize {
    template<aggregate_task_type a>
    struct body {
        std::shared_ptr<arrow::Array> operator()(std::vector<partial_aggregate_task<T> *> p_tasks) {
            for (int row_number = 0; row_number < p_tasks[0]->partial.size(); row_number++) {
                for (int i = 1; i < p_tasks.size(); i++) {
                    aggregate_internal<a>(p_tasks[0], row_number, p_tasks[i]->partial[row_number], p_tasks[i]->partial1[row_number]);
                }
            }
            if constexpr(a == count) {
                return vector_to_array<T, T4>(p_tasks[0]->partial1);
            }
            if constexpr(a == average) {
                for (int j = 0; j < p_tasks[0]->partial.size(); j++) {
                    p_tasks[0]->partial[j] = p_tasks[0]->partial[j] / p_tasks[0]->partial1[j];
                }
            }
            return vector_to_array<T, T4>(p_tasks[0]->partial);
        }
    };
};

template <typename T, typename T2, typename T4>
std::shared_ptr<arrow::Array> aggregate_PARALLEL(std::shared_ptr<arrow::ChunkedArray> column, group *gb, aggregate_task *task) {
    int N = 10;
    std::vector<partial_aggregate_task<T> *> p_tasks(N);
    int s = gb != NULL ? gb->max_index : 1;
    for (int i = 0; i < N; i++) {
        p_tasks[i] = new partial_aggregate_task<T>{task, std::vector<T>(s), std::vector<T>(s)};
        for (int k = 0; k < s; k++) {
            // TODO need some boolean point like "was used" for min and max. Not to use defult 0 for them.
            p_tasks[i]->partial[k] = 0;
            p_tasks[i]->partial1[k] = 0;
        }
    }
    for (int j = 0; j < column->num_chunks(); j++) {
        auto c = std::static_pointer_cast<T2>(column->chunk(j));
        // TBB in parallel for all available chunks or sequential for each incoming chunk
        aggregate_dispatcher::call<aggregate_sequential<T, T2>::template body>(task->type, c, gb, p_tasks[j%N], j);
    }
    auto t = aggregate_dispatcher::call<aggregate_finalize<T, T4>::template body>(task->type, p_tasks);
    for (int i = 0; i < N; i++) {
        delete(p_tasks[i]);
    }
    return t;
}

std::shared_ptr<arrow::Table> aggregate(std::shared_ptr<arrow::Table> table, group *gb, std::vector<aggregate_task*> tasks) {
	printf("TASK: aggregating %ld columns %s.\n", tasks.size(), gb != NULL ? "based on group_by" : "to zero column (no group_by)");
	auto begin = std::chrono::steady_clock::now();
	std::vector<std::shared_ptr<arrow::Column>> clmns;
	std::vector<std::shared_ptr<arrow::Field>> flds;
	if (gb != NULL) {
		// TODO Reserve
		clmns = std::move(gb->columns);
		flds = std::move(gb->fields);
	}
	for (int i = 0; i < tasks.size(); i++) {
		auto column = table->column(tasks[i]->from_column);
		std::shared_ptr<arrow::Array> data;
		if (column->type()->id() == arrow::Type::STRING) {
			// TODO data = aggregate_PARALLEL<std::string, arrow::StringArray, arrow::StringBuilder>(column->data(), gb, tasks[i]);
		} else if (column->type()->id() == arrow::Type::INT64) {
			data = aggregate_PARALLEL<arrow::Int64Type::c_type, arrow::Int64Array, arrow::Int64Builder>(column->data(), gb, tasks[i]);
		} else {
			data = aggregate_PARALLEL<arrow::DoubleType::c_type, arrow::DoubleArray, arrow::DoubleBuilder>(column->data(), gb, tasks[i]);
		}
		auto field = column->field();
		clmns.push_back(std::make_shared<arrow::Column>(field->name(), data));
		flds.push_back(field);
	}
	auto new_table = arrow::Table::Make(std::make_shared<arrow::Schema>(flds), clmns);
	print_time(begin);
	return new_table;
}

#endif
