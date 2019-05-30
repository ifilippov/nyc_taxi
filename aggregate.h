#ifndef AGGREGATE_H
#define AGGREGATE_H

#include <arrow/api.h>

#include <group_by.h>
#include <print.h>
#include <util.h>

//++++++++++++++++++++++++++++++
// AGGREGATE
//++++++++++++++++++++++++++++++

enum aggregate_task_type { count, sum, min, max, average /*TODO median*/ };

struct aggregate_task {
	aggregate_task_type type;
	int from_column;
};

template <typename T>
struct partial_aggregate_task {
	aggregate_task *task;
	int iterate;
	// mutable buffers?
	std::vector<T> partial;
	std::vector<T> partial1;
};

template <typename T>
void aggregate_internal(partial_aggregate_task<T>* p_task, int row_number, int value) {
	if (p_task->partial.size() <= row_number) {
		// TODO what if not increasing?
		p_task->partial.push_back(0);
	}
	switch (p_task->task->type) {
	case sum:
		p_task->partial[row_number] += value;
		break;
	case count:
		p_task->partial[row_number]++;
		break;
	case min:
		if (value < p_task->partial[row_number]) {
			p_task->partial[row_number] = value;
		}
		break;
	case max:
		if (value > p_task->partial[row_number]) {
			p_task->partial[row_number] = value;
		}
		break;
	case average:
		p_task->partial[row_number] += value;
		if (p_task->partial1.size() <= row_number) {
			p_task->partial1.push_back(0);
		}
		p_task->partial1[row_number]++;
		break;
	}
}

template <typename T, typename T2>
void aggregate_sequential(std::shared_ptr<T2> c, group *gb, partial_aggregate_task<T>* p_task) {
	auto cv = c->raw_values();
	for (int j = 0; j < c->length(); j++) {
		int row_number;
		if (gb != NULL) {
			row_number = gb->redirection[p_task->iterate + j];
		} else {
			row_number = 0;
		}
		aggregate_internal(p_task, row_number, cv[j]);
	}
	p_task->iterate += c->length();
}

template <typename T, typename T4>
std::shared_ptr<arrow::Array> aggregate_finalize(partial_aggregate_task<T> *p_task) {
	if (p_task->task->type == average) {
		for (int j = 0; j < p_task->partial.size(); j++) {
			p_task->partial[j] = p_task->partial[j] / p_task->partial1[j];
		}
	}
	return vector_to_array<T, T4>(p_task->partial);
}

template <typename T, typename T2, typename T4>
std::shared_ptr<arrow::Array> aggregate_PARALLEL(std::shared_ptr<arrow::ChunkedArray> column, group *gb, aggregate_task *task) {
	partial_aggregate_task<T> *p_task = new partial_aggregate_task<T>{task, 0};
	for (int j = 0; j < column->num_chunks(); j++) {
		auto c = std::static_pointer_cast<T2>(column->chunk(j));
		aggregate_sequential<T, T2>(c, gb, p_task);
	}
	auto t = aggregate_finalize<T, T4>(p_task);
	delete(p_task);
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
