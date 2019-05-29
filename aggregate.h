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
	std::shared_ptr<arrow::Field> field;
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
std::shared_ptr<arrow::Table> aggregate_finalize(group *gb, std::vector<partial_aggregate_task<T>*> p_tasks) {
	std::vector<std::shared_ptr<arrow::Column>> clmns;
	std::vector<std::shared_ptr<arrow::Field>> flds;
	if (gb != NULL) {
		// TODO Reserve
		clmns = std::move(gb->columns);
		flds = std::move(gb->fields);
	}
	for (int i = 0; i < p_tasks.size(); i++) {
		if (p_tasks[i]->task->type == average) {
			for (int j = 0; j < p_tasks[i]->partial.size(); j++) {
				p_tasks[i]->partial[j] = p_tasks[i]->partial[j] / p_tasks[i]->partial1[j];
			}
		}
		add_column<T, T4>(clmns, flds, p_tasks[i]->partial, p_tasks[i]->field);
	}
	return arrow::Table::Make(std::make_shared<arrow::Schema>(flds), clmns);
}

template <typename T, typename T2, typename T4>
std::shared_ptr<arrow::Table> aggregate_PARALLEL(std::shared_ptr<arrow::Table> table, group *gb, std::vector<aggregate_task*> tasks) {
	printf("TASK: aggregating %ld columns %s.\n", tasks.size(), gb != NULL ? "based on group_by" : "to zero column (no group_by)");
	auto begin = std::chrono::steady_clock::now();
	std::vector<partial_aggregate_task<T>*> p_tasks;
	for (int i = 0; i < tasks.size(); i++) {
		p_tasks.push_back(new partial_aggregate_task<T>{tasks[i], 0, table->schema()->field(tasks[i]->from_column)});
	}
	for (int i = 0; i < tasks.size(); i++) {
		for (int j = 0; j < table->column(tasks[i]->from_column)->data()->num_chunks(); j++) {
			auto c = std::dynamic_pointer_cast<T2>(table->column(tasks[i]->from_column)->data()->chunk(j));
			if (c == NULL) {
				printf("Type of %d column is wrong!\n", tasks[i]->from_column + 1);
			}
			aggregate_sequential<T, T2>(c, gb, p_tasks[i]);
		}
	}
	auto new_table = aggregate_finalize<T, T4>(gb, p_tasks);
	print_time(begin);
	return new_table;
}

#endif
