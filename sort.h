#ifndef SORT_H
#define SORT_H

#include <arrow/api.h>

#include <print.h>

//++++++++++++++++++++++++++++++
// SORT
//++++++++++++++++++++++++++++++
template <typename T>
struct tuple {
	T elem;
	int chunkI;
	int elemI;
};

struct index123 {
	int chunkI;
	int elemI;
};

template <typename T>
bool tuple_compare(tuple<T> a, tuple<T> b) {
	return a.elem < b.elem;
}

template <typename T, typename T2>
std::vector<tuple<T>> *sort_sequential(std::shared_ptr<T2> array, int chunk_number) {
	std::vector<tuple<T>> *result = new std::vector<tuple<T>>(array->length());
	for (int i = 0; i < array->length(); i++) {
		T value = get_value<T, T2>(array, i);
		(*result)[i] = tuple<T>{value, chunk_number, i};
	}
	std::sort(result->begin(), result->end(), tuple_compare<T>);
	return result;
}

template <typename T, typename T2>
std::vector<index123> *sort_PARALLEL(std::shared_ptr<arrow::Table> table, int column_id) {
	std::vector<tuple<T>> *result = new std::vector<tuple<T>>(0);
	for (int i = 0; i < table->column(column_id)->data()->num_chunks(); i++) {
		auto array = std::static_pointer_cast<T2>(table->column(column_id)->data()->chunk(i));
		std::vector<tuple<T>> *addition = sort_sequential<T, T2>(array, i);
		std::vector<tuple<T>> *new_result = new std::vector<tuple<T>>(result->size() + addition->size());
		std::merge(result->begin(), result->end(), addition->begin(), addition->end(), new_result->begin(), tuple_compare<T>);
		delete(result);
		delete(addition);
		result = new_result;
	}
	std::vector<index123> *r = new std::vector<index123>(result->size());
	for (int i = 0; i < result->size(); i++) {
		(*r)[i] = index123{(*result)[i].chunkI, (*result)[i].elemI};
	}
	delete(result);
	return r;
}

// TODO only for one chunk currently
template <typename T, typename T2>
std::vector<T> *build_column(std::shared_ptr<arrow::ChunkedArray> column, std::vector<index123>* r) {
	std::vector<T> *result = new std::vector<T>(r->size());
	for (int i = 0; i < r->size(); i++) {
		auto array = std::static_pointer_cast<T2>(column->chunk((*r)[i].chunkI));
		T value = get_value<T, T2>(array, (*r)[i].elemI);
		(*result)[i] = value;
	}
	return result;
}

std::shared_ptr<arrow::Table> sort_finalize(std::shared_ptr<arrow::Table> table, std::vector<index123>* r) {
	std::vector<std::shared_ptr<arrow::Column>> clmns;
	std::vector<std::shared_ptr<arrow::Field>> flds;
	for (int i = 0; i < table->schema()->num_fields(); i++) {
		auto column = table->column(i)->data();
		if (column->type()->id() == arrow::Type::STRING) {
			auto new_column = build_column<std::string, arrow::StringArray>(column, r);
			add_column<std::string, arrow::StringBuilder>(clmns, flds, *new_column, table->schema()->field(i));
		} else if (column->type()->id() == arrow::Type::INT64) {
			auto new_column = build_column<arrow::Int64Type::c_type, arrow::Int64Array>(column, r);
			add_column<arrow::Int64Type::c_type, arrow::Int64Builder>(clmns, flds, *new_column, table->schema()->field(i));
		} else {
			auto new_column = build_column<arrow::DoubleType::c_type, arrow::DoubleArray>(column, r);
			add_column<arrow::DoubleType::c_type, arrow::DoubleBuilder>(clmns, flds, *new_column, table->schema()->field(i));
		}
	}
	return arrow::Table::Make(std::make_shared<arrow::Schema>(flds), clmns);
}

std::shared_ptr<arrow::Table> sort_main(std::shared_ptr<arrow::Table> table, int column_id) {
	std::vector<index123> *r;
	if (table->column(column_id)->data()->type()->id() == arrow::Type::STRING) {
		r = sort_PARALLEL<std::string, arrow::StringArray>(table, column_id);
	} else if (table->column(column_id)->data()->type()->id() == arrow::Type::INT64) {
		r = sort_PARALLEL<arrow::Int64Type::c_type, arrow::Int64Array>(table, column_id);
	} else {
		r = sort_PARALLEL<arrow::DoubleType::c_type, arrow::DoubleArray>(table, column_id);
	}
	return sort_finalize(table, r);
}

std::shared_ptr<arrow::Table> sort(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
	printf("TASK: sorting by %s.\n", column_ids.size() == 1 ? "single column" : "multiple columns");
	auto begin = std::chrono::steady_clock::now();
	std::shared_ptr<arrow::Table> t;
	if (column_ids.size() == 1) {
		t = sort_main(table, column_ids[0]);
	} else  {
		//t = sort_main(table, column_ids);
	}
	print_time(begin);
	return t;
}
#endif
