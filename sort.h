#ifndef SORT_H
#define SORT_H

#include <arrow/api.h>

#include <print.h>

//++++++++++++++++++++++++++++++
// SORT
//++++++++++++++++++++++++++++++

// For both: single and multiple columns
struct index123 {
	int chunkI;
	int elemI;
	static std::vector<int> column_ids;
	static std::shared_ptr<arrow::Table> table;
};

template <typename T, typename T2>
std::vector<T> *build_vector(std::shared_ptr<arrow::ChunkedArray> column, std::vector<index123>* r) {
	std::vector<T> *result = new std::vector<T>(r->size());
	for (int i = 0; i < r->size(); i++) { // TODO only for one chunk currently
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
		auto column = table->column(i);
		std::shared_ptr<arrow::Array> data;
		if (column->type()->id() == arrow::Type::STRING) {
			auto new_column = build_vector<std::string, arrow::StringArray>(column->data(), r);
			data = vector_to_array<std::string, arrow::StringBuilder>(*new_column);
		} else if (column->type()->id() == arrow::Type::INT64) {
			auto new_column = build_vector<arrow::Int64Type::c_type, arrow::Int64Array>(column->data(), r);
			data = vector_to_array<arrow::Int64Type::c_type, arrow::Int64Builder>(*new_column);
		} else {
			auto new_column = build_vector<arrow::DoubleType::c_type, arrow::DoubleArray>(column->data(), r);
			data = vector_to_array<arrow::DoubleType::c_type, arrow::DoubleBuilder>(*new_column);
		}
		auto field = column->field();
		clmns.push_back(std::make_shared<arrow::Column>(field->name(), data));
		flds.push_back(field);
	}
	return arrow::Table::Make(std::make_shared<arrow::Schema>(flds), clmns);
}

// For multiple columns
template <typename T>
struct tuple {
	T elem;
	int chunkI;
	int elemI;
};

std::vector<int> index123::column_ids;
std::shared_ptr<arrow::Table> index123::table;

bool index123_compare(index123 a, index123 b) {
	for (int i = 0; i < index123::column_ids.size(); i++) {
		auto arrayA = index123::table->column(index123::column_ids[i])->data()->chunk(a.chunkI);
		auto arrayB = index123::table->column(index123::column_ids[i])->data()->chunk(b.chunkI);
		int result = compare(arrayA, a.elemI, arrayB, b.elemI);
		if (result < 0) {
			return true;//false;
		} else if (result > 0) {
			return false;//true;
		}
	}
	return false;
}

std::vector<index123> *sort_sequential_multiple(int array_length, int chunk_number) {
	std::vector<index123> *result = new std::vector<index123>(array_length);
	// TODO array_to_vector?
	for (int i = 0; i < array_length; i++) {
		(*result)[i] = index123{chunk_number, i};
	}
	std::sort(result->begin(), result->end(), index123_compare);
	return result;
}

std::shared_ptr<arrow::Table> sort_parallel_multiple(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
	printf("      Arrow is columnar database and this request is low performance\n");
	index123::column_ids = column_ids;
	index123::table = table;
	std::vector<index123> *result = new std::vector<index123>(0);
	for (int i = 0; i < table->column(column_ids[0])->data()->num_chunks(); i++) { // other columns are the same
		std::vector<index123> *addition = sort_sequential_multiple(table->column(column_ids[0])->data()->chunk(i)->length(), i);
		std::vector<index123> *new_result = new std::vector<index123>(result->size() + addition->size());
		std::merge(result->begin(), result->end(), addition->begin(), addition->end(), new_result->begin(), index123_compare);
		delete(result);
		delete(addition);
		result = new_result;
	}
	auto t = sort_finalize(table, result);
	delete(result);
	return t;
}

// For single column
template <typename T>
bool tuple_compare(tuple<T> a, tuple<T> b) {
	return a.elem < b.elem;
}

template <typename T, typename T2>
std::vector<tuple<T>> *sort_sequential_single(std::shared_ptr<T2> array, int chunk_number) {
	std::vector<tuple<T>> *result = new std::vector<tuple<T>>(array->length());
	// TODO array_to_vector?
	for (int i = 0; i < array->length(); i++) {
		T value = get_value<T, T2>(array, i);
		(*result)[i] = tuple<T>{value, chunk_number, i};
	}
	std::sort(result->begin(), result->end(), tuple_compare<T>);
	return result;
}

template <typename T, typename T2>
std::vector<index123> *sort_parallel_single(std::shared_ptr<arrow::ChunkedArray> column) {
	std::vector<tuple<T>> *result = new std::vector<tuple<T>>(0);
	for (int i = 0; i < column->num_chunks(); i++) {
		auto array = std::static_pointer_cast<T2>(column->chunk(i));
		std::vector<tuple<T>> *addition = sort_sequential_single<T, T2>(array, i);
		std::vector<tuple<T>> *new_result = new std::vector<tuple<T>>(result->size() + addition->size());
		std::merge(result->begin(), result->end(), addition->begin(), addition->end(), new_result->begin(), tuple_compare<T>);
		delete(result);
		delete(addition);
		result = new_result;
	}
	std::vector<index123> *r = new std::vector<index123>(result->size());
	// TODO remove this
	for (int i = 0; i < result->size(); i++) {
		(*r)[i] = index123{(*result)[i].chunkI, (*result)[i].elemI};
	}
	delete(result);
	return r;
}

std::shared_ptr<arrow::Table> sort_dispatch(std::shared_ptr<arrow::Table> table, int column_id) {
	std::vector<index123> *r;
	auto column = table->column(column_id);
	if (column->type()->id() == arrow::Type::STRING) {
		r = sort_parallel_single<std::string, arrow::StringArray>(column->data());
	} else if (column->type()->id() == arrow::Type::INT64) {
		r = sort_parallel_single<arrow::Int64Type::c_type, arrow::Int64Array>(column->data());
	} else {
		r = sort_parallel_single<arrow::DoubleType::c_type, arrow::DoubleArray>(column->data());
	}
	return sort_finalize(table, r);
}

// Main function
std::shared_ptr<arrow::Table> sort(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
	printf("TASK: sorting by %s.\n", column_ids.size() == 1 ? "single column" : "multiple columns");
	auto begin = std::chrono::steady_clock::now();
	std::shared_ptr<arrow::Table> t;
	if (column_ids.size() == 1) {
		t = sort_dispatch(table, column_ids[0]);
	} else  {
		t = sort_parallel_multiple(table, column_ids);
	}
	print_time(begin);
	return t;
}
#endif
